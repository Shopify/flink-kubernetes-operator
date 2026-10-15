/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.CustomApplicationDetails;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/**
 * Observes the Flink 2.3+ application running on an application cluster and records its state in
 * {@code clusterInfo}. If the application is terminal, or not listed at all, while the expected job
 * is not found, Flink skipped the job submission because of a recorded terminal result. The
 * deployment would otherwise sit in {@code RECONCILING} with no explanation, see FLINK-40467.
 *
 * <p>The alert is only raised once the deployment is older than {@link
 * KubernetesOperatorConfigOptions#DEPLOYMENT_READINESS_TIMEOUT}. A freshly started JobManager may
 * not have registered the application or submitted the job yet.
 */
public class ApplicationStatusObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStatusObserver.class);

    public static final String FIELD_NAME_APPLICATION_ID = "application-id";
    public static final String FIELD_NAME_APPLICATION_STATE = "application-state";

    /**
     * Marks in {@code clusterInfo} that the application-without-job error is active. The error is
     * kept until the job is found again, independently of the error message text.
     */
    public static final String FIELD_NAME_APPLICATION_WITHOUT_JOB = "application-without-job";

    public static final String MSG_APPLICATION_WITHOUT_JOB_HINT =
            "Check the application result store configuration and redeploy.";

    public static final String MSG_APPLICATION_TERMINAL_WITHOUT_JOB =
            "Application %s is %s but the job %s is not running on the cluster. "
                    + "Flink does not run an application that has already reached a terminal state, "
                    + "so the job will not be submitted. "
                    + MSG_APPLICATION_WITHOUT_JOB_HINT;

    public static final String MSG_APPLICATION_MISSING_WITHOUT_JOB =
            "No application is registered on the cluster and the job %s is not running. "
                    + "Flink skips the submission of an application whose terminal result is "
                    + "already recorded in the application result store, so the job will not be "
                    + "submitted. "
                    + MSG_APPLICATION_WITHOUT_JOB_HINT;

    private final EventRecorder eventRecorder;

    public ApplicationStatusObserver(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    /**
     * Observe the application after the job was observed on a ready JobManager.
     *
     * @param ctx Resource context.
     * @param jobFound The job observer found the expected job.
     * @param jobNotFound The job observer confirmed that the expected job is not on the cluster.
     *     Both flags are false when the job lookup failed.
     */
    public void observe(
            FlinkResourceContext<FlinkDeployment> ctx, boolean jobFound, boolean jobNotFound) {
        var status = ctx.getResource().getStatus();
        var clusterInfo = status.getClusterInfo();
        if (!isSupported(ctx) || isJobSuspended(ctx)) {
            clear(status);
            return;
        }

        if (!jobFound && !jobNotFound) {
            // The job lookup failed, the application lookup would most likely fail too.
            // Keep the previous observation and try again later.
            LOG.debug("Job lookup failed, skipping application observation");
            return;
        }

        if (jobFound) {
            // The job is back, clear the alert even if the application lookup below fails
            clearApplicationWithoutJob(status);
        }

        List<CustomApplicationDetails> applications;
        try {
            applications = ctx.getFlinkService().getApplications(ctx.getObserveConfig());
        } catch (Exception e) {
            // Error while accessing the rest api, will try again later...
            LOG.warn("Exception while getting application status", e);
            return;
        }

        var applicationOpt = selectApplication(applications);
        if (applicationOpt.isEmpty()) {
            LOG.debug("No application found on the cluster");
            clusterInfo.remove(FIELD_NAME_APPLICATION_ID);
            clusterInfo.remove(FIELD_NAME_APPLICATION_STATE);
        } else {
            var application = applicationOpt.get();
            var previousState = clusterInfo.get(FIELD_NAME_APPLICATION_STATE);
            clusterInfo.put(FIELD_NAME_APPLICATION_ID, application.getApplicationId());
            clusterInfo.put(FIELD_NAME_APPLICATION_STATE, application.getStatus());
            if (!application.getStatus().equals(previousState)) {
                LOG.info(
                        "Application {} state changed from {} to {}",
                        application.getApplicationId(),
                        previousState,
                        application.getStatus());
            }
        }

        boolean applicationWithoutJob =
                jobNotFound
                        && applicationOpt.map(CustomApplicationDetails::isTerminal).orElse(true);
        if (!applicationWithoutJob) {
            clearApplicationWithoutJob(status);
            return;
        }

        if (!isDeploymentOlderThanReadinessTimeout(ctx)) {
            // A freshly started JobManager may not have submitted the job yet
            LOG.debug(
                    "Job not found on a deployment within the readiness timeout, not reporting yet");
            clearApplicationWithoutJob(status);
            return;
        }

        var jobId = status.getJobStatus().getJobId();
        var message =
                applicationOpt
                        .map(
                                application ->
                                        String.format(
                                                MSG_APPLICATION_TERMINAL_WITHOUT_JOB,
                                                application.getApplicationId(),
                                                application.getStatus(),
                                                jobId))
                        .orElseGet(
                                // Flink does not list an application whose terminal result was
                                // already committed, it only skips its submission.
                                () -> String.format(MSG_APPLICATION_MISSING_WITHOUT_JOB, jobId));
        recordApplicationWithoutJob(ctx, message);
    }

    /** Removes the application state and the alert, used when the JobManager is not ready. */
    public void clear(FlinkDeploymentStatus status) {
        status.getClusterInfo().remove(FIELD_NAME_APPLICATION_ID);
        status.getClusterInfo().remove(FIELD_NAME_APPLICATION_STATE);
        clearApplicationWithoutJob(status);
    }

    /** The alert is active, the error must survive the regular health check. */
    public static boolean isApplicationWithoutJob(FlinkDeploymentStatus status) {
        return status.getClusterInfo().containsKey(FIELD_NAME_APPLICATION_WITHOUT_JOB);
    }

    private void recordApplicationWithoutJob(
            FlinkResourceContext<FlinkDeployment> ctx, String message) {
        var resource = ctx.getResource();
        resource.getStatus().getClusterInfo().put(FIELD_NAME_APPLICATION_WITHOUT_JOB, "true");
        resource.getStatus().setError(message);
        boolean newEvent =
                eventRecorder.triggerEvent(
                        resource,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.ApplicationTerminalWithoutJob,
                        EventRecorder.Component.Job,
                        message,
                        ctx.getKubernetesClient());
        if (newEvent) {
            LOG.error(message);
        } else {
            LOG.debug(message);
        }
    }

    private static void clearApplicationWithoutJob(FlinkDeploymentStatus status) {
        if (isApplicationWithoutJob(status)) {
            status.getClusterInfo().remove(FIELD_NAME_APPLICATION_WITHOUT_JOB);
            status.setError(null);
        }
    }

    private static boolean isDeploymentOlderThanReadinessTimeout(
            FlinkResourceContext<FlinkDeployment> ctx) {
        var readinessTimeout =
                ctx.getObserveConfig()
                        .get(KubernetesOperatorConfigOptions.DEPLOYMENT_READINESS_TIMEOUT);
        var deployedAt =
                Instant.ofEpochMilli(
                        ctx.getResource()
                                .getStatus()
                                .getReconciliationStatus()
                                .getReconciliationTimestamp());
        return Instant.now().minus(readinessTimeout).isAfter(deployedAt);
    }

    /**
     * Pick the application to report. Application clusters run a single application, but after a
     * JobManager failover a recovered terminal application may be listed next to the live one. In
     * that case prefer the one that is not terminal, then the most recently started one.
     */
    static Optional<CustomApplicationDetails> selectApplication(
            List<CustomApplicationDetails> applications) {
        if (applications == null || applications.isEmpty()) {
            return Optional.empty();
        }
        if (applications.size() > 1) {
            LOG.debug(
                    "Expected a single application on the cluster but found {}: {}",
                    applications.size(),
                    applications);
        }
        return applications.stream()
                .filter(a -> a.getStatus() != null)
                .min(
                        Comparator.comparing(CustomApplicationDetails::isTerminal)
                                .thenComparing(
                                        CustomApplicationDetails::getStartTime,
                                        Comparator.reverseOrder()));
    }

    private static boolean isJobSuspended(FlinkResourceContext<FlinkDeployment> ctx) {
        var lastReconciledSpec =
                ctx.getResource()
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec();
        return lastReconciledSpec == null
                || lastReconciledSpec.getJob() == null
                || lastReconciledSpec.getJob().getState() == JobState.SUSPENDED;
    }

    static boolean isSupported(FlinkResourceContext<FlinkDeployment> ctx) {
        FlinkVersion version = ctx.getObserveConfig().get(FLINK_VERSION);
        return version != null && version.isEqualOrNewer(FlinkVersion.v2_3);
    }
}
