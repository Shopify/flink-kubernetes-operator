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
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.service.CustomApplicationDetails;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.FLINK_VERSION;

/**
 * Observes the Flink 2.3+ application running on an application cluster and records its state in
 * {@code clusterInfo}. If the application is terminal, or not listed at all, while the expected job
 * is not found, Flink skipped the job submission because of a recorded terminal result. The
 * deployment would otherwise sit in {@code RECONCILING} with no explanation, see FLINK-40467.
 */
public class ApplicationStatusObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStatusObserver.class);

    public static final String FIELD_NAME_APPLICATION_ID = "application-id";
    public static final String FIELD_NAME_APPLICATION_STATE = "application-state";

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

    public void observe(FlinkResourceContext<FlinkDeployment> ctx, boolean jobMissing) {
        var status = ctx.getResource().getStatus();
        var clusterInfo = status.getClusterInfo();
        if (!isSupported(ctx) || isJobSuspended(ctx)) {
            clusterInfo.remove(FIELD_NAME_APPLICATION_ID);
            clusterInfo.remove(FIELD_NAME_APPLICATION_STATE);
            clearApplicationWithoutJobError(status);
            return;
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
            if (jobMissing) {
                onMissingApplicationWithoutJob(ctx);
            } else {
                clearApplicationWithoutJobError(status);
            }
            return;
        }

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

        if (application.isTerminal() && jobMissing) {
            onTerminalApplicationWithoutJob(ctx, application);
        } else {
            clearApplicationWithoutJobError(status);
        }
    }

    private void onTerminalApplicationWithoutJob(
            FlinkResourceContext<FlinkDeployment> ctx, CustomApplicationDetails application) {
        recordApplicationWithoutJob(
                ctx,
                String.format(
                        MSG_APPLICATION_TERMINAL_WITHOUT_JOB,
                        application.getApplicationId(),
                        application.getStatus(),
                        ctx.getResource().getStatus().getJobStatus().getJobId()));
    }

    /**
     * Flink does not list an application whose terminal result was already committed to the
     * application result store, it only skips its submission. An application cluster without any
     * application and without the expected job is therefore the same failure as a terminal
     * application without a job.
     */
    private void onMissingApplicationWithoutJob(FlinkResourceContext<FlinkDeployment> ctx) {
        recordApplicationWithoutJob(
                ctx,
                String.format(
                        MSG_APPLICATION_MISSING_WITHOUT_JOB,
                        ctx.getResource().getStatus().getJobStatus().getJobId()));
    }

    private void recordApplicationWithoutJob(
            FlinkResourceContext<FlinkDeployment> ctx, String message) {
        var resource = ctx.getResource();
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

    static boolean isApplicationWithoutJobError(@Nullable String error) {
        return error != null && error.endsWith(MSG_APPLICATION_WITHOUT_JOB_HINT);
    }

    private static void clearApplicationWithoutJobError(FlinkDeploymentStatus status) {
        if (isApplicationWithoutJobError(status.getError())) {
            status.setError(null);
        }
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
            LOG.warn(
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
