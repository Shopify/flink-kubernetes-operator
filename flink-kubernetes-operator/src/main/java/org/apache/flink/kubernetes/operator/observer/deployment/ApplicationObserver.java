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
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.observer.ClusterHealthObserver;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SnapshotObserver;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class ApplicationObserver extends AbstractFlinkDeploymentObserver {

    private final SnapshotObserver<FlinkDeployment, FlinkDeploymentStatus> savepointObserver;

    private final ClusterHealthObserver clusterHealthObserver;
    private final ApplicationStatusObserver applicationStatusObserver;

    public ApplicationObserver(EventRecorder eventRecorder) {
        super(eventRecorder);
        this.savepointObserver = new SnapshotObserver<>(eventRecorder);
        this.clusterHealthObserver = new ClusterHealthObserver();
        this.applicationStatusObserver = new ApplicationStatusObserver(eventRecorder);
    }

    @Override
    public void observeInternal(FlinkResourceContext<FlinkDeployment> ctx) {
        super.observeInternal(ctx);
        if (!isJmDeploymentReady(ctx.getResource())) {
            // The application state can only be observed through a ready JobManager, do not keep
            // the state of a JobManager that is gone
            applicationStatusObserver.clear(ctx.getResource().getStatus());
        }
    }

    @Override
    protected void observeFlinkCluster(FlinkResourceContext<FlinkDeployment> ctx) {
        logger.debug("Observing application cluster");
        // One job observer per observation, it carries the job-not-found result of this
        // observation only. The status error may still hold the result of an earlier one.
        var jobStatusObserver = new ApplicationJobObserver(eventRecorder);
        boolean jobFound = jobStatusObserver.observe(ctx);
        applicationStatusObserver.observe(ctx, jobFound, jobStatusObserver.isJobNotFound());
        if (jobFound) {
            var observeConfig = ctx.getObserveConfig();
            savepointObserver.observeSavepointStatus(ctx);
            savepointObserver.observeCheckpointStatus(ctx);
            if (observeConfig.getBoolean(OPERATOR_CLUSTER_HEALTH_CHECK_ENABLED)) {
                clusterHealthObserver.observe(ctx);
            }
        }
    }

    /** The application-without-job error persists until the job is found again. */
    @Override
    protected void clearErrorsIfDeploymentIsHealthy(FlinkDeployment dep) {
        if (ApplicationStatusObserver.isApplicationWithoutJob(dep.getStatus())) {
            return;
        }
        super.clearErrorsIfDeploymentIsHealthy(dep);
    }

    private class ApplicationJobObserver extends JobStatusObserver<FlinkDeployment> {
        private boolean jobNotFound;

        public ApplicationJobObserver(EventRecorder eventRecorder) {
            super(eventRecorder);
        }

        @Override
        public void onTimeout(FlinkResourceContext<FlinkDeployment> ctx) {
            observeJmDeployment(ctx);
        }

        @Override
        protected void onTargetJobNotFound(FlinkResourceContext<FlinkDeployment> ctx) {
            super.onTargetJobNotFound(ctx);
            jobNotFound = true;
        }

        /** The cluster answered the job lookup and the expected job was not listed. */
        boolean isJobNotFound() {
            return jobNotFound;
        }
    }
}
