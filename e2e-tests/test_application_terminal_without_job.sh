#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This script tests that the operator surfaces the Flink 2.3 application state
# and warns when the application is terminal while the job is missing (FLINK-40467):
# 1. Run a batch job with Kubernetes HA on a persistent volume until it FINISHES.
# 2. Verify that the application state is recorded in the clusterInfo status.
# 3. Kill the JobManager. On restart Flink finds the recorded terminal result in the
#    application result store and does not submit the job again.
# 4. Verify that the operator emits the ApplicationTerminalWithoutJob warning event
#    and records the error in the status.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="flink-example-wordcount-batch-ha"
APPLICATION_YAML="${SCRIPT_DIR}/data/flinkdep-batch-ha-cr.yaml"
APPLICATION_IDENTIFIER="flinkdep/$CLUSTER_ID"
TIMEOUT=300

flink_version=$(grep -E "^\s*flinkVersion:" "$APPLICATION_YAML" | head -n1 | awk '{print $2}')
case "$flink_version" in
  v1_*|v2_0|v2_1|v2_2)
    echo "Application state is only exposed by Flink 2.3 and above, skipping for $flink_version"
    exit 0
    ;;
esac

on_exit cleanup_and_exit "$APPLICATION_YAML" $TIMEOUT $CLUSTER_ID

retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

wait_for_jobmanager_running $CLUSTER_ID $TIMEOUT
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' FINISHED $TIMEOUT || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.clusterInfo.application-state' FINISHED $TIMEOUT || exit 1

jm_pod_name=$(get_jm_pod_name $CLUSTER_ID)
wait_for_logs "$jm_pod_name" "Application completed SUCCESSFULLY" ${TIMEOUT} || exit 1

# Restart the JobManager. The HA data of the finished job is gone, but the
# application result store still records the FINISHED application, so Flink
# skips the application and does not submit the job again.
kubectl exec $jm_pod_name -c flink-main-container -- kill 1

wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RECONCILING $TIMEOUT || exit 1

# The warning event and the status error must both name the missing job. The error is
# kept through the regular health check until the job is found again.
job_id=$(kubectl get -oyaml $APPLICATION_IDENTIFIER | yq '.status.jobStatus.jobId')
wait_for_event FlinkDeployment $CLUSTER_ID ".reason == \"ApplicationTerminalWithoutJob\" and (.message | contains(\"$job_id\"))" $TIMEOUT || exit 1

echo "Waiting for $APPLICATION_IDENTIFIER status error to name job $job_id..."
for i in $(seq 1 ${TIMEOUT}); do
  error=$(kubectl get -oyaml $APPLICATION_IDENTIFIER | yq '.status.error')
  if [[ "$error" == *"$job_id"* && "$error" == *"application result store"* ]]; then
    echo "Successfully verified that the status error names the missing job."
    break
  fi
  if [ "$i" == "$TIMEOUT" ]; then
    echo "Status error does not name the missing job: $error"
    exit 1
  fi
  sleep 1
done

echo "Successfully ran the application terminal without job test"
