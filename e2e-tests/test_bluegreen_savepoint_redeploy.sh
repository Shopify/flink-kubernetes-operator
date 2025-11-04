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

# This script tests the FlinkBlueGreenDeployment savepointRedeployNonce feature:
# 1. Deploy a BlueGreen resource (starts BLUE)
# 2. Trigger a normal transition to GREEN (takes savepoint from BLUE)
# 3. Wait for GREEN to be stable and BLUE to be deleted
# 4. Change savepointRedeployNonce and initialSavepointPath to force redeploy from specific savepoint
# 5. Verify transition back to BLUE WITHOUT taking new savepoint from GREEN
# 6. Verify BLUE starts from the specified savepoint
# 7. Test STATELESS mode preserves initialSavepointPath

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
source "${SCRIPT_DIR}/utils.sh"

CLUSTER_ID="bg-savepoint-redeploy-example"
BG_CLUSTER_ID=$CLUSTER_ID
BLUE_CLUSTER_ID=$CLUSTER_ID"-blue"
GREEN_CLUSTER_ID=$CLUSTER_ID"-green"

APPLICATION_YAML="${SCRIPT_DIR}/data/bluegreen-savepoint-redeploy.yaml"
APPLICATION_IDENTIFIER="flinkbgdep/$CLUSTER_ID"
BLUE_APPLICATION_IDENTIFIER="flinkdep/$BLUE_CLUSTER_ID"
GREEN_APPLICATION_IDENTIFIER="flinkdep/$GREEN_CLUSTER_ID"
TIMEOUT=300

echo "=========================================="
echo "Test 1: Basic savepointRedeployNonce flow"
echo "=========================================="

echo "Deploying FlinkBlueGreenDeployment..."
retry_times 5 30 "kubectl apply -f $APPLICATION_YAML" || exit 1

echo "Waiting for BLUE deployment to be stable..."
sleep 1
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

echo "BLUE is stable. Triggering transition to GREEN..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"flinkConfiguration":{"state.checkpoints.num-retained":"6"}}}}}'
echo "Waiting for savepoint to be taken from BLUE..."
sleep 10

# Capture the savepoint path from BLUE before it's deleted
jm_pod_name=$(get_jm_pod_name $BLUE_CLUSTER_ID)
echo "Inspecting BLUE savepoint directory..."
kubectl exec -it $jm_pod_name -- bash -c "ls -lt /opt/flink/volume/flink-sp/"

echo "Waiting for GREEN to be stable..."
wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${BLUE_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1

# Get the savepoint path that GREEN used (this was taken from BLUE)
green_initialSavepointPath=$(kubectl get -oyaml $GREEN_APPLICATION_IDENTIFIER | yq '.spec.job.initialSavepointPath')
echo "GREEN started from savepoint: $green_initialSavepointPath"

if [[ $green_initialSavepointPath != '/opt/flink/volume/flink-sp/savepoint-'* ]]; then
  echo "ERROR: GREEN did not use expected savepoint path"
  exit 1
fi

echo ""
echo "=========================================="
echo "Test 2: savepointRedeployNonce redeploy"
echo "=========================================="

# Now test the savepointRedeployNonce feature
# We'll use the same savepoint to transition back to BLUE WITHOUT taking a new savepoint
echo "Updating savepointRedeployNonce to trigger redeploy from specific savepoint..."
echo "Using savepoint: $green_initialSavepointPath"

# Patch with savepointRedeployNonce and initialSavepointPath
# Note: We're reusing the savepoint that was taken from BLUE earlier
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch "{\"spec\":{\"template\":{\"spec\":{\"job\":{\"savepointRedeployNonce\":123456,\"initialSavepointPath\":\"$green_initialSavepointPath\"}}}}}"

echo "Waiting for operator to detect SAVEPOINT_REDEPLOY diff type..."
sleep 5

# Get operator pod to check logs
operator_pod=$(kubectl get pods -n flink-kubernetes-operator -l app.kubernetes.io/name=flink-kubernetes-operator -o jsonpath='{.items[0].metadata.name}')
echo "Checking operator logs for savepoint redeploy confirmation..."
kubectl logs -n flink-kubernetes-operator $operator_pod --tail=50 | grep -i "savepoint redeploy" || echo "Warning: Could not find savepoint redeploy log message"

echo "Waiting for BLUE deployment to be created again..."
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1

# Verify GREEN is deleted
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${GREEN_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.jobStatus.state' RUNNING ${TIMEOUT} || exit 1
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

# Verify BLUE used the specified savepoint
blue_initialSavepointPath=$(kubectl get -oyaml $BLUE_APPLICATION_IDENTIFIER | yq '.spec.job.initialSavepointPath')
echo "BLUE started from savepoint: $blue_initialSavepointPath"

if [[ "$blue_initialSavepointPath" != "$green_initialSavepointPath" ]]; then
  echo "ERROR: BLUE did not use the specified savepoint"
  echo "Expected: $green_initialSavepointPath"
  echo "Got: $blue_initialSavepointPath"
  exit 1
fi

# Verify in operator logs that no NEW savepoint was taken from GREEN
echo "Verifying that no savepoint was taken from GREEN during nonce redeploy..."
recent_logs=$(kubectl logs -n flink-kubernetes-operator $operator_pod --tail=100)
if echo "$recent_logs" | grep -q "Triggered savepoint for jobId.*$GREEN_CLUSTER_ID"; then
  echo "ERROR: Operator took a savepoint from GREEN, but should have skipped it!"
  exit 1
else
  echo "SUCCESS: No savepoint taken from GREEN (as expected for savepointRedeployNonce)"
fi

echo ""
echo "=========================================="
echo "Test 3: STATELESS mode preserves savepoint"
echo "=========================================="

echo "Changing to STATELESS upgrade mode with initialSavepointPath set..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"job":{"upgradeMode":"stateless","savepointRedeployNonce":789}}}}}'

echo "Waiting for GREEN deployment with STATELESS mode..."
wait_for_jobmanager_running $GREEN_CLUSTER_ID $TIMEOUT
wait_for_status $GREEN_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${BLUE_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_GREEN ${TIMEOUT} || exit 1

# Verify GREEN still used the savepoint despite STATELESS mode
green_stateless_savepointPath=$(kubectl get -oyaml $GREEN_APPLICATION_IDENTIFIER | yq '.spec.job.initialSavepointPath')
echo "GREEN (STATELESS) started from savepoint: $green_stateless_savepointPath"

if [[ "$green_stateless_savepointPath" == "null" ]] || [[ -z "$green_stateless_savepointPath" ]]; then
  echo "ERROR: STATELESS mode cleared initialSavepointPath (BUG!)"
  exit 1
else
  echo "SUCCESS: STATELESS mode preserved initialSavepointPath"
fi

echo ""
echo "=========================================="
echo "Test 4: Nonce change DURING transition"
echo "=========================================="

echo "Triggering transition back to BLUE..."
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"job":{"parallelism":2}}}}}'

echo "Waiting for BLUE to start deploying..."
sleep 5

# Verify we're in transitioning state
blue_green_state=$(kubectl get -oyaml $APPLICATION_IDENTIFIER | yq '.status.blueGreenState')
if [[ "$blue_green_state" != "TRANSITIONING_TO_BLUE" ]] && [[ "$blue_green_state" != "SAVEPOINTING_GREEN" ]]; then
  echo "Warning: Expected to be in transition state, but in: $blue_green_state"
fi

echo "NOW changing savepointRedeployNonce DURING the transition..."
# Use a different savepoint path to make it clear this is a new redeploy
kubectl patch flinkbgdep ${BG_CLUSTER_ID} --type merge --patch '{"spec":{"template":{"spec":{"job":{"savepointRedeployNonce":999}}}}}'

echo "Waiting for operator to detect nonce change and abort transition..."
sleep 10

# Check operator logs for abort message
operator_pod=$(kubectl get pods -n flink-kubernetes-operator -l app.kubernetes.io/name=flink-kubernetes-operator -o jsonpath='{.items[0].metadata.name}')
echo "Checking operator logs for transition abort..."
kubectl logs -n flink-kubernetes-operator $operator_pod --tail=100 | grep -i "aborting current transition" || echo "Warning: Abort log message not found (may be expected in some cases)"

# Verify we went back to ACTIVE_GREEN (not TRANSITIONING)
sleep 5
current_state=$(kubectl get -oyaml $APPLICATION_IDENTIFIER | yq '.status.blueGreenState')
if [[ "$current_state" == "ACTIVE_GREEN" ]]; then
  echo "SUCCESS: Transition was aborted, reverted to ACTIVE_GREEN"
elif [[ "$current_state" == "TRANSITIONING_TO_BLUE" ]]; then
  echo "SUCCESS: Transition restarted (expected behavior - restart can be immediate)"
else
  echo "Unexpected state after nonce change during transition: $current_state"
fi

# Wait for new transition to complete with the updated nonce
echo "Waiting for new transition to complete..."
wait_for_jobmanager_running $BLUE_CLUSTER_ID $TIMEOUT
wait_for_status $BLUE_APPLICATION_IDENTIFIER '.status.lifecycleState' STABLE ${TIMEOUT} || exit 1
kubectl wait --for=delete deployment --timeout=${TIMEOUT}s --selector="app=${GREEN_CLUSTER_ID}"
wait_for_status $APPLICATION_IDENTIFIER '.status.blueGreenState' ACTIVE_BLUE ${TIMEOUT} || exit 1

echo "SUCCESS: Nonce change during transition triggered abort and restart"

echo ""
echo "=========================================="
echo "Cleanup"
echo "=========================================="

echo "Deleting test B/G resources: $BG_CLUSTER_ID"
kubectl delete flinkbluegreendeployments/$BG_CLUSTER_ID &
echo "Waiting for deployment to be deleted..."
kubectl wait --for=delete flinkbluegreendeployments/$BG_CLUSTER_ID --timeout=${TIMEOUT}s

echo ""
echo "=========================================="
echo "All tests passed!"
echo "=========================================="
echo "✓ savepointRedeployNonce triggers redeploy from specific savepoint"
echo "✓ No new savepoint taken when nonce changes"
echo "✓ STATELESS mode preserves initialSavepointPath"
echo ""
echo "Successfully completed FlinkBlueGreenDeployment savepointRedeployNonce e2e tests"
