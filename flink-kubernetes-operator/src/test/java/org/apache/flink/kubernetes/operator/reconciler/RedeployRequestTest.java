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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.RedeploySpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.reconciler.diff.ReflectiveDiffBuilder;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link RedeployRequest}. */
public class RedeployRequestTest {

    @Test
    public void testRedeployNonce() {
        var reconciled = spec();
        var desired = SpecUtils.clone(reconciled);
        assertNoRedeploy(reconciled, desired);

        // First redeploy, without state
        desired.getJob().setRedeploy(RedeploySpec.builder().nonce(1L).build());
        var request = assertRedeploy(reconciled, desired, new RedeployRequest(null, false));
        assertEquals(UpgradeMode.STATELESS, request.getRestoreMode());

        // Only changing the savepoint path does not redeploy
        reconciled = SpecUtils.clone(desired);
        desired.getJob().getRedeploy().setSavepointPath("s3://sp");
        assertNoRedeploy(reconciled, desired);

        // Redeploy from savepoint
        desired.getJob().getRedeploy().setNonce(2L);
        request = assertRedeploy(reconciled, desired, new RedeployRequest("s3://sp", false));
        assertEquals(UpgradeMode.SAVEPOINT, request.getRestoreMode());

        // Clearing the nonce or the redeploy block does not redeploy
        reconciled = SpecUtils.clone(desired);
        desired.getJob().getRedeploy().setNonce(null);
        assertNoRedeploy(reconciled, desired);
        desired.getJob().setRedeploy(null);
        assertNoRedeploy(reconciled, desired);
    }

    @Test
    public void testLegacySavepointRedeployNonce() {
        var reconciled = spec();
        reconciled.getJob().setInitialSavepointPath("s3://initial");
        var desired = SpecUtils.clone(reconciled);

        desired.getJob().setSavepointRedeployNonce(1L);
        assertRedeploy(reconciled, desired, new RedeployRequest("s3://initial", true));

        reconciled = SpecUtils.clone(desired);
        desired.getJob().setSavepointRedeployNonce(null);
        assertNoRedeploy(reconciled, desired);
    }

    @Test
    public void testRedeployNonceTakesPrecedence() {
        var reconciled = spec();
        var desired = SpecUtils.clone(reconciled);
        desired.getJob().setInitialSavepointPath("s3://initial");
        desired.getJob().setSavepointRedeployNonce(1L);
        desired.getJob().setRedeploy(RedeploySpec.builder().nonce(1L).build());
        assertRedeploy(reconciled, desired, new RedeployRequest(null, false));
    }

    @Test
    public void testNoJob() {
        assertEquals(Optional.empty(), RedeployRequest.resolve(null, spec().getJob()));
        assertEquals(Optional.empty(), RedeployRequest.resolve(spec().getJob(), null));
    }

    @Test
    public void testDowngradeReplayAndRemovingBlockBeforeDowngrade() {
        var desired = spec();
        desired.getJob().setRedeploy(RedeploySpec.builder().nonce(7L).build());
        var recorded = SpecUtils.clone(desired);
        assertThat(RedeployRequest.resolve(recorded.getJob(), desired.getJob())).isEmpty();
        // Simulate an older operator dropping the unknown block when it rewrites status.
        recorded.getJob().setRedeploy(null);
        assertThat(RedeployRequest.resolve(recorded.getJob(), desired.getJob()))
                .contains(new RedeployRequest(null, false));
        desired.getJob().setRedeploy(null);
        assertThat(RedeployRequest.resolve(recorded.getJob(), desired.getJob())).isEmpty();
    }

    @Test
    public void testOrdinaryUpdatesPreserveConsumedTriggers() {
        var recorded = spec().getJob();
        recorded.setRedeploy(RedeploySpec.builder().nonce(7L).savepointPath("recorded-sp").build());
        recorded.setSavepointRedeployNonce(3L);
        var desired = spec().getJob();
        RedeployRequest.preserveConsumedNonces(recorded, desired);
        assertThat(desired.getRedeploy())
                .isEqualTo(recorded.getRedeploy())
                .isNotSameAs(recorded.getRedeploy());
        assertThat(desired.getSavepointRedeployNonce()).isEqualTo(3L);
        desired.setRedeploy(RedeploySpec.builder().nonce(8L).build());
        RedeployRequest.preserveConsumedNonces(recorded, desired);
        assertThat(desired.getRedeploy().getNonce()).isEqualTo(8L);
        assertThat(RedeployRequest.resolve(recorded, desired))
                .contains(new RedeployRequest(null, false));
    }

    private static FlinkDeploymentSpec spec() {
        return TestUtils.buildApplicationCluster().getSpec();
    }

    // Both helpers also verify that the resolved request agrees with the spec diff

    private static RedeployRequest assertRedeploy(
            FlinkDeploymentSpec reconciled, FlinkDeploymentSpec desired, RedeployRequest expected) {
        assertEquals(DiffType.SAVEPOINT_REDEPLOY, diffType(reconciled, desired));
        var request = RedeployRequest.resolve(reconciled.getJob(), desired.getJob());
        assertEquals(Optional.of(expected), request);
        return request.get();
    }

    private static void assertNoRedeploy(
            FlinkDeploymentSpec reconciled, FlinkDeploymentSpec desired) {
        assertEquals(DiffType.IGNORE, diffType(reconciled, desired));
        assertEquals(
                Optional.empty(), RedeployRequest.resolve(reconciled.getJob(), desired.getJob()));
    }

    private static DiffType diffType(FlinkDeploymentSpec left, FlinkDeploymentSpec right) {
        return new ReflectiveDiffBuilder<>(
                        KubernetesDeploymentMode.NATIVE,
                        SpecUtils.clone(left),
                        SpecUtils.clone(right))
                .build()
                .getType();
    }
}
