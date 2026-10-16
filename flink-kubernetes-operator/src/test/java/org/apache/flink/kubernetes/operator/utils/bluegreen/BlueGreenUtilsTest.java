/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.Savepoint;
import org.apache.flink.kubernetes.operator.api.status.SavepointFormatType;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BlueGreenUtils}. */
public class BlueGreenUtilsTest {

    private static final String TEST_NAMESPACE = "test-namespace";

    @Test
    public void testPrepareFlinkDeploymentWithoutNameReplacement() {
        String parentDeploymentName = "my-app";
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment(parentDeploymentName, TEST_NAMESPACE);

        // Add configuration that contains the deployment name in values
        Map<String, String> flinkConfig =
                bgDeployment.getSpec().getTemplate().getSpec().getFlinkConfiguration().asFlatMap();
        flinkConfig.put(
                "high-availability.storageDir",
                "s3://" + parentDeploymentName + "/highavailability");
        flinkConfig.put("metrics.scope.jm", parentDeploymentName + ".jm");
        bgDeployment.getSpec().getTemplate().getSpec().setFlinkConfiguration(flinkConfig);

        BlueGreenContext context = createContext(bgDeployment);

        // Test: Prepare a BLUE deployment
        FlinkDeployment blueDeployment =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.BLUE,
                        null,
                        true,
                        bgDeployment.getMetadata());

        // Verify child deployment name is correctly set in metadata
        String expectedChildName = parentDeploymentName + "-blue";
        assertEquals(expectedChildName, blueDeployment.getMetadata().getName());

        // Verify configuration values that contain the parent name are NOT replaced
        Map<String, String> resultFlinkConfig =
                blueDeployment.getSpec().getFlinkConfiguration().asFlatMap();
        assertEquals(
                "s3://" + parentDeploymentName + "/highavailability",
                resultFlinkConfig.get("high-availability.storageDir"));
        assertEquals(parentDeploymentName + ".jm", resultFlinkConfig.get("metrics.scope.jm"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLegacySourceDoesNotOverrideNewDeclaredSizing(boolean missingStatus) {
        var bg = buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        var context = createContext(bg);
        var source = new FlinkDeployment();
        source.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-app-blue")
                        .withNamespace(TEST_NAMESPACE)
                        .build());
        source.setStatus(new FlinkDeploymentStatus());
        source.setSpec(SpecUtils.clone(bg.getSpec().getTemplate().getSpec()));
        source.getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:24");
        if (missingStatus) {
            source.setStatus(null);
        }
        var deployments = new FlinkBlueGreenDeployments();
        deployments.setFlinkDeploymentBlue(source);
        context =
                new BlueGreenContext(
                        bg, bg.getStatus(), null, deployments, context.getCtxFactory());

        var target =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.GREEN,
                        null,
                        false,
                        bg.getMetadata(),
                        source);

        assertTemplateSpec(bg, target);
    }

    @Test
    public void testInitialDeploymentDoesNotNeedParentHistory() {
        var bg = buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        var context = createContext(bg);
        bg.getStatus().setLastReconciledSpec(null);
        var target =
                BlueGreenUtils.prepareFlinkDeployment(
                        context, BlueGreenDeploymentType.BLUE, null, true, bg.getMetadata());
        assertTemplateSpec(bg, target);
    }

    @Test
    public void testSavepointRequiredBasedOnUpgradeMode() {
        // SAVEPOINT mode requires savepoint
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.SAVEPOINT);
        BlueGreenContext context = createContext(bgDeployment);
        assertTrue(BlueGreenUtils.isSavepointRequired(context));

        // LAST_STATE mode requires savepoint
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.LAST_STATE);
        assertTrue(BlueGreenUtils.isSavepointRequired(context));

        // STATELESS mode does not require savepoint
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.STATELESS);
        assertFalse(BlueGreenUtils.isSavepointRequired(context));
    }

    @Test
    public void testPrepareFlinkDeploymentStatelessInitialSavepointPath() {
        // Setup: STATELESS mode with initialSavepointPath set
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.STATELESS);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setInitialSavepointPath("s3://bucket/savepoint-xyz");

        BlueGreenContext context = createContext(bgDeployment);

        // Act: Prepare deployment with null lastCheckpoint (STATELESS transition)
        FlinkDeployment result =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.GREEN,
                        null, // No lastCheckpoint
                        false, // Not first deployment
                        bgDeployment.getMetadata());

        // Assert: initialSavepointPath should be used for STATELESS
        assertNotNull(result.getSpec().getJob().getInitialSavepointPath());
    }

    @Test
    public void testNullLastCheckpointUsesInitialSavepointPath() {
        // lastCheckpoint=null -> use initialSavepointPath from spec
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.SAVEPOINT);
        String initialPath = "s3://bucket/user-specified-savepoint";
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setInitialSavepointPath(initialPath);

        BlueGreenContext context = createContext(bgDeployment);

        FlinkDeployment result =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.GREEN,
                        null, // null = nonce changed, no new savepoint taken
                        false,
                        bgDeployment.getMetadata());

        assertEquals(initialPath, result.getSpec().getJob().getInitialSavepointPath());
    }

    @Test
    public void testNormalTransitionUsesFreshSavepoint() {
        // Normal transition → take fresh savepoint from running job → use that, not
        // initialSavepointPath
        FlinkBlueGreenDeployment bgDeployment =
                buildBlueGreenDeployment("test-app", TEST_NAMESPACE);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setUpgradeMode(UpgradeMode.SAVEPOINT);
        bgDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getJob()
                .setInitialSavepointPath("s3://bucket/ignored");

        BlueGreenContext context = createContext(bgDeployment);

        String freshSavepoint = "s3://bucket/fresh-savepoint-from-running-job";
        Savepoint triggered =
                Savepoint.of(
                        freshSavepoint, SnapshotTriggerType.UPGRADE, SavepointFormatType.CANONICAL);

        FlinkDeployment result =
                BlueGreenUtils.prepareFlinkDeployment(
                        context,
                        BlueGreenDeploymentType.GREEN,
                        triggered, // Fresh savepoint provided
                        false,
                        bgDeployment.getMetadata());

        assertEquals(freshSavepoint, result.getSpec().getJob().getInitialSavepointPath());
    }

    private void assertTemplateSpec(FlinkBlueGreenDeployment parent, FlinkDeployment child) {
        var actual = SpecUtils.clone(child.getSpec());
        assertNotNull(actual.getFlinkConfiguration().remove(BlueGreenParallelism.INTENT_KEY));
        assertEquals(parent.getSpec().getTemplate().getSpec(), actual);
    }

    private static FlinkBlueGreenDeployment buildBlueGreenDeployment(
            String name, String namespace) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withUid(UUID.randomUUID().toString())
                        .build());

        var flinkDeploymentSpec = TestUtils.buildApplicationCluster(FlinkVersion.v1_20).getSpec();
        flinkDeploymentSpec.getJob().setUpgradeMode(UpgradeMode.STATELESS);

        var bgDeploymentSpec =
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        null,
                        FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build());

        deployment.setSpec(bgDeploymentSpec);
        return deployment;
    }

    private BlueGreenContext createContext(FlinkBlueGreenDeployment bgDeployment) {
        FlinkBlueGreenDeploymentStatus status = new FlinkBlueGreenDeploymentStatus();
        status.setLastReconciledSpec(SpecUtils.writeSpecAsJSON(bgDeployment.getSpec(), "spec"));
        bgDeployment.setStatus(status);

        var config = new Configuration();
        config.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        var factory =
                new TestingFlinkResourceContextFactory(
                        new FlinkConfigManager(config),
                        TestUtils.createTestMetricGroup(new Configuration()),
                        null,
                        null);
        return new BlueGreenContext(bgDeployment, status, null, null, factory);
    }
}
