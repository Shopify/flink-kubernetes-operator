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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.ConfigOption;
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
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.SnapshotType;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlueGreenParallelismInheritanceTest {
    private FlinkBlueGreenDeployment parent;
    private FlinkDeployment source;
    private BlueGreenContext context;
    private FlinkConfigManager configManager;

    @BeforeEach
    void setup() {
        var defaults = new Configuration();
        defaults.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaults.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 64);
        configManager = new FlinkConfigManager(defaults);
        parent = new FlinkBlueGreenDeployment();
        parent.setStatus(new FlinkBlueGreenDeploymentStatus());
        parent.setMetadata(
                new ObjectMetaBuilder()
                        .withName("app")
                        .withNamespace("test")
                        .withUid("parent")
                        .build());
        var spec = TestUtils.buildApplicationCluster(FlinkVersion.v1_20).getSpec();
        spec.getJob().setUpgradeMode(UpgradeMode.STATELESS);
        parent.setSpec(
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        null,
                        FlinkDeploymentTemplateSpec.builder().spec(spec).build()));
        parent.getStatus()
                .setLastReconciledSpec(SpecUtils.writeSpecAsJSON(parent.getSpec(), "spec"));
        source = new FlinkDeployment();
        source.setStatus(new FlinkDeploymentStatus());
        source.setMetadata(
                new ObjectMetaBuilder()
                        .withName("app-blue")
                        .withNamespace("test")
                        .withGeneration(1L)
                        .build());
        source.setSpec(SpecUtils.clone(spec));
        var deployed = SpecUtils.clone(spec);
        deployed.getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:24");
        source.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployed, source);
        source.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        var deployments = new FlinkBlueGreenDeployments();
        deployments.setFlinkDeploymentBlue(source);
        var factory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(new Configuration()),
                        null,
                        null);
        context = new BlueGreenContext(parent, parent.getStatus(), null, deployments, factory);
    }

    @Test
    void effectiveSizingIsInheritedWithoutRawSpecOverrides() {
        assertEquals(Map.of("vertex", "24"), overrides());
    }

    @Test
    void recordedVertexTargetCanExceedThePipelineDefaultMaximum() {
        var defaults = configManager.getDefaultConfig();
        defaults.set(PipelineOptions.MAX_PARALLELISM, 16);
        configManager.updateDefaultConfig(defaults);
        assertEquals(Map.of("vertex", "24"), overrides());
    }

    @Test
    void changedEffectiveMaximumUsesTheTemplate() {
        targetDefault(AutoScalerOptions.VERTEX_MAX_PARALLELISM, "16");
        assertTrue(overrides().isEmpty());
    }

    @Test
    void changedEffectiveMinimumUsesTheTemplate() {
        targetDefault(AutoScalerOptions.VERTEX_MIN_PARALLELISM, "4");
        assertTrue(overrides().isEmpty());
    }

    @Test
    void changedEffectivePipelineMaximumUsesTheTemplate() {
        var defaults = configManager.getDefaultConfig();
        defaults.set(PipelineOptions.MAX_PARALLELISM, 128);
        configManager.updateDefaultConfig(defaults);
        targetDefault(PipelineOptions.MAX_PARALLELISM, "64");
        assertTrue(overrides().isEmpty());
    }

    @Test
    void acknowledgedButUnappliedResetDoesNotResurrectSizing() {
        source.getSpec().getJob().setAutoscalerResetNonce(1L);
        source.getMetadata().setGeneration(2L);
        source.getStatus().setObservedGeneration(2L);
        assertTrue(overrides().isEmpty());
    }

    @Test
    void pendingChildWithoutStatusRetainsPreparedSizing() {
        source = prepared();
        parent.getSpec().getTemplate().getSpec().setImage("flink:pending-update");
        assertEquals(Map.of("vertex", "24"), overrides());
    }

    @Test
    void snapshotAcknowledgementDoesNotConsumePendingOverride() {
        var oldStatus = SpecUtils.clone(source.getStatus());
        parent.getSpec()
                .getTemplate()
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:7");
        source = prepared();
        source.setStatus(oldStatus);
        source.getMetadata().setGeneration(2L);
        source.getSpec().getJob().setSavepointTriggerNonce(13L);
        ReconciliationUtils.updateLastReconciledSnapshotTriggerNonce(
                SnapshotTriggerType.MANUAL, source, SnapshotType.SAVEPOINT);
        assertEquals(
                source.getMetadata().getGeneration(), source.getStatus().getObservedGeneration());
        assertEquals(Map.of("vertex", "7"), overrides());
    }

    @Test
    void changedNamespaceDefaultsDoNotReplayTheOldPolicy() {
        source = prepared();
        source.setStatus(new FlinkDeploymentStatus());
        source.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(source.getSpec(), source);
        source.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        var defaults = configManager.getDefaultConfig();
        defaults.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 16);
        configManager.updateDefaultConfig(defaults);
        assertTrue(overrides().isEmpty());
    }

    @Test
    void legacyTargetCanRemainAboveTheSoftAutoscalerBound() {
        var defaults = configManager.getDefaultConfig();
        defaults.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 16);
        configManager.updateDefaultConfig(defaults);
        assertEquals(Map.of("vertex", "24"), overrides());
    }

    @Test
    void malformedRuntimeTargetsUseTheTemplate() {
        var deployed = source.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        deployed.getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:not-a-number");
        source.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployed, source);
        assertTrue(overrides().isEmpty());
    }

    @Test
    void changedCentralSeedUsesTheNewDefault() {
        verifyCentralSeedChange(Map.of("vertex", "7"));
    }

    @Test
    void removedCentralSeedUsesTheTemplate() {
        verifyCentralSeedChange(Map.of());
    }

    @Test
    void unchangedRecordedCentralSeedRetainsAutoscaledSizing() {
        recordCentrallySeededChild();
        assertEquals(Map.of("vertex", "24"), overrides());
    }

    @Test
    void unrecordedCentralSeedRemainsAuthoritativeForLegacyChildren() {
        var defaults = configManager.getDefaultConfig();
        defaults.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of("vertex", "7"));
        configManager.updateDefaultConfig(defaults);
        var target = prepared();
        assertEquals(
                Map.of("vertex", "7"),
                configManager
                        .getDeployConfig(target.getMetadata(), target.getSpec())
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));
    }

    private void recordCentrallySeededChild() {
        var defaults = configManager.getDefaultConfig();
        defaults.set(PipelineOptions.PARALLELISM_OVERRIDES, Map.of("vertex", "2"));
        configManager.updateDefaultConfig(defaults);
        source = prepared();
        source.setStatus(new FlinkDeploymentStatus());
        var deployed = SpecUtils.clone(source.getSpec());
        deployed.getFlinkConfiguration()
                .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:24");
        source.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployed, source);
        source.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
    }

    private void verifyCentralSeedChange(Map<String, String> next) {
        recordCentrallySeededChild();
        var defaults = configManager.getDefaultConfig();
        if (next.isEmpty()) {
            defaults.removeConfig(PipelineOptions.PARALLELISM_OVERRIDES);
        } else {
            defaults.set(PipelineOptions.PARALLELISM_OVERRIDES, next);
        }
        configManager.updateDefaultConfig(defaults);
        var target = prepared();
        assertEquals(
                next,
                configManager
                        .getDeployConfig(target.getMetadata(), target.getSpec())
                        .get(PipelineOptions.PARALLELISM_OVERRIDES));
    }

    @Test
    void unknownProvenanceUsesTheTemplate() {
        source.getSpec()
                .getFlinkConfiguration()
                .put(BlueGreenParallelism.INTENT_KEY, "v99:unknown");
        assertTrue(overrides().isEmpty());
    }

    @Test
    void provenanceEncodingIsStable() {
        var spec = parent.getSpec().getTemplate().getSpec();
        spec.getJob().setParallelism(2);
        spec.getJob().setAutoscalerResetNonce(null);
        spec.getTaskManager().setReplicas(null);
        spec.getFlinkConfiguration().put(PipelineOptions.PARALLELISM_OVERRIDES.key(), "vertex:2");
        spec.getFlinkConfiguration().put(PipelineOptions.MAX_PARALLELISM.key(), "128");
        var encoded =
                prepared()
                        .getSpec()
                        .getFlinkConfiguration()
                        .asFlatMap()
                        .get(BlueGreenParallelism.INTENT_KEY);
        assertEquals(
                "v1:914db67ed1e231770ba9689e09969f72b8d2a9e1f0f42875556912746ed7fc39", encoded);
    }

    @Test
    void disablingAutoscalerThroughResolvedDefaultsUsesTheTemplate() {
        targetDefault(AutoScalerOptions.AUTOSCALER_ENABLED, "false");
        assertTrue(overrides().isEmpty());
    }

    @Test
    void disabledTargetDoesNotDecodeUnusedBoundsOrRecordSizingIntent() {
        targetDefault(AutoScalerOptions.AUTOSCALER_ENABLED, "false");
        targetDefault(AutoScalerOptions.VERTEX_MAX_PARALLELISM, "unused");
        var target = prepared();
        var config = target.getSpec().getFlinkConfiguration();
        assertTrue(config.asConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES).isEmpty());
        assertFalse(config.asFlatMap().containsKey(BlueGreenParallelism.INTENT_KEY));
    }

    @Test
    void disabledSourceDoesNotDecodeUnusedBounds() {
        source.getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.AUTOSCALER_ENABLED.key(), "false");
        source.getSpec()
                .getFlinkConfiguration()
                .put(AutoScalerOptions.VERTEX_MIN_PARALLELISM.key(), "unused");
        assertTrue(overrides().isEmpty());
    }

    private void targetDefault(ConfigOption<?> option, String value) {
        var defaults = configManager.getDefaultConfig();
        defaults.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + FlinkVersion.v1_19
                        + "."
                        + option.key(),
                value);
        configManager.updateDefaultConfig(defaults);
        parent.getSpec().getTemplate().getSpec().setFlinkVersion(FlinkVersion.v1_19);
    }

    private FlinkDeployment prepared() {
        return BlueGreenUtils.prepareFlinkDeployment(
                context, BlueGreenDeploymentType.GREEN, null, false, parent.getMetadata(), source);
    }

    private Map<String, String> overrides() {
        return prepared()
                .getSpec()
                .getFlinkConfiguration()
                .asConfiguration()
                .get(PipelineOptions.PARALLELISM_OVERRIDES);
    }
}
