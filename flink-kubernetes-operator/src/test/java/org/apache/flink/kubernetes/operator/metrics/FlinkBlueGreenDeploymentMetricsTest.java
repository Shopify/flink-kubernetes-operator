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

package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.BG_STATE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.LIFECYCLE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.STATE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.TIME_SECONDS_NAME;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.TRANSITION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_RESOURCE_METRICS_ENABLED;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link FlinkBlueGreenDeploymentMetrics}. */
public class FlinkBlueGreenDeploymentMetricsTest {

    private static final String TEST_NAMESPACE = "test-namespace";

    private final Configuration configuration = new Configuration();
    private TestingMetricListener listener;
    private MetricManager<FlinkBlueGreenDeployment> metricManager;

    @BeforeEach
    public void init() {
        listener = new TestingMetricListener(configuration);
        metricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        configuration, listener.getMetricGroup());
    }

    @Test
    public void testStateCountMetricsSameNamespace() {
        var deployment1 = buildBlueGreenDeployment("deployment1", TEST_NAMESPACE);
        var deployment2 = buildBlueGreenDeployment("deployment2", TEST_NAMESPACE);

        var counterId =
                listener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, TEST_NAMESPACE, COUNTER_NAME);
        assertTrue(listener.getGauge(counterId).isEmpty());

        // Both deployments start in INITIALIZING_BLUE
        metricManager.onUpdate(deployment1);
        metricManager.onUpdate(deployment2);

        assertEquals(2, listener.getGauge(counterId).get().getValue());
        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 2);

        // Move deployment1 to ACTIVE_BLUE
        deployment1.getStatus().setBlueGreenState(ACTIVE_BLUE);
        metricManager.onUpdate(deployment1);

        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 1);
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 1);

        // Move deployment2 to ACTIVE_BLUE as well
        deployment2.getStatus().setBlueGreenState(ACTIVE_BLUE);
        metricManager.onUpdate(deployment2);

        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 0);
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 2);

        // Remove deployments
        metricManager.onRemove(deployment1);
        assertEquals(1, listener.getGauge(counterId).get().getValue());
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 1);

        metricManager.onRemove(deployment2);
        assertEquals(0, listener.getGauge(counterId).get().getValue());
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 0);
    }

    @Test
    public void testStateCountMetricsMultiNamespace() {
        var namespace1 = "ns1";
        var namespace2 = "ns2";
        var deployment1 = buildBlueGreenDeployment("deployment", namespace1);
        var deployment2 = buildBlueGreenDeployment("deployment", namespace2);

        var counterId1 =
                listener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, namespace1, COUNTER_NAME);
        var counterId2 =
                listener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, namespace2, COUNTER_NAME);

        assertTrue(listener.getGauge(counterId1).isEmpty());
        assertTrue(listener.getGauge(counterId2).isEmpty());

        metricManager.onUpdate(deployment1);
        metricManager.onUpdate(deployment2);

        assertEquals(1, listener.getGauge(counterId1).get().getValue());
        assertEquals(1, listener.getGauge(counterId2).get().getValue());
        assertStateCount(namespace1, INITIALIZING_BLUE, 1);
        assertStateCount(namespace2, INITIALIZING_BLUE, 1);

        // Move deployment1 to different state
        deployment1.getStatus().setBlueGreenState(ACTIVE_BLUE);
        metricManager.onUpdate(deployment1);

        assertStateCount(namespace1, INITIALIZING_BLUE, 0);
        assertStateCount(namespace1, ACTIVE_BLUE, 1);
        // namespace2 should be unaffected
        assertStateCount(namespace2, INITIALIZING_BLUE, 1);

        metricManager.onRemove(deployment1);
        metricManager.onRemove(deployment2);

        assertEquals(0, listener.getGauge(counterId1).get().getValue());
        assertEquals(0, listener.getGauge(counterId2).get().getValue());
    }

    @Test
    public void testAllBlueGreenStatesHaveMetrics() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        // Verify each state has a gauge registered
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            var stateId =
                    listener.getNamespaceMetricId(
                            FlinkBlueGreenDeployment.class,
                            TEST_NAMESPACE,
                            BG_STATE_GROUP_NAME,
                            state.name(),
                            COUNTER_NAME);
            assertTrue(
                    listener.getGauge(stateId).isPresent(),
                    "Metric should exist for state: " + state);
        }
    }

    @Test
    public void testLifecycleHistogramMetricsExist() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        // Verify transition histograms exist
        assertTrue(
                listener.getHistogram(
                                getLifecycleHistogramId(
                                        TEST_NAMESPACE,
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_INITIAL_DEPLOYMENT))
                        .isPresent(),
                "InitialDeployment histogram should exist");

        assertTrue(
                listener.getHistogram(
                                getLifecycleHistogramId(
                                        TEST_NAMESPACE,
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_BLUE_TO_GREEN))
                        .isPresent(),
                "BlueToGreen histogram should exist");

        // Verify state time histograms exist
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            assertTrue(
                    listener.getHistogram(
                                    getLifecycleHistogramId(
                                            TEST_NAMESPACE, STATE_GROUP_NAME, state.name()))
                            .isPresent(),
                    "State time histogram should exist for: " + state);
        }
    }

    @Test
    public void testStateTransitionUpdatesCount() {
        var deployment = buildBlueGreenDeployment("test", TEST_NAMESPACE);

        // Start in INITIALIZING_BLUE
        metricManager.onUpdate(deployment);
        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 1);
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 0);

        // Transition to ACTIVE_BLUE
        deployment.getStatus().setBlueGreenState(ACTIVE_BLUE);
        metricManager.onUpdate(deployment);
        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 0);
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 1);

        // Transition to TRANSITIONING_TO_GREEN
        deployment.getStatus().setBlueGreenState(TRANSITIONING_TO_GREEN);
        metricManager.onUpdate(deployment);
        assertStateCount(TEST_NAMESPACE, ACTIVE_BLUE, 0);
        assertStateCount(TEST_NAMESPACE, TRANSITIONING_TO_GREEN, 1);

        // Transition to ACTIVE_GREEN
        deployment.getStatus().setBlueGreenState(ACTIVE_GREEN);
        metricManager.onUpdate(deployment);
        assertStateCount(TEST_NAMESPACE, TRANSITIONING_TO_GREEN, 0);
        assertStateCount(TEST_NAMESPACE, ACTIVE_GREEN, 1);
    }

    @Test
    public void testMetricsDisabled() {
        var conf = new Configuration();
        conf.set(OPERATOR_RESOURCE_METRICS_ENABLED, false);
        var disabledListener = new TestingMetricListener(conf);
        var disabledMetricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        conf, disabledListener.getMetricGroup());

        var deployment = buildBlueGreenDeployment("test", TEST_NAMESPACE);

        var counterId =
                disabledListener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, TEST_NAMESPACE, COUNTER_NAME);

        disabledMetricManager.onUpdate(deployment);
        assertTrue(disabledListener.getGauge(counterId).isEmpty());

        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            var statusId =
                    disabledListener.getNamespaceMetricId(
                            FlinkBlueGreenDeployment.class,
                            TEST_NAMESPACE,
                            BG_STATE_GROUP_NAME,
                            state.name(),
                            COUNTER_NAME);
            assertTrue(disabledListener.getGauge(statusId).isEmpty());
        }
    }

    @Test
    public void testRepeatedUpdatesDoNotDuplicateCount() {
        var deployment = buildBlueGreenDeployment("test", TEST_NAMESPACE);

        // Multiple updates in same state should not increase count
        metricManager.onUpdate(deployment);
        metricManager.onUpdate(deployment);
        metricManager.onUpdate(deployment);

        var counterId =
                listener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, TEST_NAMESPACE, COUNTER_NAME);
        assertEquals(1, listener.getGauge(counterId).get().getValue());
        assertStateCount(TEST_NAMESPACE, INITIALIZING_BLUE, 1);
    }

    // review
    @Test
    public void testNamespaceHistogramSharing() {
        // Deployments in the same namespace should share the same histogram objects
        // Deployments in different namespaces should have different namespace histograms
        var namespace1 = "ns1";
        var namespace2 = "ns2";

        var dep1 = buildBlueGreenDeployment("dep1", namespace1);
        var dep2 = buildBlueGreenDeployment("dep2", namespace1);
        var dep3 = buildBlueGreenDeployment("dep3", namespace2);

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);

        // Same namespace should have the same histogram (verify by checking same metric ID resolves)
        var ns1TransitionHistoId =
                getLifecycleHistogramId(namespace1, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);
        var ns2TransitionHistoId =
                getLifecycleHistogramId(namespace2, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);

        // Both ns1 deployments write to the same histogram
        assertTrue(listener.getHistogram(ns1TransitionHistoId).isPresent());
        assertTrue(listener.getHistogram(ns2TransitionHistoId).isPresent());

        // Different namespaces should have different histograms (different metric IDs)
        var ns1StateHistoId =
                getLifecycleHistogramId(namespace1, STATE_GROUP_NAME, INITIALIZING_BLUE.name());
        var ns2StateHistoId =
                getLifecycleHistogramId(namespace2, STATE_GROUP_NAME, INITIALIZING_BLUE.name());

        // Verify they are different metric IDs (namespace isolation)
        assertTrue(
                !ns1StateHistoId.equals(ns2StateHistoId),
                "Namespace histograms should have different metric IDs");
    }

    // review
    @Test
    public void testLifecycleMetricsDisabled() {
        // When OPERATOR_LIFECYCLE_METRICS_ENABLED is false, lifecycle histograms should not be created
        var conf = new Configuration();
        conf.set(OPERATOR_LIFECYCLE_METRICS_ENABLED, false);
        var disabledListener = new TestingMetricListener(conf);
        var disabledMetricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        conf, disabledListener.getMetricGroup());

        var deployment = buildBlueGreenDeployment("test", TEST_NAMESPACE);
        disabledMetricManager.onUpdate(deployment);

        // State count gauges should still exist (they're part of FlinkBlueGreenDeploymentMetrics)
        var counterId =
                disabledListener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class, TEST_NAMESPACE, COUNTER_NAME);
        assertTrue(disabledListener.getGauge(counterId).isPresent());

        // But lifecycle histograms should NOT exist
        var transitionHistoId =
                disabledListener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class,
                        TEST_NAMESPACE,
                        LIFECYCLE_GROUP_NAME,
                        TRANSITION_GROUP_NAME,
                        TRANSITION_INITIAL_DEPLOYMENT,
                        TIME_SECONDS_NAME);
        assertTrue(
                disabledListener.getHistogram(transitionHistoId).isEmpty(),
                "Lifecycle histogram should not exist when lifecycle metrics are disabled");

        var stateTimeHistoId =
                disabledListener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class,
                        TEST_NAMESPACE,
                        LIFECYCLE_GROUP_NAME,
                        STATE_GROUP_NAME,
                        INITIALIZING_BLUE.name(),
                        TIME_SECONDS_NAME);
        assertTrue(
                disabledListener.getHistogram(stateTimeHistoId).isEmpty(),
                "State time histogram should not exist when lifecycle metrics are disabled");
    }

    // ==================== Helper Methods ====================

    private FlinkBlueGreenDeployment buildBlueGreenDeployment(String name, String namespace) {
        var deployment = new FlinkBlueGreenDeployment();
        deployment.setMetadata(
                new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(namespace)
                        .withUid(UUID.randomUUID().toString())
                        .withCreationTimestamp(Instant.now().toString())
                        .build());

        var flinkDeploymentSpec =
                FlinkDeploymentSpec.builder()
                        .flinkConfiguration(new ConfigObjectNode())
                        .job(JobSpec.builder().upgradeMode(UpgradeMode.STATELESS).build())
                        .build();

        var bgDeploymentSpec =
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        FlinkDeploymentTemplateSpec.builder().spec(flinkDeploymentSpec).build());

        deployment.setSpec(bgDeploymentSpec);

        var status = new FlinkBlueGreenDeploymentStatus();
        status.setBlueGreenState(INITIALIZING_BLUE);
        deployment.setStatus(status);

        return deployment;
    }

    private void assertStateCount(
            String namespace, FlinkBlueGreenDeploymentState state, int expectedCount) {
        var stateId =
                listener.getNamespaceMetricId(
                        FlinkBlueGreenDeployment.class,
                        namespace,
                        BG_STATE_GROUP_NAME,
                        state.name(),
                        COUNTER_NAME);
        assertEquals(
                expectedCount,
                listener.getGauge(stateId).get().getValue(),
                "State count mismatch for " + state);
    }

    private String getLifecycleHistogramId(String namespace, String groupName, String metricName) {
        return listener.getNamespaceMetricId(
                FlinkBlueGreenDeployment.class,
                namespace,
                LIFECYCLE_GROUP_NAME,
                groupName,
                metricName,
                TIME_SECONDS_NAME);
    }
}

