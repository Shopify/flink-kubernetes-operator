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

package org.apache.flink.kubernetes.operator.metrics.lifecycle;

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
import org.apache.flink.kubernetes.operator.metrics.CustomResourceMetrics;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.kubernetes.operator.metrics.MetricManager;
import org.apache.flink.kubernetes.operator.metrics.TestingMetricListener;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.UUID;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.FlinkBlueGreenDeploymentMetrics.COUNTER_NAME;
import static org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.LIFECYCLE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.STATE_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TIME_SECONDS_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetrics.TRANSITION_GROUP_NAME;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BlueGreenLifecycleMetrics}. */
public class BlueGreenLifecycleMetricsTest {

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

    // ==================== Histogram Existence Tests ====================

    @Test
    public void testNamespaceHistogramMetricsExist() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        // Verify namespace-level transition histograms exist
        assertTrue(
                listener.getHistogram(
                                getNamespaceHistogramId(
                                        TEST_NAMESPACE,
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_INITIAL_DEPLOYMENT))
                        .isPresent(),
                "InitialDeployment histogram should exist");

        assertTrue(
                listener.getHistogram(
                                getNamespaceHistogramId(
                                        TEST_NAMESPACE,
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_BLUE_TO_GREEN))
                        .isPresent(),
                "BlueToGreen histogram should exist");

        assertTrue(
                listener.getHistogram(
                                getNamespaceHistogramId(
                                        TEST_NAMESPACE,
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_GREEN_TO_BLUE))
                        .isPresent(),
                "GreenToBlue histogram should exist");

        // Verify namespace-level state time histograms exist
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            assertTrue(
                    listener.getHistogram(
                                    getNamespaceHistogramId(
                                            TEST_NAMESPACE, STATE_GROUP_NAME, state.name()))
                            .isPresent(),
                    "State time histogram should exist for: " + state);
        }
    }

    @Test
    public void testSystemLevelHistogramsExist() {
        var deployment = buildBlueGreenDeployment("test-deployment", TEST_NAMESPACE);
        metricManager.onUpdate(deployment);

        // Verify system-level transition histograms exist (without namespace in path)
        assertTrue(
                listener.getHistogram(
                                getSystemLevelHistogramId(
                                        TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT))
                        .isPresent(),
                "System-level InitialDeployment histogram should exist");

        assertTrue(
                listener.getHistogram(
                                getSystemLevelHistogramId(
                                        TRANSITION_GROUP_NAME, TRANSITION_BLUE_TO_GREEN))
                        .isPresent(),
                "System-level BlueToGreen histogram should exist");

        assertTrue(
                listener.getHistogram(
                                getSystemLevelHistogramId(
                                        TRANSITION_GROUP_NAME, TRANSITION_GREEN_TO_BLUE))
                        .isPresent(),
                "System-level GreenToBlue histogram should exist");

        // Verify system-level state time histograms exist
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            assertTrue(
                    listener.getHistogram(getSystemLevelHistogramId(STATE_GROUP_NAME, state.name()))
                            .isPresent(),
                    "System-level state time histogram should exist for: " + state);
        }
    }

    // ==================== Namespace Sharing Tests ====================

    @Test
    public void testNamespaceHistogramSharing() {
        // Deployments in the same namespace should share the same namespace histogram objects
        // Deployments in different namespaces should have different namespace histograms
        // But all should share system-level histograms
        var namespace1 = "ns1";
        var namespace2 = "ns2";

        var dep1 = buildBlueGreenDeployment("dep1", namespace1);
        var dep2 = buildBlueGreenDeployment("dep2", namespace1);
        var dep3 = buildBlueGreenDeployment("dep3", namespace2);

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);

        // Same namespace should have the same histogram (verify by checking same metric ID
        // resolves)
        var ns1TransitionHistoId =
                getNamespaceHistogramId(
                        namespace1, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);
        var ns2TransitionHistoId =
                getNamespaceHistogramId(
                        namespace2, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);

        // Both ns1 deployments write to the same histogram
        assertTrue(listener.getHistogram(ns1TransitionHistoId).isPresent());
        assertTrue(listener.getHistogram(ns2TransitionHistoId).isPresent());

        // Different namespaces should have different histograms (different metric IDs)
        var ns1StateHistoId =
                getNamespaceHistogramId(namespace1, STATE_GROUP_NAME, INITIALIZING_BLUE.name());
        var ns2StateHistoId =
                getNamespaceHistogramId(namespace2, STATE_GROUP_NAME, INITIALIZING_BLUE.name());

        // Verify they are different metric IDs (namespace isolation)
        assertNotEquals(
                ns1StateHistoId,
                ns2StateHistoId,
                "Namespace histograms should have different metric IDs");

        // But system-level histogram should be shared (same metric ID for all)
        var systemHistoId =
                getSystemLevelHistogramId(TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);
        assertTrue(
                listener.getHistogram(systemHistoId).isPresent(),
                "System-level histogram should exist and be shared");
    }

    @Test
    public void testTrackerSharingAcrossDeployments() {
        // Similar to testLifecycleMetricsConfig in ResourceLifecycleMetricsTest
        // Verifies that trackers in same namespace share histogram references
        var dep1 = buildBlueGreenDeployment("n1", "ns1");
        var dep2 = buildBlueGreenDeployment("n2", "ns1");
        var dep3 = buildBlueGreenDeployment("n3", "ns2");

        metricManager.onUpdate(dep1);
        metricManager.onUpdate(dep2);
        metricManager.onUpdate(dep3);

        var lifecycleMetrics = getBlueGreenLifecycleMetrics(metricManager);

        // Verify lifecycle metrics was registered
        assertTrue(lifecycleMetrics != null, "BlueGreenLifecycleMetrics should be registered");

        // All should have namespace histograms created
        assertTrue(
                listener.getHistogram(
                                getNamespaceHistogramId(
                                        "ns1",
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_INITIAL_DEPLOYMENT))
                        .isPresent());
        assertTrue(
                listener.getHistogram(
                                getNamespaceHistogramId(
                                        "ns2",
                                        TRANSITION_GROUP_NAME,
                                        TRANSITION_INITIAL_DEPLOYMENT))
                        .isPresent());
    }

    // ==================== Config Tests ====================

    @Test
    public void testLifecycleMetricsDisabled() {
        // When OPERATOR_LIFECYCLE_METRICS_ENABLED is false, lifecycle histograms should not be
        // created
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

        // System-level histograms should also not exist
        var systemHistoId =
                getSystemLevelHistogramIdForListener(
                        disabledListener, TRANSITION_GROUP_NAME, TRANSITION_INITIAL_DEPLOYMENT);
        assertTrue(
                disabledListener.getHistogram(systemHistoId).isEmpty(),
                "System-level histogram should not exist when lifecycle metrics are disabled");
    }

    @Test
    public void testLifecycleMetricsNotRegisteredWhenDisabled() {
        var conf = new Configuration();
        conf.set(KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED, false);
        var disabledMetricManager =
                MetricManager.createFlinkBlueGreenDeploymentMetricManager(
                        conf, new TestingMetricListener(conf).getMetricGroup());

        assertNull(
                getBlueGreenLifecycleMetrics(disabledMetricManager),
                "BlueGreenLifecycleMetrics should not be registered when disabled");
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

    private String getNamespaceHistogramId(String namespace, String groupName, String metricName) {
        return listener.getNamespaceMetricId(
                FlinkBlueGreenDeployment.class,
                namespace,
                LIFECYCLE_GROUP_NAME,
                groupName,
                metricName,
                TIME_SECONDS_NAME);
    }

    private String getSystemLevelHistogramId(String groupName, String metricName) {
        return listener.getMetricId(
                String.format(
                        "%s.%s.%s.%s.%s",
                        FlinkBlueGreenDeployment.class.getSimpleName(),
                        LIFECYCLE_GROUP_NAME,
                        groupName,
                        metricName,
                        TIME_SECONDS_NAME));
    }

    private String getSystemLevelHistogramIdForListener(
            TestingMetricListener metricListener, String groupName, String metricName) {
        return metricListener.getMetricId(
                String.format(
                        "%s.%s.%s.%s.%s",
                        FlinkBlueGreenDeployment.class.getSimpleName(),
                        LIFECYCLE_GROUP_NAME,
                        groupName,
                        metricName,
                        TIME_SECONDS_NAME));
    }

    private static BlueGreenLifecycleMetrics getBlueGreenLifecycleMetrics(
            MetricManager<FlinkBlueGreenDeployment> metricManager) {
        for (CustomResourceMetrics<?> metrics : metricManager.getRegisteredMetrics()) {
            if (metrics instanceof BlueGreenLifecycleMetrics) {
                return (BlueGreenLifecycleMetrics) metrics;
            }
        }
        return null;
    }
}
