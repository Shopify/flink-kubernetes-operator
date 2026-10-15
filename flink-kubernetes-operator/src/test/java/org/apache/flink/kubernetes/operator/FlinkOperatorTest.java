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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.metrics.KubernetesOperatorMetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.testutils.InMemoryReporter;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link FlinkOperator} unit tests. */
@EnableKubernetesMockClient(crud = true)
class FlinkOperatorTest {

    KubernetesClient kubernetesClient;

    @Test
    void testConfigurationPassedToJOSDK() {
        var testParallelism = 42;
        var testSelector = "flink=enabled";
        var testLeaseName = "test-lease";

        var operatorConfig = new Configuration();

        operatorConfig.setInteger(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM, testParallelism);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LABEL_SELECTOR, testSelector);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_STOP_ON_INFORMER_ERROR, false);
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED, true);
        operatorConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_NAME, testLeaseName);
        operatorConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_DURATION,
                Duration.ofSeconds(42));
        operatorConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_RENEW_DEADLINE,
                Duration.ofSeconds(24));
        operatorConfig.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_RETRY_PERIOD,
                Duration.ofSeconds(7));
        operatorConfig.set(KubernetesOperatorMetricOptions.OPERATOR_JOSDK_METRICS_ENABLED, false);
        operatorConfig.set(KubernetesOperatorMetricOptions.OPERATOR_JVM_METRICS_ENABLED, false);

        var reporter = InMemoryReporter.create();
        addReporter(operatorConfig, reporter);
        var testOperator = new FlinkOperator(operatorConfig, kubernetesClient);
        try {
            // Registration happens before SDK startup, including on replicas that remain standby.
            var leadershipGauge = leadershipGauge(reporter);
            assertEquals(0, leadershipGauge.getValue());
            assertEquals(1, reporter.getMetricsByGroup().size());
            testOperator.registerDeploymentController();
            testOperator.registerSessionJobController();

            var configService = testOperator.getOperator().getConfigurationService();

            // Test parallelism being passed
            var executorService = configService.getExecutorService();
            Assertions.assertInstanceOf(ThreadPoolExecutor.class, executorService);
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            Assertions.assertEquals(threadPoolExecutor.getMaximumPoolSize(), testParallelism);

            // Test label selector being passed
            // We have a label selector for each controller
            var labelSelectors =
                    testOperator.registeredControllers.stream()
                            .map(RegisteredController::getConfiguration)
                            .map(c -> c.getInformerConfig().getLabelSelector());

            labelSelectors.forEach(selector -> Assertions.assertEquals(testSelector, selector));
            Assertions.assertFalse(configService.stopOnInformerErrorDuringStartup());

            testOperator.registeredControllers.stream()
                    .map(RegisteredController::getConfiguration)
                    .map(ControllerConfiguration::getRateLimiter)
                    .forEach(rl -> Assertions.assertTrue(((LinearRateLimiter) rl).isActivated()));

            var leaderElectionConfiguration = configService.getLeaderElectionConfiguration().get();

            Assertions.assertEquals(testLeaseName, leaderElectionConfiguration.getLeaseName());
            Assertions.assertFalse(leaderElectionConfiguration.getLeaseNamespace().isPresent());
            Assertions.assertFalse(leaderElectionConfiguration.getIdentity().isPresent());
            assertEquals(Duration.ofSeconds(42), leaderElectionConfiguration.getLeaseDuration());
            assertEquals(Duration.ofSeconds(24), leaderElectionConfiguration.getRenewDeadline());
            assertEquals(Duration.ofSeconds(7), leaderElectionConfiguration.getRetryPeriod());
            assertTrue(leaderElectionConfiguration.isExitOnStopLeading());
            var callbacks = leaderElectionConfiguration.getLeaderCallbacks().orElseThrow();
            callbacks.onStartLeading();
            assertEquals(1, leadershipGauge.getValue());
            callbacks.onNewLeader("another-replica");
            assertEquals(1, leadershipGauge.getValue());
            // These are the configured callbacks, not the SDK manager's wrapper that exits the JVM.
            callbacks.onStopLeading();
            callbacks.onStopLeading();
            assertEquals(0, leadershipGauge.getValue());
            callbacks.onStartLeading();
            testOperator.stop();
            assertEquals(0, leadershipGauge.getValue());
        } finally {
            testOperator.stop();
            reporter.close();
        }
    }

    @Test
    void testLeadershipDuringStartupAndStop() {
        verifyLeadershipLifecycle(false, false, false);
        verifyLeadershipLifecycle(false, true, false);
        verifyLeadershipLifecycle(false, false, true);
        verifyLeadershipLifecycle(true, false, false);
    }

    private void verifyLeadershipLifecycle(
            boolean electionEnabled, boolean failStart, boolean failStop) {
        var reporter = InMemoryReporter.create();
        var config = new Configuration();
        config.set(KubernetesOperatorMetricOptions.OPERATOR_JOSDK_METRICS_ENABLED, false);
        config.set(KubernetesOperatorMetricOptions.OPERATOR_JVM_METRICS_ENABLED, false);
        config.set(KubernetesOperatorConfigOptions.OPERATOR_HEALTH_PROBE_ENABLED, false);
        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED, electionEnabled);
        config.set(
                KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_LEASE_NAME, "test-lease");
        addReporter(config, reporter);
        var startupFailure = new IllegalStateException("start failed");
        var stopFailure = new IllegalStateException("stop failed");
        var hookCalls = new AtomicInteger();
        var stopCalls = new AtomicInteger();
        var testOperator =
                new FlinkOperator(config, kubernetesClient) {
                    @Override
                    protected Operator createOperator() {
                        assertEquals(0, leadershipGauge(reporter).getValue());
                        // Keep SDK lifecycle actions deterministic; configuration wiring is
                        // exercised separately with the real SDK in testConfigurationPassedToJOSDK.
                        return new Operator(o -> o.withKubernetesClient(kubernetesClient)) {
                            @Override
                            public void installShutdownHook(Duration timeout) {
                                hookCalls.incrementAndGet();
                                assertEquals(
                                        config.get(
                                                KubernetesOperatorConfigOptions
                                                        .OPERATOR_TERMINATION_TIMEOUT),
                                        timeout);
                            }

                            @Override
                            public void start() {
                                assertEquals(0, leadershipGauge(reporter).getValue());
                                if (failStart) {
                                    throw startupFailure;
                                }
                            }

                            @Override
                            public void stop() {
                                // FlinkOperator must clear the gauge before delegating to SDK stop.
                                assertEquals(0, leadershipGauge(reporter).getValue());
                                stopCalls.incrementAndGet();
                                if (failStop) {
                                    throw stopFailure;
                                }
                            }
                        };
                    }

                    @Override
                    void registerDeploymentController() {}

                    @Override
                    void registerSessionJobController() {}

                    @Override
                    void registerSnapshotController() {}

                    @Override
                    void registerBlueGreenController() {}
                };
        try {
            assertEquals(0, leadershipGauge(reporter).getValue());
            if (failStart) {
                assertSame(
                        startupFailure,
                        assertThrows(IllegalStateException.class, testOperator::run));
            } else {
                testOperator.run();
            }
            assertEquals(1, hookCalls.get());
            assertEquals(
                    !failStart && !electionEnabled ? 1 : 0, leadershipGauge(reporter).getValue());
            for (int i = 0; i < 2; i++) {
                if (failStop) {
                    assertSame(
                            stopFailure,
                            assertThrows(IllegalStateException.class, testOperator::stop));
                } else {
                    testOperator.stop();
                }
                assertEquals(0, leadershipGauge(reporter).getValue());
            }
            assertEquals(2, stopCalls.get());
        } finally {
            reporter.close();
        }
    }

    private static void addReporter(Configuration config, InMemoryReporter reporter) {
        reporter.addToConfiguration(new Configuration())
                .toMap()
                .forEach((key, value) -> config.setString("kubernetes.operator." + key, value));
    }

    private static Gauge<?> leadershipGauge(InMemoryReporter reporter) {
        var groups = reporter.findGroups("^LeaderElection$");
        assertEquals(1, groups.size());
        var metrics = reporter.getMetricsByGroup(groups.iterator().next());
        assertEquals(1, metrics.size());
        var gauge = Assertions.assertInstanceOf(Gauge.class, metrics.get("IsLeader"));
        Assertions.assertInstanceOf(Integer.class, gauge.getValue());
        return gauge;
    }

    @Test
    void testLeaderElectionConfig() {
        var operatorConfig = new Configuration();
        operatorConfig.set(KubernetesOperatorConfigOptions.OPERATOR_LEADER_ELECTION_ENABLED, true);

        try {
            new FlinkOperator(operatorConfig, kubernetesClient);
        } catch (IllegalConfigurationException ice) {
            assertTrue(
                    ice.getMessage()
                            .startsWith(
                                    "kubernetes.operator.leader-election.lease-name must be defined"));
        }

        var flinkOperator = new FlinkOperator(new Configuration(), kubernetesClient);

        assertTrue(
                flinkOperator
                        .getOperator()
                        .getConfigurationService()
                        .getLeaderElectionConfiguration()
                        .isEmpty());
    }
}
