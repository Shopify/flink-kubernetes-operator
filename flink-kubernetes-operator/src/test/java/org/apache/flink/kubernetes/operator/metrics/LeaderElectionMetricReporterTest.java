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
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.prometheus.PrometheusReporter;
import org.apache.flink.metrics.prometheus.PrometheusReporterFactory;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the Prometheus exposition of {@link LeaderElectionMetric}. */
class LeaderElectionMetricReporterTest {

    private static final String METRIC_FAMILY = "flink_k8soperator_LeaderElection_IsLeader";
    private static final String TEST_NAMESPACE = "test-namespace";
    private static final String TEST_OPERATOR_NAME = "test-operator";
    private static final String TEST_HOST = "test-host";
    private static final String EXPORTED_NAMESPACE = "test_namespace";
    private static final String EXPORTED_OPERATOR_NAME = "test_operator";
    private static final String EXPORTED_HOST = "test_host";

    private MetricRegistryImpl registry;

    @AfterEach
    void closeRegistry() throws Exception {
        if (registry != null) {
            registry.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPrometheusExpositionAndRepeatedLeadershipTransitions(boolean excludeHost)
            throws IOException {
        int reporterPort = getAvailablePort();
        Configuration metricConfig = createPrometheusMetricConfig(reporterPort, excludeHost);
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(metricConfig);
        List<ReporterSetup> reporterSetups =
                ReporterSetup.fromConfiguration(metricConfig, pluginManager);

        assertEquals(1, reporterSetups.size());
        assertTrue(reporterSetups.get(0).getReporter() instanceof PrometheusReporter);
        PrometheusReporter reporter = (PrometheusReporter) reporterSetups.get(0).getReporter();

        registry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.fromConfiguration(metricConfig, Long.MAX_VALUE),
                        reporterSetups);
        KubernetesOperatorMetricGroup operatorMetricGroup =
                KubernetesOperatorMetricGroup.create(
                        registry, metricConfig, TEST_NAMESPACE, TEST_OPERATOR_NAME, TEST_HOST);
        LeaderElectionMetric leadershipMetric = new LeaderElectionMetric(operatorMetricGroup);

        assertExposedValue(reporterPort, excludeHost, 0);

        leadershipMetric.onStartLeading();
        assertExposedValue(reporterPort, excludeHost, 1);
        leadershipMetric.onStartLeading();
        assertExposedValue(reporterPort, excludeHost, 1);

        leadershipMetric.onStopLeading();
        assertExposedValue(reporterPort, excludeHost, 0);
        leadershipMetric.onStopLeading();
        assertExposedValue(reporterPort, excludeHost, 0);

        leadershipMetric.onStartLeading();
        assertExposedValue(reporterPort, excludeHost, 1);
        leadershipMetric.onStopLeading();
        assertExposedValue(reporterPort, excludeHost, 0);
    }

    private static Configuration createPrometheusMetricConfig(
            int reporterPort, boolean excludeHost) {
        Configuration operatorConfig =
                Configuration.fromMap(
                        Map.of(
                                "kubernetes.operator.metrics.reporters",
                                "prom",
                                "kubernetes.operator.metrics.reporter.prom.factory.class",
                                PrometheusReporterFactory.class.getName(),
                                "kubernetes.operator.metrics.reporter.prom.port",
                                String.valueOf(reporterPort)));
        if (excludeHost) {
            operatorConfig.setString(
                    "kubernetes.operator.metrics.reporter.prom.scope.variables.excludes", "host");
        }
        return OperatorMetricUtils.createMetricConfig(operatorConfig);
    }

    private static int getAvailablePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private static void assertExposedValue(
            int reporterPort, boolean excludeHost, int expectedValue) {
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            String exposition = scrape(reporterPort);
                            List<String> samples =
                                    exposition
                                            .lines()
                                            .filter(line -> !line.startsWith("#"))
                                            .filter(line -> line.contains("LeaderElection"))
                                            .toList();

                            assertTrue(
                                    exposition.contains("# TYPE " + METRIC_FAMILY + " gauge"),
                                    exposition);
                            assertEquals(1, samples.size(), exposition);
                            assertMetricSample(samples.get(0), excludeHost, expectedValue);
                        });
    }

    private static String scrape(int reporterPort) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + reporterPort + "/"))
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build();
        HttpResponse<String> response =
                HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    private static void assertMetricSample(String sample, boolean excludeHost, int expectedValue) {
        assertTrue(sample.startsWith(METRIC_FAMILY + "{"), sample);

        int labelsEnd = sample.indexOf('}');
        assertTrue(labelsEnd > METRIC_FAMILY.length(), sample);
        String labels = sample.substring(METRIC_FAMILY.length() + 1, labelsEnd);
        Set<String> labelKeys =
                Arrays.stream(labels.split(","))
                        .map(label -> label.substring(0, label.indexOf('=')))
                        .collect(Collectors.toSet());
        assertEquals(
                excludeHost ? Set.of("namespace", "name") : Set.of("namespace", "name", "host"),
                labelKeys,
                sample);
        // The Prometheus reporter's default filter normalizes label-value hyphens to underscores.
        assertTrue(labels.contains("namespace=\"" + EXPORTED_NAMESPACE + "\""), sample);
        assertTrue(labels.contains("name=\"" + EXPORTED_OPERATOR_NAME + "\""), sample);
        if (excludeHost) {
            assertFalse(labels.contains("host="), sample);
        } else {
            assertTrue(labels.contains("host=\"" + EXPORTED_HOST + "\""), sample);
        }

        assertFalse(sample.contains("flink_flink_"), sample);
        assertFalse(sample.contains("system"), sample);
        assertFalse(sample.contains("JOSDK"), sample);

        double value = Double.parseDouble(sample.substring(labelsEnd + 1).trim());
        assertEquals((double) expectedValue, value, sample);
    }
}
