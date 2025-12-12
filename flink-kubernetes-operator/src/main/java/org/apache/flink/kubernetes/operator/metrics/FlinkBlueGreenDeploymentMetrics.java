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
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;

/** Metrics for FlinkBlueGreenDeployment resources. */
public class FlinkBlueGreenDeploymentMetrics
        implements CustomResourceMetrics<FlinkBlueGreenDeployment> {

    public static final String BG_STATE_GROUP_NAME = "BlueGreenState";
    public static final String COUNTER_NAME = "Count";
    public static final String LIFECYCLE_GROUP_NAME = "Lifecycle";
    public static final String TRANSITION_GROUP_NAME = "Transition";
    public static final String STATE_GROUP_NAME = "State";
    public static final String TIME_SECONDS_NAME = "TimeSeconds";

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;
    private final FlinkOperatorConfiguration operatorConfig;
    private final Clock clock;
    private final boolean lifecycleMetricsEnabled;

    // State counts: namespace -> state -> set of deployment names
    private final Map<String, Map<FlinkBlueGreenDeploymentState, Set<String>>> deploymentStatuses =
            new ConcurrentHashMap<>();

    // Lifecycle trackers: (namespace, name) -> tracker
    private final Map<String, Map<String, BlueGreenLifecycleMetricTracker>> lifecycleTrackers =
            new ConcurrentHashMap<>();

    // Namespace-scoped histograms: metricKey -> namespace -> histogram
    private final Map<String, Map<String, Histogram>> transitionHistograms = new ConcurrentHashMap<>();
    private final Map<FlinkBlueGreenDeploymentState, Map<String, Histogram>> stateTimeHistograms =
            new ConcurrentHashMap<>();

    public FlinkBlueGreenDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
        this.operatorConfig = FlinkOperatorConfiguration.fromConfiguration(configuration);
        this.clock = Clock.systemDefaultZone();
        this.lifecycleMetricsEnabled =
                configuration.get(KubernetesOperatorMetricOptions.OPERATOR_LIFECYCLE_METRICS_ENABLED);

        // Initialize histogram maps for each metric key
        for (String transition : List.of(
                BlueGreenLifecycleMetricTracker.TRANSITION_INITIAL_DEPLOYMENT,
                BlueGreenLifecycleMetricTracker.TRANSITION_BLUE_TO_GREEN,
                BlueGreenLifecycleMetricTracker.TRANSITION_GREEN_TO_BLUE)) {
            transitionHistograms.put(transition, new ConcurrentHashMap<>());
        }
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            stateTimeHistograms.put(state, new ConcurrentHashMap<>());
        }
    }

    @Override
    public void onUpdate(FlinkBlueGreenDeployment flinkBgDep) {
        onRemove(flinkBgDep);

        var namespace = flinkBgDep.getMetadata().getNamespace();
        var name = flinkBgDep.getMetadata().getName();
        var state = flinkBgDep.getStatus().getBlueGreenState();

        // Update state counts
        deploymentStatuses
                .computeIfAbsent(namespace, this::initNamespaceMetrics)
                .get(state)
                .add(name);

        // Update lifecycle metrics
        if (lifecycleMetricsEnabled) {
            getOrCreateTracker(namespace, name, flinkBgDep).onUpdate(state, clock.instant());
        }
    }

    @Override
    public void onRemove(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var name = flinkBgDep.getMetadata().getName();

        var namespaceStatuses = deploymentStatuses.get(namespace);
        if (namespaceStatuses != null) {
            namespaceStatuses.values().forEach(names -> names.remove(name));
        }

        var namespaceTrackers = lifecycleTrackers.get(namespace);
        if (namespaceTrackers != null) {
            namespaceTrackers.remove(name);
        }
    }

    // ==================== State Count Metrics ====================

    private Map<FlinkBlueGreenDeploymentState, Set<String>> initNamespaceMetrics(String namespace) {
        MetricGroup nsGroup = parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkBlueGreenDeployment.class, namespace);

        // Total deployment count
        nsGroup.gauge(COUNTER_NAME, () ->
                deploymentStatuses.get(namespace).values().stream()
                        .mapToInt(Set::size)
                        .sum());

        // Per-state counts
        Map<FlinkBlueGreenDeploymentState, Set<String>> statuses = new ConcurrentHashMap<>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            statuses.put(state, ConcurrentHashMap.newKeySet());
            nsGroup.addGroup(BG_STATE_GROUP_NAME)
                    .addGroup(state.toString())
                    .gauge(COUNTER_NAME, () -> deploymentStatuses.get(namespace).get(state).size());
        }
        return statuses;
    }

    // ==================== Lifecycle Metrics ====================

    private BlueGreenLifecycleMetricTracker getOrCreateTracker(
            String namespace, String name, FlinkBlueGreenDeployment flinkBgDep) {

        return lifecycleTrackers
                .computeIfAbsent(namespace, ns -> new ConcurrentHashMap<>())
                .computeIfAbsent(name, n -> {
                    var initialState = flinkBgDep.getStatus().getBlueGreenState();
                    var time = initialState == INITIALIZING_BLUE
                            ? Instant.parse(flinkBgDep.getMetadata().getCreationTimestamp())
                            : clock.instant();
                    return new BlueGreenLifecycleMetricTracker(
                            initialState,
                            time,
                            getTransitionHistograms(namespace),
                            getStateTimeHistograms(namespace));
                });
    }

    private Map<String, List<Histogram>> getTransitionHistograms(String namespace) {
        var histos = new HashMap<String, List<Histogram>>();
        transitionHistograms.forEach((transitionName, nsMap) ->
                histos.put(transitionName, List.of(
                        nsMap.computeIfAbsent(namespace, ns ->
                                createHistogram(ns, TRANSITION_GROUP_NAME, transitionName)))));
        return histos;
    }

    private Map<FlinkBlueGreenDeploymentState, List<Histogram>> getStateTimeHistograms(String namespace) {
        var histos = new HashMap<FlinkBlueGreenDeploymentState, List<Histogram>>();
        stateTimeHistograms.forEach((state, nsMap) ->
                histos.put(state, List.of(
                        nsMap.computeIfAbsent(namespace, ns ->
                                createHistogram(ns, STATE_GROUP_NAME, state.name())))));
        return histos;
    }

    private Histogram createHistogram(String namespace, String groupName, String metricName) {
        return parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkBlueGreenDeployment.class, namespace)
                .addGroup(LIFECYCLE_GROUP_NAME)
                .addGroup(groupName)
                .addGroup(metricName)
                .histogram(TIME_SECONDS_NAME, OperatorMetricUtils.createHistogram(operatorConfig));
    }
}
