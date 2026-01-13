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
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Metrics for FlinkBlueGreenDeployment resources. */
public class FlinkBlueGreenDeploymentMetrics
        implements CustomResourceMetrics<FlinkBlueGreenDeployment> {

    public static final String BG_STATE_GROUP_NAME = "BlueGreenState";
    public static final String COUNTER_NAME = "Count";

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;

    // Tracks which deployments are in which state per namespace (for gauge metrics)
    // Map: namespace -> state -> set of deployment names
    private final Map<String, Map<FlinkBlueGreenDeploymentState, Set<String>>> deploymentStatuses =
            new ConcurrentHashMap<>();

    public FlinkBlueGreenDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    @Override
    public void onUpdate(FlinkBlueGreenDeployment flinkBgDep) {
        clearStateCount(flinkBgDep);

        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();
        var state = flinkBgDep.getStatus().getBlueGreenState();

        deploymentStatuses
                .computeIfAbsent(namespace, this::initNamespaceMetrics)
                .get(state)
                .add(deploymentName);
    }

    @Override
    public void onRemove(FlinkBlueGreenDeployment flinkBgDep) {
        clearStateCount(flinkBgDep);
    }

    /** Clears the deployment from all state count sets (used before updating to new state). */
    private void clearStateCount(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();

        var namespaceStatuses = deploymentStatuses.get(namespace);
        if (namespaceStatuses != null) {
            namespaceStatuses
                    .values()
                    .forEach(deploymentNames -> deploymentNames.remove(deploymentName));
        }
    }

    private Map<FlinkBlueGreenDeploymentState, Set<String>> initNamespaceMetrics(String namespace) {
        MetricGroup nsGroup =
                parentMetricGroup.createResourceNamespaceGroup(
                        configuration, FlinkBlueGreenDeployment.class, namespace);

        // Total deployment count
        nsGroup.gauge(
                COUNTER_NAME,
                () ->
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
}
