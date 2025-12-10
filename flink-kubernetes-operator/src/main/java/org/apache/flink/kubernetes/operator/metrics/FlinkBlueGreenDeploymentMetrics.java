package org.apache.flink.kubernetes.operator.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FlinkBlueGreenDeploymentMetrics
        implements CustomResourceMetrics<FlinkBlueGreenDeployment> {

    private final KubernetesOperatorMetricGroup parentMetricGroup;
    private final Configuration configuration;

    // map(namespace, map(state, set(deployment)))
    private final Map<String, Map<FlinkBlueGreenDeploymentState, Set<String>>> deploymentStatuses =
            new ConcurrentHashMap<>();

    public static final String BG_STATE_GROUP_NAME = "BlueGreenState";
    public static final String COUNTER_NAME = "Count";

    public FlinkBlueGreenDeploymentMetrics(
            KubernetesOperatorMetricGroup parentMetricGroup, Configuration configuration) {
        this.parentMetricGroup = parentMetricGroup;
        this.configuration = configuration;
    }

    @Override
    public void onUpdate(FlinkBlueGreenDeployment flinkBgDep) {
        onRemove(flinkBgDep);

        var namespace = flinkBgDep.getMetadata().getNamespace();
        var deploymentName = flinkBgDep.getMetadata().getName();

        deploymentStatuses
                .computeIfAbsent(
                        namespace,
                        ns -> {
                            initNamespaceDeploymentCounts(ns);
                            initNamespaceStatusCounts(ns);
                            return createDeploymentStatusMap();
                        })
                .get(flinkBgDep.getStatus().getBlueGreenState())
                .add(deploymentName);
    }

    @Override
    public void onRemove(FlinkBlueGreenDeployment flinkBgDep) {
        var namespace = flinkBgDep.getMetadata().getNamespace();
        var name = flinkBgDep.getMetadata().getName();

        if (deploymentStatuses.containsKey(namespace)) {
            deploymentStatuses.get(namespace).values().forEach(names -> names.remove(name));
        }
    }

    private void initNamespaceDeploymentCounts(String ns) {
        parentMetricGroup
                .createResourceNamespaceGroup(configuration, FlinkBlueGreenDeployment.class, ns)
                .gauge(
                        COUNTER_NAME,
                        () ->
                                deploymentStatuses.get(ns).values().stream()
                                        .mapToInt(Set::size)
                                        .sum());
    }

    private void initNamespaceStatusCounts(String ns) {
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            parentMetricGroup
                    .createResourceNamespaceGroup(configuration, FlinkBlueGreenDeployment.class, ns)
                    .addGroup(BG_STATE_GROUP_NAME)
                    .addGroup(state.toString())
                    .gauge(COUNTER_NAME, () -> deploymentStatuses.get(ns).get(state).size());
        }
    }

    private Map<FlinkBlueGreenDeploymentState, Set<String>> createDeploymentStatusMap() {
        Map<FlinkBlueGreenDeploymentState, Set<String>> statuses = new ConcurrentHashMap<>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            statuses.put(state, ConcurrentHashMap.newKeySet());
        }
        return statuses;
    }
}
