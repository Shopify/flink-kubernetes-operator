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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.autoscaler.NoopJobAutoscaler;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.TestingStatusRecorder;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentTemplateSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;
import org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.K8S_OP_CONF_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executes first-submission sizing locally, without invoking a Kubernetes application deployer.
 * This does not exercise Kubernetes provisioning, savepoint restoration, or blue/green cutover.
 */
@EnableKubernetesMockClient(crud = true)
class BlueGreenFirstSubmissionTest {

    private static final String MAPPER_UID = "observed-mapper";
    private static final String INHERITANCE_MARKER =
            "kubernetes.operator.bluegreen.parallelism-inheritance";
    private static final String OBSERVATIONS = "first-attempt-sizing";
    private static final String RECORDS = "processed-records";

    private KubernetesClient kubernetesClient;
    private final EventRecorder eventRecorder =
            new EventRecorder(
                    new FlinkResourceEventCollector(), new FlinkStateSnapshotEventCollector());

    @ParameterizedTest(name = "inherited override = {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    void firstSubmissionStartsAtInheritedSizingWithoutRescaling(boolean inheritOverride)
            throws Exception {
        var defaults = localRuntimeConfig();
        defaults.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaults.set(PipelineOptions.MAX_PARALLELISM, 2);
        // Resolve the real JobVertexID from a baseline graph, not an operator-state UID hash.
        var mapperId =
                createJobGraph(defaults).getVerticesSortedTopologicallyFromSources().stream()
                        .filter(vertex -> vertex.getName().equals(MAPPER_UID))
                        .findFirst()
                        .orElseThrow()
                        .getID()
                        .toHexString();
        var configManager = new FlinkConfigManager(defaults);
        var nativeService = new CapturingNativeService(configManager.getOperatorConfiguration());
        var josdkContext =
                TestUtils.<FlinkBlueGreenDeployment>createEmptyContextWithClient(kubernetesClient);
        var contextFactory =
                new TestingFlinkResourceContextFactory(
                        configManager,
                        TestUtils.createTestMetricGroup(defaults),
                        nativeService,
                        eventRecorder);

        var parent = new FlinkBlueGreenDeployment();
        parent.setStatus(new FlinkBlueGreenDeploymentStatus());
        parent.setMetadata(
                new ObjectMetaBuilder()
                        .withName("startup-sizing")
                        .withNamespace("local-test")
                        .withUid("parent")
                        .build());
        var template = TestUtils.buildApplicationCluster(FlinkVersion.v1_20).getSpec();
        // Remove the fixture's HA and checkpoint paths; this job is local and stateless.
        template.setFlinkConfiguration(new Configuration());
        template.getJob().setParallelism(2);
        parent.setSpec(
                new FlinkBlueGreenDeploymentSpec(
                        new HashMap<>(),
                        null,
                        FlinkDeploymentTemplateSpec.builder().spec(template).build()));
        parent.getStatus()
                .setLastReconciledSpec(SpecUtils.writeSpecAsJSON(parent.getSpec(), "spec"));

        var source = new FlinkDeployment();
        source.setStatus(source.initStatus());
        source.setMetadata(
                new ObjectMetaBuilder()
                        .withName("startup-sizing-blue")
                        .withNamespace("local-test")
                        .withUid("source")
                        .withGeneration(1L)
                        .build());
        source.setSpec(SpecUtils.clone(template));
        var deployed = SpecUtils.clone(template);
        if (inheritOverride) {
            deployed.getFlinkConfiguration()
                    .put(PipelineOptions.PARALLELISM_OVERRIDES.key(), mapperId + ":4");
        }
        source.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployed, source);
        source.getStatus().getReconciliationStatus().markReconciledSpecAsStable();
        source.getStatus().setObservedGeneration(1L);
        var deployments = new FlinkBlueGreenDeployments();
        deployments.setFlinkDeploymentBlue(source);
        var bgContext =
                new BlueGreenContext(
                        parent, parent.getStatus(), josdkContext, deployments, contextFactory);
        var child =
                BlueGreenUtils.prepareFlinkDeployment(
                        bgContext,
                        BlueGreenDeploymentType.GREEN,
                        null,
                        false,
                        parent.getMetadata(),
                        source);
        // Match the API-created child that the FlinkDeployment controller receives.
        child.getMetadata().setUid("new-child");
        child.getMetadata().setGeneration(1L);
        child.setStatus(child.initStatus());
        assertTrue(child.getStatus().getReconciliationStatus().isBeforeFirstDeployment());
        assertEquals(2, child.getSpec().getJob().getParallelism());
        assertTrue(
                template.getFlinkConfiguration()
                        .asConfiguration()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES)
                        .isEmpty());

        assertTrue(
                source.getSpec()
                        .getFlinkConfiguration()
                        .asConfiguration()
                        .get(PipelineOptions.PARALLELISM_OVERRIDES)
                        .isEmpty());
        assertTrue(
                child.getSpec()
                        .getFlinkConfiguration()
                        .asFlatMap()
                        .containsKey(INHERITANCE_MARKER));
        var childContext = contextFactory.getResourceContext(child, josdkContext);
        var autoscaler =
                new NoopJobAutoscaler<ResourceID, KubernetesJobAutoScalerContext>() {
                    @Override
                    public void scale(KubernetesJobAutoScalerContext ignored) {
                        throw new AssertionError("First submission must not invoke autoscaling");
                    }
                };
        var reconciler =
                new ApplicationReconciler(eventRecorder, new TestingStatusRecorder<>(), autoscaler);
        reconciler.reconcile(childContext);

        assertEquals(1, nativeService.submissions);
        assertNotNull(nativeService.submissionConfig);
        var submissionConfig = nativeService.submissionConfig;
        assertEquals(
                inheritOverride ? Map.of(mapperId, "4") : Map.of(),
                submissionConfig.get(PipelineOptions.PARALLELISM_OVERRIDES));
        assertFalse(
                submissionConfig.keySet().stream().anyMatch(k -> k.startsWith(K8S_OP_CONF_PREFIX)));
        assertEquals(2, submissionConfig.get(CoreOptions.DEFAULT_PARALLELISM));
        assertEquals(
                JobManagerDeploymentStatus.DEPLOYING,
                child.getStatus().getJobManagerDeploymentStatus());
        assertTrue(
                kubernetesClient.apps().deployments().inAnyNamespace().list().getItems().isEmpty());

        // Build the p=2 job from the exact native-service submission configuration.
        // No application code sets p=4; Flink's dispatcher must apply the inherited override.
        var graph = createJobGraph(submissionConfig);
        var mapper = graph.findVertexByID(JobVertexID.fromHexString(mapperId));
        assertNotNull(mapper);
        assertEquals(2, mapper.getParallelism());

        try (var miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(submissionConfig)
                                .setCommonBindAddress("127.0.0.1")
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(4)
                                .withRandomPorts()
                                .build())) {
            miniCluster.start();
            var result = miniCluster.executeJobBlocking(graph);
            int expectedParallelism = inheritOverride ? 4 : 2;
            List<String> expected =
                    IntStream.range(0, expectedParallelism)
                            .mapToObj(i -> i + ":" + expectedParallelism + ":0")
                            .collect(Collectors.toList());
            List<String> observed = result.getAccumulatorResult(OBSERVATIONS);
            assertThat(observed).containsExactlyInAnyOrderElementsOf(expected);
            assertEquals(Long.valueOf(8), result.getAccumulatorResult(RECORDS));
        }
        assertEquals(1, nativeService.submissions);
        assertTrue(
                kubernetesClient.apps().deployments().inAnyNamespace().list().getItems().isEmpty());
    }

    private static JobGraph createJobGraph(Configuration submissionConfig) {
        var env = new StreamExecutionEnvironment(submissionConfig);
        env.disableOperatorChaining();
        env.fromData(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .map(new ObserveFirstAttempt())
                .name(MAPPER_UID)
                .uid(MAPPER_UID)
                .setParallelism(2)
                .setMaxParallelism(128)
                .sinkTo(new DiscardingSink<>())
                .setParallelism(1);
        return FlinkPipelineTranslationUtil.getJobGraph(
                BlueGreenFirstSubmissionTest.class.getClassLoader(),
                env.getStreamGraph(),
                submissionConfig,
                2);
    }

    private static Configuration localRuntimeConfig() {
        var config = new Configuration();
        config.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        config.set(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT, Duration.ZERO);
        config.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofSeconds(10));
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        config.set(JobManagerOptions.ADDRESS, "127.0.0.1");
        config.set(JobManagerOptions.BIND_HOST, "127.0.0.1");
        config.set(TaskManagerOptions.HOST, "127.0.0.1");
        config.set(TaskManagerOptions.BIND_HOST, "127.0.0.1");
        config.set(RestOptions.ADDRESS, "127.0.0.1");
        config.set(RestOptions.BIND_ADDRESS, "127.0.0.1");
        config.set(RestOptions.BIND_PORT, "0");
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.ofMebiBytes(16));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(16));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(16));
        return config;
    }

    private class CapturingNativeService extends NativeFlinkService {
        private Configuration submissionConfig;
        private int submissions;

        private CapturingNativeService(FlinkOperatorConfiguration operatorConfig) {
            super(
                    BlueGreenFirstSubmissionTest.this.kubernetesClient,
                    null,
                    Executors.newDirectExecutorService(),
                    operatorConfig,
                    BlueGreenFirstSubmissionTest.this.eventRecorder);
        }

        @Override
        protected void deployApplicationCluster(JobSpec jobSpec, Configuration conf) {
            assertEquals(0, submissions++, "Only the first native submission may be used");
            submissionConfig = new Configuration(conf);
            // Deliberately never call super: it would invoke the Kubernetes application deployer.
        }
    }

    /** Records every subtask, even one which receives no input records. */
    private static class ObserveFirstAttempt extends RichMapFunction<Integer, Integer> {
        @Override
        public void open(OpenContext openContext) {
            var observations = new ListAccumulator<String>();
            getRuntimeContext().addAccumulator(OBSERVATIONS, observations);
            var taskInfo = getRuntimeContext().getTaskInfo();
            observations.add(
                    taskInfo.getIndexOfThisSubtask()
                            + ":"
                            + taskInfo.getNumberOfParallelSubtasks()
                            + ":"
                            + taskInfo.getAttemptNumber());
        }

        @Override
        public Integer map(Integer value) {
            getRuntimeContext().getLongCounter(RECORDS).add(1);
            return value;
        }
    }
}
