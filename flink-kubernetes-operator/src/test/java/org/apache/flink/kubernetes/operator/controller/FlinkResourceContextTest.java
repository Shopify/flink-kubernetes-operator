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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.ConfigObjectNode;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_FLINK_CLIENT_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Resource Context tests. */
public class FlinkResourceContextTest {

    private Context<HasMetadata> josdkCtx;
    private FlinkConfigManager configManager;
    private Function<FlinkResourceContext<?>, FlinkService> serviceFactory;

    @BeforeEach
    void setup() {
        josdkCtx = TestUtils.createContextWithReadyFlinkDeployment();
        configManager = new FlinkConfigManager(new Configuration());
        serviceFactory = ctx -> new TestingFlinkService();
    }

    @ParameterizedTest
    @MethodSource("crTypes")
    void testCreateService(AbstractFlinkResource<?, ?> cr) {
        var counter = new AtomicInteger(0);
        var service = new TestingFlinkService();
        serviceFactory =
                ctx -> {
                    counter.incrementAndGet();
                    return service;
                };

        var ctx = getContext(cr);
        assertTrue(ctx.getFlinkService() == service);
        assertTrue(ctx.getFlinkService() == service);
        assertTrue(ctx.getFlinkService() == service);
        assertEquals(1, counter.get());
    }

    @Test
    void testOperatorConfigHandling() {
        var counter = new AtomicInteger(0);
        var conf = new Configuration();
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_INTERVAL, Duration.ofMinutes(1));
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_OBSERVER_REST_READY_DELAY,
                Duration.ofMinutes(2));

        conf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + "v1_17.kubernetes.operator.reconcile.interval",
                "17m");
        conf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX
                        + "v1_18.kubernetes.operator.reconcile.interval",
                "18m");

        configManager =
                new FlinkConfigManager(conf) {

                    @Override
                    public Configuration getDefaultConfig(
                            String namespace, FlinkVersion flinkVersion) {
                        counter.incrementAndGet();
                        return super.getDefaultConfig(namespace, flinkVersion);
                    }
                };

        var ctx = getContext(TestUtils.buildApplicationCluster(FlinkVersion.v1_18));
        assertEquals(Duration.ofMinutes(18), ctx.getOperatorConfig().getReconcileInterval());
        assertEquals(Duration.ofMinutes(2), ctx.getOperatorConfig().getRestApiReadyDelay());

        assertEquals(1, counter.get());

        ctx = getContext(TestUtils.buildApplicationCluster(FlinkVersion.v1_17));
        assertEquals(Duration.ofMinutes(17), ctx.getOperatorConfig().getReconcileInterval());
        assertEquals(Duration.ofMinutes(2), ctx.getOperatorConfig().getRestApiReadyDelay());

        assertEquals(2, counter.get());
    }

    @ParameterizedTest
    @MethodSource("crTypes")
    void testFlinkClientTimeoutOverride(AbstractFlinkResource<?, ?> cr) {
        var key = OPERATOR_FLINK_CLIENT_TIMEOUT.key();
        var alias = "kubernetes.operator.observer.flink.client.timeout";
        var conf = new Configuration();
        conf.set(OPERATOR_FLINK_CLIENT_TIMEOUT, Duration.ofSeconds(12));
        conf.setString(
                KubernetesOperatorConfigOptions.NAMESPACE_CONF_PREFIX
                        + cr.getMetadata().getNamespace()
                        + "."
                        + key,
                "13 s");
        var version = getContext(cr).getFlinkVersion();
        conf.setString(
                KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX + version + "." + key, "14 s");
        configManager = new FlinkConfigManager(conf);
        assertEquals(
                Duration.ofSeconds(14), getContext(cr).getOperatorConfig().getFlinkClientTimeout());

        // Resolve the resource alias before merging with canonical operator defaults.
        cr.getSpec().getFlinkConfiguration().put(alias, "15 s");
        var ctx = getContext(cr);
        assertEquals(Duration.ofSeconds(15), ctx.getOperatorConfig().getFlinkClientTimeout());
        assertSame(ctx.getOperatorConfig(), ctx.getOperatorConfig());
        var sibling = ReconciliationUtils.clone(cr);
        sibling.getMetadata().setName("other-job");
        sibling.getSpec().getFlinkConfiguration().remove(alias);
        assertEquals(
                Duration.ofSeconds(14),
                getContext(sibling).getOperatorConfig().getFlinkClientTimeout());

        ReconciliationUtils.updateStatusForDeployedSpec(cr, new Configuration());
        cr.getSpec().getFlinkConfiguration().put(key, "20 s");
        cr.getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM.key(), "99");
        assertEquals(
                Duration.ofSeconds(20), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
        assertEquals(
                conf.get(KubernetesOperatorConfigOptions.OPERATOR_RECONCILE_PARALLELISM),
                getContext(cr).getOperatorConfig().getReconcilerMaxParallelism());

        // A fresh context sees updates; an existing context remains a consistent snapshot.
        assertEquals(Duration.ofSeconds(15), ctx.getOperatorConfig().getFlinkClientTimeout());
        cr.getSpec().getFlinkConfiguration().remove(key);
        assertEquals(
                Duration.ofSeconds(15), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
        cr.getSpec().getFlinkConfiguration().remove(alias);
        assertEquals(
                Duration.ofSeconds(14), getContext(cr).getOperatorConfig().getFlinkClientTimeout());

        // No resource override may mutate namespace/version defaults shared with another job.
        assertEquals(
                Duration.ofSeconds(14),
                configManager
                        .getOperatorConfiguration(cr.getMetadata().getNamespace(), version)
                        .getFlinkClientTimeout());
        cr.getSpec().setFlinkConfiguration((ConfigObjectNode) null);
        assertEquals(
                Duration.ofSeconds(14), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
    }

    @ParameterizedTest
    @MethodSource("crTypes")
    void testInvalidFlinkClientTimeoutUsesLastReconciledSpec(AbstractFlinkResource<?, ?> cr) {
        var key = OPERATOR_FLINK_CLIENT_TIMEOUT.key();
        cr.getSpec().getFlinkConfiguration().put(key, "15 s");
        ReconciliationUtils.updateStatusForDeployedSpec(cr, new Configuration());
        cr.getSpec().getFlinkConfiguration().put(key, "invalid");
        assertEquals(
                Duration.ofSeconds(15), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
    }

    @ParameterizedTest
    @MethodSource("crTypes")
    void testInvalidInitialFlinkClientTimeoutUsesDefaults(AbstractFlinkResource<?, ?> cr) {
        cr.getSpec().getFlinkConfiguration().put(OPERATOR_FLINK_CLIENT_TIMEOUT.key(), "500 ms");
        assertEquals(
                Duration.ofSeconds(10), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
        // A spec accepted by an older operator may itself contain an invalid override.
        ReconciliationUtils.updateStatusForDeployedSpec(cr, new Configuration());
        assertEquals(
                Duration.ofSeconds(10), getContext(cr).getOperatorConfig().getFlinkClientTimeout());
    }

    @Test
    void testSessionJobTimeoutDoesNotInheritClusterOverride() {
        josdkCtx.getSecondaryResource(FlinkDeployment.class)
                .orElseThrow()
                .getSpec()
                .getFlinkConfiguration()
                .put(OPERATOR_FLINK_CLIENT_TIMEOUT.key(), "30 s");
        var job = TestUtils.buildSessionJob();
        assertEquals(
                Duration.ofSeconds(10),
                getContext(job).getOperatorConfig().getFlinkClientTimeout());
        job.getSpec().getFlinkConfiguration().put(OPERATOR_FLINK_CLIENT_TIMEOUT.key(), "15 s");
        assertEquals(
                Duration.ofSeconds(15),
                getContext(job).getOperatorConfig().getFlinkClientTimeout());
        var sibling = ReconciliationUtils.clone(job);
        sibling.getMetadata().setName("other-job");
        sibling.getSpec().getFlinkConfiguration().remove(OPERATOR_FLINK_CLIENT_TIMEOUT.key());
        assertEquals(
                Duration.ofSeconds(10),
                getContext(sibling).getOperatorConfig().getFlinkClientTimeout());
    }

    @Test
    void testObserveConfigHandling() {
        var counter = new AtomicInteger(0);
        var conf = new Configuration();
        configManager =
                new FlinkConfigManager(new Configuration()) {
                    @Override
                    public Configuration getObserveConfig(FlinkDeployment deployment) {
                        counter.incrementAndGet();
                        return conf;
                    }
                };

        var deployCtx = getContext(TestUtils.buildApplicationCluster());
        assertTrue(deployCtx.getObserveConfig() == conf);
        assertTrue(deployCtx.getObserveConfig() == conf);
        assertTrue(deployCtx.getObserveConfig() == conf);
        assertEquals(1, counter.get());

        josdkCtx = TestUtils.createEmptyContext();
        var sessionJobCtx = getContext(TestUtils.buildSessionJob());
        assertNull(sessionJobCtx.getObserveConfig());
        assertEquals(1, counter.get());

        josdkCtx = TestUtils.createContextWithReadyFlinkDeployment();
        sessionJobCtx = getContext(TestUtils.buildSessionJob());
        assertTrue(sessionJobCtx.getObserveConfig() == conf);
        assertTrue(sessionJobCtx.getObserveConfig() == conf);
        assertEquals(2, counter.get());
    }

    FlinkResourceContext getContext(AbstractFlinkResource<?, ?> cr) {
        if (cr instanceof FlinkDeployment) {
            return new FlinkDeploymentContext(
                    (FlinkDeployment) cr, josdkCtx, null, configManager, serviceFactory, null);
        } else {
            return new FlinkSessionJobContext(
                    (FlinkSessionJob) cr, josdkCtx, null, configManager, serviceFactory, null);
        }
    }

    private static Stream<AbstractFlinkResource<?, ?>> crTypes() {
        return Stream.of(TestUtils.buildApplicationCluster(), TestUtils.buildSessionJob());
    }
}
