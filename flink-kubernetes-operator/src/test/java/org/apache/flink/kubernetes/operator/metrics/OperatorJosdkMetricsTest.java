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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentController;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.metrics.Histogram;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** {@link OperatorJosdkMetrics} tests. */
public class OperatorJosdkMetricsTest {

    private static final ResourceID resourceId = new ResourceID("testname", "testns");
    private static final HasMetadata resource = testResource(resourceId);

    private static HasMetadata testResource(ResourceID resourceId) {
        var flinkDeployment = new FlinkDeployment();
        flinkDeployment.getMetadata().setName(resourceId.getName());
        flinkDeployment.getMetadata().setNamespace(resourceId.getNamespace().orElseThrow());
        return flinkDeployment;
    }

    private static final String controllerName = FlinkDeploymentController.class.getSimpleName();
    private static final Map<String, Object> metadata =
            Map.of(Constants.RESOURCE_GVK_KEY, GroupVersionKind.gvkFor(FlinkDeployment.class));

    private OperatorJosdkMetrics operatorMetrics;
    private TestingMetricListener listener;

    @BeforeEach
    public void setup() {
        listener = new TestingMetricListener(new Configuration());
        operatorMetrics =
                new OperatorJosdkMetrics(
                        listener.getMetricGroup(), new FlinkConfigManager(new Configuration()));
    }

    @Test
    public void testTimeControllerExecution() throws Exception {
        var successExecution = new TestingExecutionBase<>();
        operatorMetrics.timeControllerExecution(successExecution);

        assertEquals(1, listener.size());
        assertEquals(1, getHistogram("reconcile", "resource").getCount());
        assertEquals(1, getHistogram("reconcile", "resource").getStatistics().getMin());
        operatorMetrics.timeControllerExecution(successExecution);
        operatorMetrics.timeControllerExecution(successExecution);
        assertEquals(1, listener.size());
        assertEquals(3, getHistogram("reconcile", "resource").getCount());
        assertEquals(1, getHistogram("reconcile", "resource").getStatistics().getMin());

        var failureExecution =
                new TestingExecutionBase<>() {
                    @Override
                    public String name() {
                        return "cleanup";
                    }

                    @Override
                    public Object execute() throws Exception {
                        Thread.sleep(1000);
                        throw new ReconciliationException(new RuntimeException());
                    }
                };
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception e) {
            assertEquals(2, listener.size());
            assertEquals(1, getHistogram("cleanup", "failed").getCount());
            assertEquals(1, getHistogram("cleanup", "failed").getStatistics().getMin());
        }
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception ignored) {
        }
        try {
            operatorMetrics.timeControllerExecution(failureExecution);
            fail();
        } catch (Exception e) {
            assertEquals(2, listener.size());
            assertEquals(3, getHistogram("cleanup", "failed").getCount());
            assertEquals(1, getHistogram("cleanup", "failed").getStatistics().getMin());
        }
    }

    @Test
    public void testMetrics() {
        // JOSDK always reports a reconcile as submitted before it reports it failed or finished.
        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        assertEquals(1, listener.size());
        assertEquals(1, getCount("Reconciliation"));

        operatorMetrics.failedReconciliation(resource, null, metadata);
        assertEquals(2, listener.size());
        assertEquals(1, getCount("Reconciliation.failed"));
        operatorMetrics.failedReconciliation(resource, null, metadata);
        operatorMetrics.failedReconciliation(resource, null, metadata);
        assertEquals(2, listener.size());
        assertEquals(3, getCount("Reconciliation.failed"));

        operatorMetrics.reconcileCustomResource(
                resource,
                new RetryInfo() {
                    @Override
                    public int getAttemptCount() {
                        return 0;
                    }

                    @Override
                    public boolean isLastAttempt() {
                        return false;
                    }
                },
                metadata);
        assertEquals(3, listener.size());
        assertEquals(2, getCount("Reconciliation"));
        assertEquals(1, getCount("Reconciliation.retries"));

        operatorMetrics.receivedEvent(
                new ResourceEvent(ResourceAction.ADDED, resourceId, null), metadata);
        assertEquals(5, listener.size());
        assertEquals(1, getCount("Resource.Event"));
        assertEquals(1, getCount("Resource.Event.ADDED"));

        operatorMetrics.finishedReconciliation(resource, metadata);
        assertEquals(6, listener.size());
        assertEquals(1, getCount("Reconciliation.finished"));

        operatorMetrics.monitorSizeOf(Map.of("a", "b", "c", "d"), "mymap");
        assertEquals(7, listener.size());
        assertEquals(
                2,
                listener.getGauge(listener.getMetricId("JOSDK", "mymap", "size")).get().getValue());

        operatorMetrics.reconcileCustomResource(
                testResource(new ResourceID("other", "otherns")), null, metadata);
        assertEquals(8, listener.size());
        assertEquals(
                1,
                listener.getCounter(
                                listener.getResourceMetricId(
                                        FlinkDeployment.class,
                                        "otherns",
                                        "other",
                                        "JOSDK",
                                        "Reconciliation",
                                        "Count"))
                        .get()
                        .getCount());
    }

    @Test
    public void testCleanupUnregistersResourceMetrics() {
        operatorMetrics.receivedEvent(
                new ResourceEvent(ResourceAction.ADDED, resourceId, null), metadata);
        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        operatorMetrics.finishedReconciliation(resource, metadata);
        operatorMetrics.failedReconciliation(resource, null, metadata);
        assertEquals(5, listener.size());

        operatorMetrics.cleanupDoneFor(resourceId, metadata);

        assertEquals(0, listener.size());
    }

    @Test
    public void testDeletionWithCleanerLeavesNoResourceMetrics() {
        operatorMetrics.receivedEvent(
                new ResourceEvent(ResourceAction.ADDED, resourceId, null), metadata);
        operatorMetrics.reconcileCustomResource(resource, null, metadata);

        // JOSDK 5.2.2 callback order when a controller with a Cleaner handles a delete:
        // deletionTimestamp is set (UPDATED), cleanup reconcile finishes and the finalizer is
        // removed (first cleanupDoneFor), then the object is gone (DELETED event, which records
        // metrics again, followed by a second cleanupDoneFor).
        operatorMetrics.receivedEvent(
                new ResourceEvent(ResourceAction.UPDATED, resourceId, null), metadata);
        operatorMetrics.finishedReconciliation(resource, metadata);
        operatorMetrics.cleanupDoneFor(resourceId, metadata);
        operatorMetrics.receivedEvent(
                new ResourceEvent(ResourceAction.DELETED, resourceId, null), metadata);
        operatorMetrics.cleanupDoneFor(resourceId, metadata);

        assertEquals(0, listener.size());
    }

    @Test
    public void testCallbacksAfterCleanupDoNotReRegisterMetrics() {
        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        operatorMetrics.cleanupDoneFor(resourceId, metadata);

        operatorMetrics.finishedReconciliation(resource, metadata);
        operatorMetrics.failedReconciliation(resource, null, metadata);

        assertEquals(0, listener.size());
    }

    @Test
    public void testCleanupOnlyAffectsTheSameKind() {
        var snapshot = new FlinkStateSnapshot();
        snapshot.getMetadata().setName(resourceId.getName());
        snapshot.getMetadata().setNamespace(resourceId.getNamespace().orElseThrow());
        var snapshotMetadata =
                Map.<String, Object>of(
                        Constants.RESOURCE_GVK_KEY,
                        GroupVersionKind.gvkFor(FlinkStateSnapshot.class));

        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        operatorMetrics.reconcileCustomResource(snapshot, null, snapshotMetadata);
        assertEquals(1, getCount(FlinkDeployment.class, "Reconciliation"));
        assertEquals(1, getCount(FlinkStateSnapshot.class, "Reconciliation"));

        operatorMetrics.cleanupDoneFor(resourceId, snapshotMetadata);

        assertEquals(1, listener.size());
        assertEquals(1, getCount(FlinkDeployment.class, "Reconciliation"));
        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        assertEquals(2, getCount(FlinkDeployment.class, "Reconciliation"));
    }

    @Test
    public void testRecreatedResourceStartsFromZero() {
        operatorMetrics.reconcileCustomResource(resource, null, metadata);
        operatorMetrics.cleanupDoneFor(resourceId, metadata);

        operatorMetrics.reconcileCustomResource(resource, null, metadata);

        assertEquals(1, getCount("Reconciliation"));
    }

    @Test
    public void testCleanupKeepsSharedHistograms() throws Exception {
        var execution = new TestingExecutionBase<>();
        operatorMetrics.timeControllerExecution(execution);
        operatorMetrics.reconcileCustomResource(resource, null, metadata);

        operatorMetrics.cleanupDoneFor(resourceId, metadata);
        operatorMetrics.timeControllerExecution(execution);

        var histogram =
                listener.getHistogram(
                        listener.getNamespaceMetricId(
                                FlinkDeployment.class,
                                "testns",
                                "JOSDK",
                                "reconcile.resource",
                                "TimeSeconds"));
        assertTrue(histogram.isPresent(), "shared histogram was unregistered by cleanup");
        assertEquals(2, histogram.get().getCount());
    }

    private Histogram getHistogram(String... names) {
        return listener.getHistogram(
                        listener.getNamespaceMetricId(
                                FlinkDeployment.class,
                                "testns",
                                "JOSDK",
                                String.join(".", names),
                                "TimeSeconds"))
                .get();
    }

    private long getCount(String name) {
        return getCount(FlinkDeployment.class, name);
    }

    private long getCount(Class<? extends CustomResource<?, ?>> resourceClass, String name) {
        var counter =
                listener.getCounter(
                        listener.getResourceMetricId(
                                resourceClass, "testns", "testname", "JOSDK", name, "Count"));
        assertTrue(
                counter.isPresent(),
                "no "
                        + name
                        + " counter for "
                        + resourceClass.getSimpleName()
                        + " testns/testname");
        return counter.get().getCount();
    }

    private static class TestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
        @Override
        public String controllerName() {
            return controllerName;
        }

        @Override
        public String successTypeName(Object o) {
            return "resource";
        }

        @Override
        public ResourceID resourceID() {
            return resourceId;
        }

        @Override
        public Map<String, Object> metadata() {
            return metadata;
        }

        @Override
        public String name() {
            return "reconcile";
        }

        @Override
        public T execute() throws Exception {
            Thread.sleep(1000);
            return null;
        }
    }
}
