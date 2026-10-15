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
import org.apache.flink.metrics.Gauge;

import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link LeaderElectionMetric} tests. */
class LeaderElectionMetricTest {

    @Test
    void testGaugeTracksRepeatedLeadershipTransitions() {
        var listener = new TestingMetricListener(new Configuration());
        var metric = new LeaderElectionMetric(listener.getMetricGroup());
        var gauge = getGauge(listener);

        assertEquals(Integer.valueOf(0), gauge.getValue());

        metric.onStartLeading();
        assertEquals(Integer.valueOf(1), gauge.getValue());
        metric.onStartLeading();
        assertEquals(Integer.valueOf(1), gauge.getValue());

        metric.onStopLeading();
        assertEquals(Integer.valueOf(0), gauge.getValue());
        metric.onStopLeading();
        assertEquals(Integer.valueOf(0), gauge.getValue());
    }

    @Test
    void testDecoratePreservesConfigurationAndDelegatesInOrder() {
        var listener = new TestingMetricListener(new Configuration());
        var metric = new LeaderElectionMetric(listener.getMetricGroup());
        var gauge = getGauge(listener);
        var callbackStates = new ArrayList<Integer>();
        var newLeader = new AtomicReference<String>();
        var original =
                new LeaderElectionConfiguration(
                        "test-lease",
                        "test-namespace",
                        Duration.ofSeconds(21),
                        Duration.ofSeconds(13),
                        Duration.ofSeconds(3),
                        "test-identity",
                        new LeaderCallbacks(
                                () -> callbackStates.add(gauge.getValue()),
                                () -> callbackStates.add(gauge.getValue()),
                                newLeader::set),
                        false);

        var decorated = metric.decorate(original);

        assertEquals(original.getLeaseName(), decorated.getLeaseName());
        assertEquals(original.getLeaseNamespace(), decorated.getLeaseNamespace());
        assertEquals(original.getLeaseDuration(), decorated.getLeaseDuration());
        assertEquals(original.getRenewDeadline(), decorated.getRenewDeadline());
        assertEquals(original.getRetryPeriod(), decorated.getRetryPeriod());
        assertEquals(original.getIdentity(), decorated.getIdentity());
        assertFalse(decorated.isExitOnStopLeading());

        var callbacks = decorated.getLeaderCallbacks().orElseThrow();
        callbacks.onNewLeader("another-replica");
        assertEquals("another-replica", newLeader.get());
        assertEquals(Integer.valueOf(0), gauge.getValue());
        callbacks.onStartLeading();
        assertEquals(Integer.valueOf(1), gauge.getValue());
        callbacks.onNewLeader("new-leader");
        assertEquals(Integer.valueOf(1), gauge.getValue());
        callbacks.onStopLeading();

        assertEquals(List.of(0, 0), callbackStates);
        assertEquals("new-leader", newLeader.get());
        assertEquals(Integer.valueOf(0), gauge.getValue());
    }

    @Test
    void testDecoratePreservesAbsentOptionalsAndExitOnStopLeading() {
        var listener = new TestingMetricListener(new Configuration());
        var metric = new LeaderElectionMetric(listener.getMetricGroup());
        var original =
                new LeaderElectionConfiguration(
                        "test-lease",
                        null,
                        Duration.ofSeconds(22),
                        Duration.ofSeconds(14),
                        Duration.ofSeconds(4),
                        null,
                        null,
                        true);

        var decorated = metric.decorate(original);

        assertEquals(original.getLeaseName(), decorated.getLeaseName());
        assertTrue(decorated.getLeaseNamespace().isEmpty());
        assertEquals(original.getLeaseDuration(), decorated.getLeaseDuration());
        assertEquals(original.getRenewDeadline(), decorated.getRenewDeadline());
        assertEquals(original.getRetryPeriod(), decorated.getRetryPeriod());
        assertTrue(decorated.getIdentity().isEmpty());
        assertTrue(decorated.isExitOnStopLeading());
        assertTrue(decorated.getLeaderCallbacks().isPresent());
    }

    @Test
    void testDelegateFailuresPropagateWithSafeMetricState() {
        var listener = new TestingMetricListener(new Configuration());
        var metric = new LeaderElectionMetric(listener.getMetricGroup());
        var gauge = getGauge(listener);
        var startFailure = new RuntimeException("start failure");
        var stopFailure = new RuntimeException("stop failure");
        var newLeaderFailure = new RuntimeException("new leader failure");
        var original =
                new LeaderElectionConfiguration(
                        "test-lease",
                        null,
                        Duration.ofSeconds(15),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(2),
                        null,
                        new LeaderCallbacks(
                                () -> {
                                    throw startFailure;
                                },
                                () -> {
                                    throw stopFailure;
                                },
                                ignored -> {
                                    throw newLeaderFailure;
                                }),
                        true);
        var callbacks = metric.decorate(original).getLeaderCallbacks().orElseThrow();

        assertSame(startFailure, assertThrows(RuntimeException.class, callbacks::onStartLeading));
        assertEquals(Integer.valueOf(0), gauge.getValue());

        metric.onStartLeading();
        assertSame(stopFailure, assertThrows(RuntimeException.class, callbacks::onStopLeading));
        assertEquals(Integer.valueOf(0), gauge.getValue());

        assertSame(
                newLeaderFailure,
                assertThrows(RuntimeException.class, () -> callbacks.onNewLeader("other")));
        assertEquals(Integer.valueOf(0), gauge.getValue());
    }

    private static Gauge<Integer> getGauge(TestingMetricListener listener) {
        return listener.<Integer>getGauge(listener.getMetricId("LeaderElection", "IsLeader"))
                .orElseThrow();
    }
}
