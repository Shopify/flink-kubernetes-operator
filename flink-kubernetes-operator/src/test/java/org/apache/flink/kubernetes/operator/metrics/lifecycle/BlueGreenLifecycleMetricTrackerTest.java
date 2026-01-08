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
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.metrics.OperatorMetricUtils;
import org.apache.flink.metrics.Histogram;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenLifecycleMetricTracker.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link BlueGreenLifecycleMetricTracker}. */
public class BlueGreenLifecycleMetricTrackerTest {

    @Test
    public void testInitialDeployment() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        INITIALIZING_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        // T=0: INITIALIZING_BLUE
        // T=5s: Still INITIALIZING_BLUE (just updating timestamp)
        // T=10s: ACTIVE_BLUE (initial deployment complete)
        tracker.onUpdate(INITIALIZING_BLUE, Instant.ofEpochMilli(5000));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(10000));

        // Should record InitialDeployment transition: 10s from start
        validateTransition(transitionHistos, TRANSITION_INITIAL_DEPLOYMENT, 1, 10);

        // Should record INITIALIZING_BLUE time: 5s (from 0 to 5)
        validateTime(timeHistos, INITIALIZING_BLUE, 1, 5);
    }

    @Test
    public void testBlueToGreenTransition() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        // Start in ACTIVE_BLUE (after initial deployment)
        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        ACTIVE_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        long ts = 0;

        // Simulate: ACTIVE_BLUE -> TRANSITIONING_TO_GREEN -> ACTIVE_GREEN
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 5000)); // 5s in ACTIVE_BLUE
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochMilli(ts += 5000)); // T=10s
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochMilli(ts += 3000)); // T=13s
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(ts += 2000)); // T=15s

        // BlueToGreen transition: from ACTIVE_BLUE start (0) to ACTIVE_GREEN (15s) = 15s
        validateTransition(transitionHistos, TRANSITION_BLUE_TO_GREEN, 1, 15);

        // ACTIVE_BLUE time: from 0 to 10 = 10s (when we left for TRANSITIONING)
        validateTime(timeHistos, ACTIVE_BLUE, 1, 10);

        // TRANSITIONING_TO_GREEN time: from 10s to 15s = 5s
        validateTime(timeHistos, TRANSITIONING_TO_GREEN, 1, 5);
    }

    @Test
    public void testBlueToGreenWithSavepointing() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        ACTIVE_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        long ts = 0;

        // Simulate: ACTIVE_BLUE -> SAVEPOINTING_BLUE -> TRANSITIONING_TO_GREEN -> ACTIVE_GREEN
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 5000)); // 5s in ACTIVE_BLUE
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochMilli(ts += 5000)); // T=10s
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochMilli(ts += 7000)); // T=17s
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochMilli(ts += 3000)); // T=20s
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(ts += 5000)); // T=25s

        // BlueToGreen transition: from ACTIVE_BLUE start (0) to ACTIVE_GREEN (25s) = 25s
        validateTransition(transitionHistos, TRANSITION_BLUE_TO_GREEN, 1, 25);

        // State times
        validateTime(timeHistos, ACTIVE_BLUE, 1, 10); // 0 to 10
        validateTime(timeHistos, SAVEPOINTING_BLUE, 1, 10); // 10 to 20
        validateTime(timeHistos, TRANSITIONING_TO_GREEN, 1, 5); // 20 to 25
    }

    @Test
    public void testGreenToBlueTransition() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        // Start in ACTIVE_GREEN
        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        ACTIVE_GREEN, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        long ts = 0;

        // Simulate: ACTIVE_GREEN -> SAVEPOINTING_GREEN -> TRANSITIONING_TO_BLUE -> ACTIVE_BLUE
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(ts += 5000)); // 5s in ACTIVE_GREEN
        tracker.onUpdate(SAVEPOINTING_GREEN, Instant.ofEpochMilli(ts += 5000)); // T=10s
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochMilli(ts += 8000)); // T=18s
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 2000)); // T=20s

        // GreenToBlue transition: from ACTIVE_GREEN start (0) to ACTIVE_BLUE (20s) = 20s
        validateTransition(transitionHistos, TRANSITION_GREEN_TO_BLUE, 1, 20);

        // State times
        validateTime(timeHistos, ACTIVE_GREEN, 1, 10); // 0 to 10
        validateTime(timeHistos, SAVEPOINTING_GREEN, 1, 8); // 10 to 18
        validateTime(timeHistos, TRANSITIONING_TO_BLUE, 1, 2); // 18 to 20
    }

    @Test
    public void testMultipleTransitionCycles() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        // Full lifecycle: INITIALIZING_BLUE -> ACTIVE_BLUE -> ACTIVE_GREEN -> ACTIVE_BLUE
        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        INITIALIZING_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        long ts = 0;

        // Initial deployment: INITIALIZING_BLUE -> ACTIVE_BLUE
        tracker.onUpdate(INITIALIZING_BLUE, Instant.ofEpochMilli(ts += 2000));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 3000)); // T=5s

        validateTransition(transitionHistos, TRANSITION_INITIAL_DEPLOYMENT, 1, 5);
        validateTime(timeHistos, INITIALIZING_BLUE, 1, 2); // 0 to 2

        // Blue to Green: ACTIVE_BLUE -> TRANSITIONING_TO_GREEN -> ACTIVE_GREEN
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 5000)); // T=10s
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochMilli(ts += 5000)); // T=15s
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(ts += 5000)); // T=20s

        validateTransition(transitionHistos, TRANSITION_BLUE_TO_GREEN, 1, 15); // from T=5 to T=20
        validateTime(timeHistos, ACTIVE_BLUE, 1, 10); // 5 to 15

        // Green to Blue: ACTIVE_GREEN -> TRANSITIONING_TO_BLUE -> ACTIVE_BLUE
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(ts += 3000)); // T=23s
        tracker.onUpdate(TRANSITIONING_TO_BLUE, Instant.ofEpochMilli(ts += 2000)); // T=25s
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(ts += 5000)); // T=30s

        validateTransition(transitionHistos, TRANSITION_GREEN_TO_BLUE, 1, 10); // from T=20 to T=30
        validateTime(timeHistos, ACTIVE_GREEN, 2, 3 + 5); // second entry: 20 to 25 = 5s
    }

    @Test
    public void testSameStateUpdatesDoNotRecordMetrics() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        ACTIVE_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        // Multiple updates in the same state should just update timestamp, not record metrics
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(1000));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(2000));
        tracker.onUpdate(ACTIVE_BLUE, Instant.ofEpochMilli(3000));

        // No transitions should be recorded yet
        assertEquals(0, getHistogramCount(transitionHistos, TRANSITION_BLUE_TO_GREEN));
        assertEquals(0, getHistogramCount(transitionHistos, TRANSITION_GREEN_TO_BLUE));

        // No state times should be recorded yet (only recorded at stable state transitions)
        assertEquals(0, getHistogramCount(timeHistos, ACTIVE_BLUE));
    }

    @Test
    public void testIntermediateStatesNotClearedUntilStable() {
        var transitionHistos = initTransitionHistos();
        var timeHistos = initTimeHistos();

        var tracker =
                new BlueGreenLifecycleMetricTracker(
                        ACTIVE_BLUE, Instant.ofEpochMilli(0), transitionHistos, timeHistos);

        // Enter intermediate states but don't reach stable state yet
        tracker.onUpdate(SAVEPOINTING_BLUE, Instant.ofEpochMilli(5000));
        tracker.onUpdate(TRANSITIONING_TO_GREEN, Instant.ofEpochMilli(10000));

        // No state times should be recorded yet (only recorded when reaching stable state)
        assertEquals(0, getHistogramCount(timeHistos, ACTIVE_BLUE));
        assertEquals(0, getHistogramCount(timeHistos, SAVEPOINTING_BLUE));
        assertEquals(0, getHistogramCount(timeHistos, TRANSITIONING_TO_GREEN));

        // Now reach stable state
        tracker.onUpdate(ACTIVE_GREEN, Instant.ofEpochMilli(15000));

        // Now all accumulated states should be recorded
        validateTime(timeHistos, ACTIVE_BLUE, 1, 5); // 0 to 5
        validateTime(timeHistos, SAVEPOINTING_BLUE, 1, 5); // 5 to 10
        validateTime(timeHistos, TRANSITIONING_TO_GREEN, 1, 5); // 10 to 15
    }

    // ==================== Helper Methods ====================

    private Map<String, List<Histogram>> initTransitionHistos() {
        var histos = new ConcurrentHashMap<String, List<Histogram>>();
        histos.put(TRANSITION_INITIAL_DEPLOYMENT, List.of(createHistogram()));
        histos.put(TRANSITION_BLUE_TO_GREEN, List.of(createHistogram()));
        histos.put(TRANSITION_GREEN_TO_BLUE, List.of(createHistogram()));
        return histos;
    }

    private Map<FlinkBlueGreenDeploymentState, List<Histogram>> initTimeHistos() {
        var histos = new ConcurrentHashMap<FlinkBlueGreenDeploymentState, List<Histogram>>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            histos.put(state, List.of(createHistogram()));
        }
        return histos;
    }

    private Histogram createHistogram() {
        return OperatorMetricUtils.createHistogram(
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
    }

    private void validateTransition(
            Map<String, List<Histogram>> histos, String name, int size, long mean) {
        histos.get(name)
                .forEach(
                        h -> {
                            var stat = h.getStatistics();
                            assertEquals(size, stat.size(), "Transition " + name + " count");
                            assertEquals(mean, stat.getMean(), "Transition " + name + " mean");
                        });
    }

    private void validateTime(
            Map<FlinkBlueGreenDeploymentState, List<Histogram>> histos,
            FlinkBlueGreenDeploymentState state,
            int size,
            long sum) {
        histos.get(state)
                .forEach(
                        h -> {
                            var stat = h.getStatistics();
                            assertEquals(size, stat.size(), "State " + state + " count");
                            assertEquals(
                                    sum,
                                    LongStream.of(stat.getValues()).sum(),
                                    "State " + state + " sum");
                        });
    }

    private long getHistogramCount(Map<String, List<Histogram>> histos, String name) {
        return histos.get(name).get(0).getStatistics().size();
    }

    private long getHistogramCount(
            Map<FlinkBlueGreenDeploymentState, List<Histogram>> histos,
            FlinkBlueGreenDeploymentState state) {
        return histos.get(state).get(0).getStatistics().size();
    }
}


