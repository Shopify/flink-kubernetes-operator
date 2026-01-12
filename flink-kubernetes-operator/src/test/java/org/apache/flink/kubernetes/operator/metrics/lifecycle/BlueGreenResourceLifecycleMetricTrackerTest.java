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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_BLUE_TO_GREEN;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_GREEN_TO_BLUE;
import static org.apache.flink.kubernetes.operator.metrics.lifecycle.BlueGreenResourceLifecycleMetricTracker.TRANSITION_INITIAL_DEPLOYMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link BlueGreenResourceLifecycleMetricTracker}. */
public class BlueGreenResourceLifecycleMetricTrackerTest {

    private Map<String, List<Histogram>> transitionHistos;
    private Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos;
    private long currentTimeSeconds;

    @BeforeEach
    void setUp() {
        transitionHistos = new ConcurrentHashMap<>();
        transitionHistos.put(TRANSITION_INITIAL_DEPLOYMENT, List.of(createHistogram()));
        transitionHistos.put(TRANSITION_BLUE_TO_GREEN, List.of(createHistogram()));
        transitionHistos.put(TRANSITION_GREEN_TO_BLUE, List.of(createHistogram()));

        stateTimeHistos = new ConcurrentHashMap<>();
        for (FlinkBlueGreenDeploymentState state : FlinkBlueGreenDeploymentState.values()) {
            stateTimeHistos.put(state, List.of(createHistogram()));
        }

        currentTimeSeconds = 0;
    }

    // ==================== Initial Deployment ====================
    // Note: Real flow is INITIALIZING_BLUE → TRANSITIONING_TO_BLUE → ACTIVE_BLUE

    @Test
    void initialDeployment_recordsTransitionTime() {
        var tracker = createTracker(INITIALIZING_BLUE);
        tick(5);
        tracker.onUpdate(TRANSITIONING_TO_BLUE, now());
        tick(5);

        tracker.onUpdate(ACTIVE_BLUE, now());

        // InitialDeployment measures from INITIALIZING_BLUE entry to ACTIVE_BLUE
        assertTransitionRecorded(TRANSITION_INITIAL_DEPLOYMENT, 10);
    }

    @Test
    void initialDeployment_recordsStateTime() {
        var tracker = createTracker(INITIALIZING_BLUE);
        tick(5);
        tracker.onUpdate(TRANSITIONING_TO_BLUE, now());
        tick(5);

        tracker.onUpdate(ACTIVE_BLUE, now());

        // Both INITIALIZING_BLUE and TRANSITIONING_TO_BLUE state times should be recorded
        assertStateTimeRecorded(INITIALIZING_BLUE, 5);
        assertStateTimeRecorded(TRANSITIONING_TO_BLUE, 5);
    }

    // ==================== Blue to Green Transition ====================

    @Test
    void blueToGreen_recordsTransitionTime() {
        var tracker = createTracker(ACTIVE_BLUE);
        tick(5);
        tracker.onUpdate(SAVEPOINTING_BLUE, now());
        tick(10);
        tracker.onUpdate(TRANSITIONING_TO_GREEN, now());
        tick(5);

        tracker.onUpdate(ACTIVE_GREEN, now());

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 20);
    }

    @Test
    void blueToGreen_recordsAllIntermediateStateTimes() {
        var tracker = createTracker(ACTIVE_BLUE);
        tick(5);
        tracker.onUpdate(SAVEPOINTING_BLUE, now());
        tick(10);
        tracker.onUpdate(TRANSITIONING_TO_GREEN, now());
        tick(3);

        tracker.onUpdate(ACTIVE_GREEN, now());

        assertStateTimeRecorded(ACTIVE_BLUE, 5);
        assertStateTimeRecorded(SAVEPOINTING_BLUE, 10);
        assertStateTimeRecorded(TRANSITIONING_TO_GREEN, 3);
    }

    // ==================== Green to Blue Transition ====================

    @Test
    void greenToBlue_recordsTransitionTime() {
        var tracker = createTracker(ACTIVE_GREEN);
        tick(5);
        tracker.onUpdate(SAVEPOINTING_GREEN, now());
        tick(8);
        tracker.onUpdate(TRANSITIONING_TO_BLUE, now());
        tick(2);

        tracker.onUpdate(ACTIVE_BLUE, now());

        assertTransitionRecorded(TRANSITION_GREEN_TO_BLUE, 15);
    }

    @Test
    void greenToBlue_recordsAllIntermediateStateTimes() {
        var tracker = createTracker(ACTIVE_GREEN);
        tick(5);
        tracker.onUpdate(SAVEPOINTING_GREEN, now());
        tick(8);
        tracker.onUpdate(TRANSITIONING_TO_BLUE, now());
        tick(2);

        tracker.onUpdate(ACTIVE_BLUE, now());

        assertStateTimeRecorded(ACTIVE_GREEN, 5);
        assertStateTimeRecorded(SAVEPOINTING_GREEN, 8);
        assertStateTimeRecorded(TRANSITIONING_TO_BLUE, 2);
    }

    // ==================== Edge Cases ====================

    @Test
    void sameStateUpdates_onlyUpdateTimestamp() {
        var tracker = createTracker(ACTIVE_BLUE);
        tick(1);
        tracker.onUpdate(ACTIVE_BLUE, now());
        tick(1);
        tracker.onUpdate(ACTIVE_BLUE, now());
        tick(1);
        tracker.onUpdate(ACTIVE_BLUE, now());

        assertNoTransitionRecorded(TRANSITION_BLUE_TO_GREEN);
        assertNoTransitionRecorded(TRANSITION_GREEN_TO_BLUE);
        assertNoStateTimeRecorded(ACTIVE_BLUE);
    }

    @Test
    void intermediateStateMetrics_onlyRecordedAtStableState() {
        var tracker = createTracker(ACTIVE_BLUE);
        tick(5);
        tracker.onUpdate(SAVEPOINTING_BLUE, now());
        tick(5);
        tracker.onUpdate(TRANSITIONING_TO_GREEN, now());

        // Metrics should not be recorded yet
        assertNoStateTimeRecorded(ACTIVE_BLUE);
        assertNoStateTimeRecorded(SAVEPOINTING_BLUE);

        // Now reach stable state
        tick(5);
        tracker.onUpdate(ACTIVE_GREEN, now());

        // Now all should be recorded
        assertStateTimeRecorded(ACTIVE_BLUE, 5);
        assertStateTimeRecorded(SAVEPOINTING_BLUE, 5);
        assertStateTimeRecorded(TRANSITIONING_TO_GREEN, 5);
    }

    @Test
    void recoveryFromActiveState_tracksNextTransition() {
        // Simulates operator restart with resource already in ACTIVE_BLUE
        var tracker = createTracker(ACTIVE_BLUE);
        tick(10);
        tracker.onUpdate(TRANSITIONING_TO_GREEN, now());
        tick(5);

        tracker.onUpdate(ACTIVE_GREEN, now());

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 15);
    }

    @Test
    void consecutiveTransitions_eachTrackedIndependently() {
        // Full cycle: blue -> green -> blue
        var tracker = createTracker(ACTIVE_BLUE);

        // Blue to Green
        tick(10);
        tracker.onUpdate(TRANSITIONING_TO_GREEN, now());
        tick(5);
        tracker.onUpdate(ACTIVE_GREEN, now());

        assertTransitionRecorded(TRANSITION_BLUE_TO_GREEN, 15);

        // Green to Blue
        tick(8);
        tracker.onUpdate(TRANSITIONING_TO_BLUE, now());
        tick(2);
        tracker.onUpdate(ACTIVE_BLUE, now());

        assertTransitionRecorded(TRANSITION_GREEN_TO_BLUE, 10);
    }

    // ==================== Helpers ====================

    private BlueGreenResourceLifecycleMetricTracker createTracker(
            FlinkBlueGreenDeploymentState initialState) {
        return new BlueGreenResourceLifecycleMetricTracker(
                initialState, now(), transitionHistos, stateTimeHistos);
    }

    private Instant now() {
        return Instant.ofEpochSecond(currentTimeSeconds);
    }

    private void tick(long seconds) {
        currentTimeSeconds += seconds;
    }

    private Histogram createHistogram() {
        return OperatorMetricUtils.createHistogram(
                FlinkOperatorConfiguration.fromConfiguration(new Configuration()));
    }

    private void assertTransitionRecorded(String transitionName, long expectedSeconds) {
        var stats = transitionHistos.get(transitionName).get(0).getStatistics();
        assertEquals(1, stats.size(), transitionName + " should have 1 sample");
        assertEquals(expectedSeconds, (long) stats.getMean(), transitionName + " duration");
    }

    private void assertNoTransitionRecorded(String transitionName) {
        var stats = transitionHistos.get(transitionName).get(0).getStatistics();
        assertEquals(0, stats.size(), transitionName + " should have no samples");
    }

    private void assertStateTimeRecorded(
            FlinkBlueGreenDeploymentState state, long expectedSeconds) {
        var stats = stateTimeHistos.get(state).get(0).getStatistics();
        assertEquals(1, stats.size(), state + " should have 1 sample");
        assertEquals(expectedSeconds, (long) stats.getMean(), state + " duration");
    }

    private void assertNoStateTimeRecorded(FlinkBlueGreenDeploymentState state) {
        var stats = stateTimeHistos.get(state).get(0).getStatistics();
        assertEquals(0, stats.size(), state + " should have no samples");
    }
}
