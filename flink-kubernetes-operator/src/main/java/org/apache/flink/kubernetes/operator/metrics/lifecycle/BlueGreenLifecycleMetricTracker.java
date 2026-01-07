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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.metrics.Histogram;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;

/**
 * Tracks state transitions and timing for a single FlinkBlueGreenDeployment resource. Records
 * metrics for: - Transition times (e.g., BlueToGreen, InitialDeployment) - Time spent in each state
 */
public class BlueGreenLifecycleMetricTracker {

    // Transition metric names
    public static final String TRANSITION_INITIAL_DEPLOYMENT = "InitialDeployment";
    public static final String TRANSITION_BLUE_TO_GREEN = "BlueToGreen";
    public static final String TRANSITION_GREEN_TO_BLUE = "GreenToBlue";

    // Current state of this resource
    private FlinkBlueGreenDeploymentState currentState;

    // map(state -> (firstEntryTime, lastUpdateTime))
    // Tracks when we entered each state and when we last saw it
    private final Map<FlinkBlueGreenDeploymentState, Tuple2<Instant, Instant>> stateTimeMap =
            new HashMap<>();

    // Histograms for recording transition times (e.g., "BlueToGreen" -> histogram)
    private final Map<String, List<Histogram>> transitionHistos;

    // Histograms for recording time spent in each state
    private final Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos;

    public BlueGreenLifecycleMetricTracker(
            FlinkBlueGreenDeploymentState initialState,
            Instant time,
            Map<String, List<Histogram>> transitionHistos,
            Map<FlinkBlueGreenDeploymentState, List<Histogram>> stateTimeHistos) {
        this.currentState = initialState;
        this.transitionHistos = transitionHistos;
        this.stateTimeHistos = stateTimeHistos;
        // Record initial state entry time
        stateTimeMap.put(initialState, Tuple2.of(time, time));
    }

    public FlinkBlueGreenDeploymentState getCurrentState() {
        return currentState;
    }

    /**
     * Called on every reconciliation. Updates timestamps and records metrics on state changes.
     *
     * @param newState the current state from the resource status
     * @param time the current timestamp
     */
    public void onUpdate(FlinkBlueGreenDeploymentState newState, Instant time) {
        if (newState == currentState) {
            // Same state - just update the lastUpdate timestamp
            updateLastUpdateTime(newState, time);
            return;
        }

        // Update the end time for the state we're leaving
        updateLastUpdateTime(currentState, time);

        recordTransitionMetrics(currentState, newState, time);
        recordStateTimeMetrics(newState);
        clearTrackedStates(newState);

        stateTimeMap.put(newState, Tuple2.of(time, time));
        currentState = newState;
    }

    private void updateLastUpdateTime(FlinkBlueGreenDeploymentState state, Instant time) {
        var times = stateTimeMap.get(state);
        if (times != null) {
            times.f1 = time;
        }
    }

    /**
     * Records transition time metrics when moving to a new state. Checks if this transition matches
     * any tracked transitions and records duration.
     */
    private void recordTransitionMetrics(
            FlinkBlueGreenDeploymentState fromState,
            FlinkBlueGreenDeploymentState toState,
            Instant time) {

        // InitialDeployment: INITIALIZING_BLUE -> ACTIVE_BLUE
        if (fromState == INITIALIZING_BLUE && toState == ACTIVE_BLUE) {
            recordTransition(TRANSITION_INITIAL_DEPLOYMENT, INITIALIZING_BLUE, time);
        }

        // BlueToGreen: when reaching ACTIVE_GREEN from ACTIVE_BLUE path
        if (toState == ACTIVE_GREEN && stateTimeMap.containsKey(ACTIVE_BLUE)) {
            recordTransition(TRANSITION_BLUE_TO_GREEN, ACTIVE_BLUE, time);
        }

        // GreenToBlue: when reaching ACTIVE_BLUE from ACTIVE_GREEN path (not initial)
        if (toState == ACTIVE_BLUE
                && fromState != INITIALIZING_BLUE
                && stateTimeMap.containsKey(ACTIVE_GREEN)) {
            recordTransition(TRANSITION_GREEN_TO_BLUE, ACTIVE_GREEN, time);
        }
    }

    private void recordTransition(
            String transitionName, FlinkBlueGreenDeploymentState fromState, Instant time) {
        var fromTimes = stateTimeMap.get(fromState);
        if (fromTimes == null) {
            return;
        }

        // Measure from when we first entered the "from" state
        long durationSeconds = Duration.between(fromTimes.f0, time).toSeconds();

        var histograms = transitionHistos.get(transitionName);
        if (histograms != null) {
            histograms.forEach(h -> h.update(durationSeconds));
        }
    }

    /**
     * Records state duration metrics for all tracked states. Only processes when reaching a stable
     * state (ACTIVE_BLUE/ACTIVE_GREEN).
     */
    private void recordStateTimeMetrics(FlinkBlueGreenDeploymentState toState) {
        if (toState != ACTIVE_BLUE && toState != ACTIVE_GREEN) {
            return;
        }

        for (var state : stateTimeMap.keySet()) {
            var times = stateTimeMap.get(state);
            if (times != null) {
                long durationSeconds = Duration.between(times.f0, times.f1).toSeconds();
                var histograms = stateTimeHistos.get(state);
                if (histograms != null) {
                    histograms.forEach(h -> h.update(durationSeconds));
                }
            }
        }
    }

    /**
     * Clears all tracked states from the map. Only clears when reaching a stable state
     * (ACTIVE_BLUE/ACTIVE_GREEN).
     */
    private void clearTrackedStates(FlinkBlueGreenDeploymentState toState) {
        if (toState != ACTIVE_BLUE && toState != ACTIVE_GREEN) {
            return;
        }
        stateTimeMap.clear();
    }
}
