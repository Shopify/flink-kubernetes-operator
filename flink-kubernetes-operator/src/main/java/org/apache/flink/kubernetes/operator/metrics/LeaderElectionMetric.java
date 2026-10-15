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

import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

/** Tracks whether this operator instance currently holds leader election leadership. */
public class LeaderElectionMetric {

    private final AtomicInteger leadershipState = new AtomicInteger();

    public LeaderElectionMetric(KubernetesOperatorMetricGroup operatorMetricGroup) {
        operatorMetricGroup
                .addGroup("LeaderElection")
                .gauge("IsLeader", () -> leadershipState.get());
    }

    public void onStartLeading() {
        leadershipState.set(1);
    }

    public void onStopLeading() {
        leadershipState.set(0);
    }

    /** Adds metric updates to the configured callbacks while preserving all election settings. */
    public LeaderElectionConfiguration decorate(LeaderElectionConfiguration original) {
        var delegate = original.getLeaderCallbacks().orElse(null);
        var callbacks =
                new LeaderCallbacks(
                        () -> {
                            if (delegate != null) {
                                delegate.onStartLeading();
                            }
                            onStartLeading();
                        },
                        () -> {
                            onStopLeading();
                            if (delegate != null) {
                                delegate.onStopLeading();
                            }
                        },
                        identity -> {
                            if (delegate != null) {
                                delegate.onNewLeader(identity);
                            }
                        });

        return new LeaderElectionConfiguration(
                original.getLeaseName(),
                original.getLeaseNamespace().orElse(null),
                original.getLeaseDuration(),
                original.getRenewDeadline(),
                original.getRetryPeriod(),
                original.getIdentity().orElse(null),
                callbacks,
                original.isExitOnStopLeading());
    }
}
