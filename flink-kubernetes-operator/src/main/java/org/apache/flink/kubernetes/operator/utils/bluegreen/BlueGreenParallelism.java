/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.kubernetes.operator.utils.bluegreen;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.flink.configuration.PipelineOptions.PARALLELISM_OVERRIDES;

/** Preserves horizontal sizing without turning runtime decisions into parent configuration. */
final class BlueGreenParallelism {
    private static final Logger LOG = LoggerFactory.getLogger(BlueGreenParallelism.class);

    // Kept in the child spec so reconciliation status durably identifies consumed sizing intent.
    // Operator-prefixed configuration is removed before application submission.
    static final String INTENT_KEY = "kubernetes.operator.bluegreen.parallelism-inheritance";

    private BlueGreenParallelism() {}

    static void apply(
            BlueGreenContext context, @Nullable FlinkDeployment source, FlinkDeployment target) {
        var requested = target.getSpec();
        if (requested.getJob() == null) {
            return;
        }
        var targetConfig =
                context.getCtxFactory()
                        .getResourceContext(target, context.getJosdkContext())
                        .getDeployConfig(requested);
        if (!targetConfig.get(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            requested.getFlinkConfiguration().remove(INTENT_KEY);
            return;
        }
        var targetIntent =
                intent(requested, targetConfig, requested.getJob().getAutoscalerResetNonce());
        if (source != null && source.getMetadata().getDeletionTimestamp() == null) {
            inherit(context, source, target, targetConfig, targetIntent);
        }
        requested.getFlinkConfiguration().put(INTENT_KEY, targetIntent.encoded());
    }

    private static void inherit(
            BlueGreenContext context,
            FlinkDeployment source,
            FlinkDeployment target,
            Configuration targetConfig,
            Intent targetIntent) {
        var requested = target.getSpec();
        var sourceSpec = source.getSpec();
        if (sourceSpec.getJob() == null) {
            return;
        }
        var sourceReset = sourceSpec.getJob().getAutoscalerResetNonce();
        var sourceContext =
                context.getCtxFactory().getResourceContext(source, context.getJosdkContext());
        var seedConfig = sourceContext.getDeployConfig(sourceSpec);
        if (!seedConfig.get(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            return;
        }
        var recorded = sourceSpec.getFlinkConfiguration().asFlatMap().get(INTENT_KEY);
        Intent sourceIntent;
        if (recorded == null) {
            // Legacy children do not record which central defaults were used at creation.
            if (!sourceSpec
                    .getFlinkConfiguration()
                    .asConfiguration()
                    .get(PARALLELISM_OVERRIDES)
                    .equals(seedConfig.get(PARALLELISM_OVERRIDES))) {
                return;
            }
            sourceIntent = intent(sourceSpec, seedConfig, sourceReset);
        } else {
            sourceIntent = Intent.parse(recorded);
            if (sourceIntent == null) {
                LOG.warn(
                        "Ignoring incompatible blue-green sizing provenance on {}",
                        source.getMetadata().getName());
                return;
            }
        }
        var reset = requested.getJob().getAutoscalerResetNonce();
        var comparison =
                reset == null ? intent(requested, targetConfig, sourceReset) : targetIntent;
        if (!sourceIntent.equals(comparison)) {
            return;
        }

        var status = source.getStatus();
        var deployed =
                status == null || status.getReconciliationStatus() == null
                        ? null
                        : ReconciliationUtils.getDeployedSpec(source);
        var deployedIntent =
                deployed == null
                        ? null
                        : deployed.getFlinkConfiguration().asFlatMap().get(INTENT_KEY);
        var deployedJob = deployed == null ? null : deployed.getJob();
        boolean pendingReset =
                deployed != null
                        && (deployedJob == null
                                || (sourceReset != null
                                        && !Objects.equals(
                                                sourceReset,
                                                deployedJob.getAutoscalerResetNonce())));
        // Observation can acknowledge a generation while retaining an older deployed spec.
        var sourceConfig =
                deployed == null || !Objects.equals(recorded, deployedIntent) || pendingReset
                        ? seedConfig
                        : sourceContext.getObserveConfig();
        if (!sourceConfig.get(AutoScalerOptions.AUTOSCALER_ENABLED)) {
            return;
        }
        var overrides = sourceConfig.get(PARALLELISM_OVERRIDES);
        if (!validTargets(overrides)) {
            LOG.info(
                    "Retaining template sizing for {} because inherited targets are invalid",
                    target.getMetadata().getName());
        } else if (!overrides.isEmpty()) {
            String serialized =
                    ConfigurationUtils.convertValue(new TreeMap<>(overrides), String.class);
            requested.getFlinkConfiguration().put(PARALLELISM_OVERRIDES.key(), serialized);
        }
    }

    private static boolean validTargets(Map<String, String> overrides) {
        for (var value : overrides.values()) {
            try {
                if (Integer.parseInt(value) <= 0) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private static Intent intent(
            FlinkDeploymentSpec spec, Configuration resolved, @Nullable Long resetNonce) {
        var declared = spec.getFlinkConfiguration().asConfiguration();
        String seed =
                ConfigurationUtils.convertValue(
                        new TreeMap<>(declared.get(PARALLELISM_OVERRIDES)), String.class);
        var values = new TreeMap<String, String>();
        values.put("seed", seed);
        values.put(
                "resolved-seed",
                ConfigurationUtils.convertValue(
                        new TreeMap<>(resolved.get(PARALLELISM_OVERRIDES)), String.class));
        values.put("parallelism", Integer.toString(spec.getJob().getParallelism()));
        values.put("reset", Objects.toString(resetNonce));
        var replicas = spec.getTaskManager() == null ? null : spec.getTaskManager().getReplicas();
        values.put("replicas", Objects.toString(replicas));
        if (replicas != null) {
            values.put("slots", Integer.toString(resolved.get(TaskManagerOptions.NUM_TASK_SLOTS)));
        }
        values.put(
                CoreOptions.DEFAULT_PARALLELISM.key(),
                Objects.toString(declared.get(CoreOptions.DEFAULT_PARALLELISM)));
        for (var option :
                List.of(
                        PipelineOptions.MAX_PARALLELISM,
                        AutoScalerOptions.VERTEX_MIN_PARALLELISM,
                        AutoScalerOptions.VERTEX_MAX_PARALLELISM)) {
            values.put(option.key(), Objects.toString(resolved.get(option)));
        }
        values.put(
                AutoScalerOptions.VERTEX_EXCLUDE_IDS.key(),
                ConfigurationUtils.convertValue(
                        List.copyOf(
                                new TreeSet<>(resolved.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS))),
                        String.class));
        return new Intent(digest(ConfigurationUtils.convertValue(values, String.class)));
    }

    private static String digest(String value) {
        try {
            return HexFormat.of()
                    .formatHex(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private record Intent(String fingerprint) {
        @Nullable
        static Intent parse(String value) {
            if (!value.matches("v1:[0-9a-f]{64}")) {
                return null;
            }
            return new Intent(value.substring(3));
        }

        String encoded() {
            return "v1:" + fingerprint;
        }
    }
}
