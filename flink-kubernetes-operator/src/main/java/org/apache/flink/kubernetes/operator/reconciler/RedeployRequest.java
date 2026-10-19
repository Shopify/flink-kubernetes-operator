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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.RedeploySpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;
import org.apache.flink.util.StringUtils;

import lombok.Value;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * A resolved request to redeploy a job, discarding its current state. The spec diff detects that
 * one of the redeploy triggers of the {@link JobSpec} changed ({@code redeploy.nonce} or the
 * deprecated {@code savepointRedeployNonce}), this class resolves which one and what state the job
 * has to be restored from. It is shared by the reconcilers, the validator and Blue/Green
 * deployments.
 */
@Value
public class RedeployRequest {

    /** Savepoint or checkpoint to restore from, null to redeploy without state. */
    @Nullable String savepointPath;

    /** True if triggered through the deprecated savepointRedeployNonce. */
    boolean legacy;

    /** The upgrade mode the redeployed job is restored with. */
    public UpgradeMode getRestoreMode() {
        return savepointPath == null ? UpgradeMode.STATELESS : UpgradeMode.SAVEPOINT;
    }

    /**
     * Resolves the redeploy requested by a spec change, if any. Changing both triggers in the same
     * update is rejected by the validator, {@code redeploy.nonce} takes precedence defensively.
     *
     * @param reconciled Job spec of the last reconciled spec.
     * @param desired Job spec of the current spec.
     * @return The requested redeploy, or empty if the change does not request one.
     */
    public static Optional<RedeployRequest> resolve(
            @Nullable JobSpec reconciled, @Nullable JobSpec desired) {
        if (reconciled == null || desired == null) {
            return Optional.empty();
        }
        if (isRedeployNonceChanged(reconciled, desired)) {
            return Optional.of(
                    new RedeployRequest(desired.getRedeploy().getSavepointPath(), false));
        }
        if (isLegacyNonceChanged(reconciled, desired)) {
            return Optional.of(new RedeployRequest(desired.getInitialSavepointPath(), true));
        }
        return Optional.empty();
    }

    /**
     * Validates the redeploy triggers of a job spec change.
     *
     * @param oldJob Last reconciled job spec.
     * @param newJob New job spec.
     * @return Optional error string.
     */
    public static Optional<String> validate(@Nullable JobSpec oldJob, @Nullable JobSpec newJob) {
        if (oldJob == null || newJob == null) {
            return Optional.empty();
        }

        boolean redeploy = isRedeployNonceChanged(oldJob, newJob);
        boolean legacyRedeploy = isLegacyNonceChanged(oldJob, newJob);

        if (redeploy && legacyRedeploy) {
            return Optional.of(
                    "Cannot change redeploy.nonce and the deprecated savepointRedeployNonce at the same time");
        }

        if (redeploy) {
            var savepointPath = newJob.getRedeploy().getSavepointPath();
            if (savepointPath != null && savepointPath.isBlank()) {
                return Optional.of(
                        "redeploy.savepointPath must not be blank, leave it unset to redeploy without state");
            }
        }

        if (legacyRedeploy
                && StringUtils.isNullOrWhitespaceOnly(newJob.getInitialSavepointPath())) {
            return Optional.of("InitialSavepointPath must not be empty for savepoint redeployment");
        }

        return Optional.empty();
    }

    /** Preserves consumed triggers when an ordinary update clears them. */
    public static void preserveConsumedNonces(@Nullable JobSpec reconciled, JobSpec desired) {
        if (reconciled == null || desired == null) {
            return;
        }
        if (nonce(desired.getRedeploy()) == null) {
            desired.setRedeploy(SpecUtils.clone(reconciled.getRedeploy()));
        }
        if (desired.getSavepointRedeployNonce() == null) {
            desired.setSavepointRedeployNonce(reconciled.getSavepointRedeployNonce());
        }
    }

    /** Whether recorded operation intent still needs to be confirmed after submission. */
    public static boolean isPendingRedeploy(CommonStatus<?> status) {
        var reconciliation = status.getReconciliationStatus();
        var recorded = reconciliation.deserializeLastReconciledSpecWithMeta();
        if (recorded == null || recorded.getMeta() == null) {
            return false;
        }
        var job = recorded.getSpec().getJob();
        if (job == null
                || (nonce(job.getRedeploy()) == null && job.getSavepointRedeployNonce() == null)) {
            return false;
        }
        var path = status.getJobStatus().getUpgradeSavepointPath();
        var block = job.getRedeploy();
        boolean targetMatches =
                block != null
                                && block.getNonce() != null
                                && job.getUpgradeMode()
                                        == new RedeployRequest(block.getSavepointPath(), false)
                                                .getRestoreMode()
                                && Objects.equals(block.getSavepointPath(), path)
                        || job.getSavepointRedeployNonce() != null
                                && job.getUpgradeMode() == UpgradeMode.SAVEPOINT
                                && job.getInitialSavepointPath() != null
                                && Objects.equals(job.getInitialSavepointPath(), path);
        var stable = reconciliation.deserializeLastStableSpec();
        if (!targetMatches) {
            return false;
        }
        if (stable == null) {
            // An explicit restore must survive failures even before the first stable job.
            // Do not treat a retained empty-state block as proof that an ordinary fix is stable.
            return path != null && !recorded.getMeta().isFirstDeployment();
        }
        return stable.getJob() != null
                && (stable.getJob().getState() == JobState.SUSPENDED
                        || isRedeployNonceChanged(stable.getJob(), job)
                        || isLegacyNonceChanged(stable.getJob(), job));
    }

    public static boolean isRedeployNonceChanged(JobSpec reconciled, JobSpec desired) {
        return nonceChanged(nonce(reconciled.getRedeploy()), nonce(desired.getRedeploy()));
    }

    public static boolean isLegacyNonceChanged(JobSpec reconciled, JobSpec desired) {
        return nonceChanged(
                reconciled.getSavepointRedeployNonce(), desired.getSavepointRedeployNonce());
    }

    /** Same semantics as {@code @SpecDiff(onNullIgnore = true)}: clearing the nonce is a no-op. */
    private static boolean nonceChanged(@Nullable Long reconciled, @Nullable Long desired) {
        return desired != null && !desired.equals(reconciled);
    }

    @Nullable
    private static Long nonce(@Nullable RedeploySpec redeploy) {
        return redeploy == null ? null : redeploy.getNonce();
    }
}
