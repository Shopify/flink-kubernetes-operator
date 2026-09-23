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
import org.apache.flink.kubernetes.operator.api.spec.RedeploySpec;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;

import lombok.Value;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * A resolved request to redeploy a job, discarding its current state. This is the only place that
 * interprets the redeploy triggers of the {@link JobSpec}: {@code redeploy.nonce} and the
 * deprecated {@code savepointRedeployNonce}.
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
     * Resolves the redeploy requested by a spec change, if any. If both triggers changed, {@code
     * redeploy.nonce} takes precedence (such changes are rejected by validation).
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

    public static boolean isRedeployNonceChanged(JobSpec reconciled, JobSpec desired) {
        return nonceChanged(nonce(reconciled.getRedeploy()), nonce(desired.getRedeploy()));
    }

    @SuppressWarnings("deprecation")
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
