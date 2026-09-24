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

package org.apache.flink.kubernetes.operator.api.spec;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.diff.Diffable;
import org.apache.flink.kubernetes.operator.api.diff.SpecDiff;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** One-shot redeployment of a Flink job, discarding its current state. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class RedeploySpec implements Diffable<RedeploySpec> {

    /**
     * Nonce used to trigger a redeployment of the job. In order to trigger a redeployment, change
     * the number to a different non-null value. Has no effect on the first deployment.
     */
    @SpecDiff(value = DiffType.SAVEPOINT_REDEPLOY, onNullIgnore = true)
    private Long nonce;

    /**
     * Savepoint or retained checkpoint path to restore the redeployed job from. If not set, the job
     * is redeployed without state. Only read when the nonce changes.
     */
    @SpecDiff(DiffType.IGNORE)
    private String savepointPath;
}
