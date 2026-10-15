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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/**
 * Details of a single Flink application as returned by the {@code /applications/overview} REST
 * endpoint. Available since Flink 2.3. Mirrors {@code
 * org.apache.flink.runtime.messages.webmonitor.ApplicationDetails} which is not available in the
 * Flink version the operator is built against. Only the fields the operator reads are mapped,
 * unknown fields are ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomApplicationDetails {

    public static final String FIELD_NAME_APPLICATION_ID = "id";
    public static final String FIELD_NAME_START_TIME = "start-time";
    public static final String FIELD_NAME_STATUS = "status";

    private static final Set<String> TERMINAL_STATES = Set.of("FINISHED", "FAILED", "CANCELED");

    @JsonProperty(FIELD_NAME_APPLICATION_ID)
    private String applicationId;

    @JsonProperty(FIELD_NAME_START_TIME)
    private long startTime;

    @JsonProperty(FIELD_NAME_STATUS)
    private String status;

    public boolean isTerminal() {
        return status != null && TERMINAL_STATES.contains(status);
    }
}
