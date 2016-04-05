/**
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

package org.apache.atlas.falcon.model;

/**
 * Falcon Data Types for model and bridge.
 */
public enum FalconDataTypes {

    // Structs
    FALCON_WORKFLOW_PROPERTIES("falcon_wf_properties"),

    // Classes
    FALCON_CLUSTER_ENTITY("falcon_cluster"),
    FALCON_FEED_ENTITY("falcon_feed"),
    FALCON_FEED_DATASET("falcon_feed_datset"),
    FALCON_REPLICATION_FEED_ENTITY("falcon_replication_feed"),
    FALCON_PROCESS_ENTITY("falcon_process"),
    FALCON_USER("falcon_user"),
    FALCON_COLO("falcon_data_center"),
    FALCON_GROUP("falcon_groups"),
    FALCON_PIPELINE("falcon_pipelines"),
//    FALCON_RETENTION_FEED_ENTITY("falcon_replication_feed"),
    ;

    private final String name;

    FalconDataTypes(java.lang.String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
