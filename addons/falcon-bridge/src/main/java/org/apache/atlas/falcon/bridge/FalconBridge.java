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

package org.apache.atlas.falcon.bridge;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.falcon.model.FalconDataModelGenerator;
import org.apache.atlas.falcon.model.FalconDataTypes;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.atlas.Util.EventUtil;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.FileSystemStorage;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Bridge Utility to register Falcon entities metadata to Atlas.
 */
public class FalconBridge {
    private static final Logger LOG = LoggerFactory.getLogger(FalconBridge.class);
    private static final String DATASET_STR = "-dataset";
    private static final String REPLICATED_STR = "-replicated";

    /**
     * Creates cluster entity
     *
     * @param cluster ClusterEntity
     * @return cluster instance reference
     */
    public static List<Referenceable> createClusterEntity(final org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                                          final String user,
                                                          final long timestamp) throws Exception {
        LOG.info("Creating cluster Entity : {}", cluster.getName());

        List<Referenceable> entities = new ArrayList<>();

        Referenceable clusterRef = new Referenceable(FalconDataTypes.FALCON_CLUSTER_ENTITY.getName());
        clusterRef.set(FalconDataModelGenerator.NAME, cluster.getName());
        clusterRef.set(FalconDataModelGenerator.TIMESTAMP, timestamp);
        Referenceable coloReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                .FALCON_COLO.getName()), cluster.getColo());
        entities.add(coloReferenceable);
        clusterRef.set(FalconDataModelGenerator.COLO, coloReferenceable);

        // Set user
        Referenceable userReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                .FALCON_USER.getName()), user);
        entities.add(userReferenceable);
        clusterRef.set(FalconDataModelGenerator.USER, userReferenceable);


        if (StringUtils.isNotEmpty(cluster.getTags())) {
            clusterRef.set(FalconDataModelGenerator.TAGS,
                    EventUtil.convertKeyValueStringToMap(cluster.getTags()));
        }

        return entities;
    }

    private static List<Referenceable> createFeedDataset(Feed feed, org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                                         String user,
                                                         long timestamp) throws Exception {
        LOG.info("Creating feed dataset: {}", feed.getName());
        List<Referenceable> entities = new ArrayList<>();

        Referenceable datasetReferenceable = new Referenceable(FalconDataTypes.FALCON_FEED_DATASET.getName());
        datasetReferenceable.set(FalconDataModelGenerator.NAME, String.format("%s@%s", feed.getName() + DATASET_STR,
                cluster.getName()));
        datasetReferenceable.set(FalconDataModelGenerator.FEED_NAME, feed.getName() + DATASET_STR);
        datasetReferenceable.set(FalconDataModelGenerator.TIMESTAMP, timestamp);

        // set cluster
        Referenceable clusterReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                .FALCON_CLUSTER_ENTITY.getName()), cluster.getName());
        entities.add(clusterReferenceable);
        datasetReferenceable.set(FalconDataModelGenerator.STOREDIN, clusterReferenceable);

        // Set user
        Referenceable userReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                .FALCON_USER.getName()), user);
        entities.add(userReferenceable);
        datasetReferenceable.set(FalconDataModelGenerator.USER, userReferenceable);

        if (StringUtils.isNotEmpty(feed.getTags())) {
            datasetReferenceable.set(FalconDataModelGenerator.TAGS,
                    EventUtil.convertKeyValueStringToMap(feed.getTags()));
        }

        List<String> groups = EventUtil.convertStringToList(feed.getGroups());
        List<Referenceable> groupList = getGroupReferences(groups);
        if (groups != null) {
            entities.addAll(groupList);
        }

        if (StringUtils.isNotEmpty(feed.getTags())) {
            datasetReferenceable.set(FalconDataModelGenerator.TAGS,
                    EventUtil.convertKeyValueStringToMap(feed.getTags()));
        }

        entities.add(datasetReferenceable);
        return entities;
    }

    public static List<Referenceable> createFeedEntity(Feed feed,
                                                       ConfigurationStore falconStore, String user,
                                                       long timestamp) throws Exception {
        LOG.info("Creating feed : {}", feed.getName());

        List<Referenceable> entities = new ArrayList<>();

        if (feed.getClusters() != null) {
            for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
                org.apache.falcon.entity.v0.cluster.Cluster cluster = falconStore.get(EntityType.CLUSTER,
                        feedCluster.getName());

                // input as hive_table or hdfs_path, output as falcon_feed dataset
                List<Referenceable> inputs = new ArrayList<>();
                List<Referenceable> inputReferenceables = getInputEntities(cluster,
                        (Feed) falconStore.get(EntityType.FEED, feed.getName()));
                if (inputReferenceables != null) {
                    entities.addAll(inputReferenceables);
                    inputs.add(inputReferenceables.get(inputReferenceables.size() - 1));
                }

                List<Referenceable> outputs = new ArrayList<>();
                List<Referenceable> outputReferenceables = createFeedDataset(feed, cluster, user, timestamp);
                if (outputReferenceables != null) {
                    entities.addAll(outputReferenceables);
                    outputs.add(outputReferenceables.get(outputReferenceables.size() - 1));
                }

                if (!inputs.isEmpty() || !outputs.isEmpty()) {
                    Referenceable feedReferenceable = new Referenceable(FalconDataTypes.FALCON_FEED_DATASET.getName());
                    feedReferenceable.set(FalconDataModelGenerator.NAME, String.format("%s@%s", feed.getName(),
                            cluster.getName()));
                    feedReferenceable.set(FalconDataModelGenerator.FEED_NAME, feed.getName());

                    feedReferenceable.set(FalconDataModelGenerator.TIMESTAMP, timestamp);
                    if (!inputs.isEmpty()) {
                        feedReferenceable.set(FalconDataModelGenerator.INPUTS, inputs);
                    }
                    if (!outputs.isEmpty()) {
                        feedReferenceable.set(FalconDataModelGenerator.OUTPUTS, outputs);
                    }
                    // set cluster
                    Referenceable clusterReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                            .FALCON_CLUSTER_ENTITY.getName()), cluster.getName());
                    entities.add(clusterReferenceable);
                    feedReferenceable.set(FalconDataModelGenerator.STOREDIN, clusterReferenceable);

                    // Set user
                    Referenceable userReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                            .FALCON_USER.getName()), user);
                    entities.add(userReferenceable);
                    feedReferenceable.set(FalconDataModelGenerator.USER, userReferenceable);
                    entities.add(feedReferenceable);
                }
            }

        }
        return entities;
    }

    public static List<Referenceable> createReplicationFeedEntity(Feed feed,
                                                                  ConfigurationStore falconStore, String user,
                                                                  long timestamp) throws Exception {
        LOG.info("Creating replication feed : {}", feed.getName());

        List<Referenceable> entities = new ArrayList<>();
        // input as falcon_feed in source cluster, output as falcon_feed in target cluster
        List<Referenceable> inputs = new ArrayList<>();
        List<Referenceable> outputs = new ArrayList<>();
        if (feed.getClusters() != null) {
            for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
                org.apache.falcon.entity.v0.cluster.Cluster cluster = falconStore.get(EntityType.CLUSTER,
                        feedCluster.getName());

                if (ClusterType.SOURCE == feedCluster.getType()) {
                    Referenceable inputReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                            .FALCON_FEED_DATASET.getName()), getFeedDatasetName(feed.getName(), cluster.getName()));
                    entities.add(inputReferenceable);
                    inputs.add(inputReferenceable);
                }

                if (ClusterType.TARGET == feedCluster.getType()) {
                    Referenceable outputReferenceables = getEntityReference(new Referenceable(FalconDataTypes
                            .FALCON_FEED_DATASET.getName()), getFeedDatasetName(feed.getName(), cluster.getName()));
                    entities.add(outputReferenceables);
                    outputs.add(outputReferenceables);
                }
            }

            if (!inputs.isEmpty() || !outputs.isEmpty()) {
                Referenceable replicationFeedReferenceable = new Referenceable(FalconDataTypes
                        .FALCON_REPLICATION_FEED_ENTITY.getName());
                replicationFeedReferenceable.set(FalconDataModelGenerator.NAME, feed.getName() + REPLICATED_STR);
                replicationFeedReferenceable.set(FalconDataModelGenerator.FEED_NAME, feed.getName());

                replicationFeedReferenceable.set(FalconDataModelGenerator.TIMESTAMP, timestamp);
                if (!inputs.isEmpty()) {
                    replicationFeedReferenceable.set(FalconDataModelGenerator.INPUTS, inputs);
                }
                if (!outputs.isEmpty()) {
                    replicationFeedReferenceable.set(FalconDataModelGenerator.OUTPUTS, outputs);
                }

                // Set user
                Referenceable userReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                        .FALCON_USER.getName()), user);
                entities.add(userReferenceable);
                replicationFeedReferenceable.set(FalconDataModelGenerator.USER, userReferenceable);
                entities.add(replicationFeedReferenceable);
            }

        }
        return entities;
    }

    /**
     * +     * Creates process entity
     * +     *
     * +     * @param process process entity
     * +     * @param falconStore config store
     * +     * @param user falcon user
     * +     * @param timestamp timestamp of entity
     * +     * @return process instance reference
     * +
     */
    public static List<Referenceable> createProcessEntity(org.apache.falcon.entity.v0.process.Process process,
                                                          ConfigurationStore falconStore, String user, long timestamp) throws Exception {
        LOG.info("Creating process Entity : {}", process.getName());

        // The requirement is for each cluster, create a process entity with name
        // clustername.processname
        List<Referenceable> entities = new ArrayList<>();

        if (process.getClusters() != null) {

            for (Cluster processCluster : process.getClusters().getClusters()) {
                org.apache.falcon.entity.v0.cluster.Cluster cluster = falconStore.get(EntityType.CLUSTER, processCluster.getName());

                List<Referenceable> inputs = new ArrayList<>();
                if (process.getInputs() != null) {
                    for (Input input : process.getInputs().getInputs()) {
                        Referenceable inputReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                                .FALCON_FEED_DATASET
                                .getName()), getFeedDatasetName(input.getName(), cluster.getName()));
                        entities.add(inputReferenceable);
                        inputs.add(inputReferenceable);
                    }
                }

                List<Referenceable> outputs = new ArrayList<>();
                if (process.getOutputs() != null) {
                    for (Output output : process.getOutputs().getOutputs()) {
                        Referenceable outputReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                                .FALCON_FEED_DATASET.getName()), getFeedDatasetName(output.getName(), cluster.getName()));
                        entities.add(outputReferenceable);
                        outputs.add(outputReferenceable);
                    }
                }

                if (!inputs.isEmpty() || !outputs.isEmpty()) {
                    /* TODO: Check one process for each cluster */
                    Referenceable processReferenceable = new Referenceable(FalconDataTypes.FALCON_PROCESS_ENTITY.getName());
                    processReferenceable.set(FalconDataModelGenerator.NAME, String.format("%s@%s", process.getName(),
                            cluster.getName()));
                    processReferenceable.set(FalconDataModelGenerator.PROCESS_NAME, process.getName());

                    processReferenceable.set(FalconDataModelGenerator.TIMESTAMP, timestamp);
                    if (!inputs.isEmpty()) {
                        processReferenceable.set(FalconDataModelGenerator.INPUTS, inputs);
                    }
                    if (!outputs.isEmpty()) {
                        processReferenceable.set(FalconDataModelGenerator.OUTPUTS, outputs);
                    }

                    // set cluster
                    Referenceable clusterReferenceable = getEntityReference(new Referenceable(FalconDataTypes
                            .FALCON_CLUSTER_ENTITY.getName()), cluster.getName());
                    entities.add(clusterReferenceable);
                    processReferenceable.set(FalconDataModelGenerator.RUNSON, clusterReferenceable);

                    // Set user
                    Referenceable userReferenceable = createSimpleReferenceable(new Referenceable(FalconDataTypes
                            .FALCON_USER.getName()), user);
                    entities.add(userReferenceable);
                    processReferenceable.set(FalconDataModelGenerator.USER, userReferenceable);

                    if (StringUtils.isNotEmpty(process.getTags())) {
                        processReferenceable.set(FalconDataModelGenerator.TAGS,
                                EventUtil.convertKeyValueStringToMap(process.getTags()));
                    }

                    List<Referenceable> pipelines = getPipelineReferences(process.getPipelines());
                    if (pipelines != null) {
                        entities.addAll(pipelines);
                        processReferenceable.set(FalconDataModelGenerator.PIPELINES, pipelines);
                    }

                    Struct wfStruct = constructWFStruct(getProcessEntityWFProperties(process.getWorkflow(),
                            process.getName()));

                    if (wfStruct != null) {
                        processReferenceable.set(FalconDataModelGenerator.WFPROPERTIES, wfStruct);
                    }
                    entities.add(processReferenceable);
                }

            }
        }
        return entities;
    }

    private static List<Referenceable> getInputEntities(org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                                        Feed feed) throws Exception {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());

        final CatalogTable table = getTable(feedCluster, feed);
        if (table != null) {
            CatalogStorage storage = new CatalogStorage(cluster, table);
            return createHiveTableInstance(cluster.getName(), storage.getDatabase().toLowerCase(),
                    storage.getTable().toLowerCase());
        } else {
            List<Location> locations = FeedHelper.getLocations(feedCluster, feed);
            Location dataLocation = FileSystemStorage.getLocation(locations, LocationType.DATA);
            final String pathUri = normalize(dataLocation.getPath());
            LOG.info("Registering DFS Path {} ", pathUri);
            return fillHDFSDataSet(pathUri, cluster.getName());
        }
    }

    private static CatalogTable getTable(org.apache.falcon.entity.v0.feed.Cluster cluster, Feed feed) {
        // check if table is overridden in cluster
        if (cluster.getTable() != null) {
            return cluster.getTable();
        }

        return feed.getTable();
    }

    private static List<Referenceable> fillHDFSDataSet(final String pathUri, final String clusterName) {
        List<Referenceable> entities = new ArrayList<>();
        Referenceable ref = new Referenceable(FSDataTypes.HDFS_PATH().toString());
        ref.set("path", pathUri);
//        Path path = new Path(pathUri);
//        ref.set("name", path.getName());
        //TODO - Fix after ATLAS-542 to shorter Name
        ref.set(FalconDataModelGenerator.NAME, pathUri);
        ref.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, pathUri);
        ref.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        entities.add(ref);
        return entities;
    }

    private static Referenceable createHiveDatabaseInstance(String clusterName, String dbName)
            throws Exception {
        Referenceable dbRef = new Referenceable(HiveDataTypes.HIVE_DB.getName());
        dbRef.set(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName);
        dbRef.set(HiveDataModelGenerator.NAME, dbName);
        dbRef.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                HiveMetaStoreBridge.getDBQualifiedName(clusterName, dbName));
        return dbRef;
    }

    private static List<Referenceable> createHiveTableInstance(String clusterName, String dbName,
                                                               String tableName) throws Exception {
        List<Referenceable> entities = new ArrayList<>();
        Referenceable dbRef = createHiveDatabaseInstance(clusterName, dbName);
        entities.add(dbRef);

        Referenceable tableRef = new Referenceable(HiveDataTypes.HIVE_TABLE.getName());
        tableRef.set(HiveDataModelGenerator.NAME,
                HiveMetaStoreBridge.getTableQualifiedName(clusterName, dbName, tableName));
        tableRef.set(HiveDataModelGenerator.TABLE_NAME, tableName);
        tableRef.set(HiveDataModelGenerator.DB, dbRef);
        entities.add(tableRef);

        return entities;
    }

    public static Struct constructWFStruct(final Map<String, String> wfProperties) {
        if (wfProperties == null || wfProperties.size() <= 0) {
            return null;
        }
        String wfPropsName = FalconDataTypes.FALCON_WORKFLOW_PROPERTIES.getName();
        Struct wfStruct = new Struct(wfPropsName);
        wfStruct.set(FalconDataModelGenerator.USERWFENGINE,
                wfProperties.get(FalconDataModelGenerator.USERWFENGINE));
        wfStruct.set(FalconDataModelGenerator.USERWFNAME,
                wfProperties.get(FalconDataModelGenerator.USERWFNAME));
        wfStruct.set(FalconDataModelGenerator.USERWFVERSION,
                wfProperties.get(FalconDataModelGenerator.USERWFVERSION));
        wfStruct.set(FalconDataModelGenerator.WFID,
                wfProperties.get(FalconDataModelGenerator.WFID));
        wfStruct.set(FalconDataModelGenerator.WFRUNID,
                wfProperties.get(FalconDataModelGenerator.WFRUNID));
        wfStruct.set(FalconDataModelGenerator.WFSTATUS,
                wfProperties.get(FalconDataModelGenerator.WFSTATUS));
        wfStruct.set(FalconDataModelGenerator.WFENGINEURL,
                wfProperties.get(FalconDataModelGenerator.WFENGINEURL));
        wfStruct.set(FalconDataModelGenerator.WFSUBFLOWID,
                wfProperties.get(FalconDataModelGenerator.WFSUBFLOWID));

        return wfStruct;
    }

    /**
     * Creates simple Referenceable which has only name property
     *
     * @param ref  entity Referenceable
     * @param name Ref name
     * @return pipeline instance reference
     */
    public static Referenceable createSimpleReferenceable(final Referenceable ref, final String name) {
        ref.set(FalconDataModelGenerator.NAME, name);
        return ref;
    }

    /**
     * Gets reference to the atlas entity for the entity
     *
     * @param entityRef  entity Referenceable
     * @param entityName entity Name
     * @return Reference for cluster entity if exists, else null
     */
    public static Referenceable getEntityReference(final Referenceable entityRef, final String entityName) {
        /* This method should be called if reference is expected to exist beforehand */
        LOG.debug("Getting reference for entity {}", entityName);
        entityRef.set(FalconDataModelGenerator.NAME, entityName.toLowerCase());
        return entityRef;
    }

    private static List<Referenceable> getPipelineReferences(final String pipelines) {
        if (StringUtils.isNotBlank(pipelines)) {
            return null;
        }

        List<String> pipelineList = Arrays.asList(pipelines.split(","));
        List<Referenceable> referenceables = new ArrayList<>();
        for (String pipeline : pipelineList) {
            Referenceable reference = createSimpleReferenceable(new Referenceable(FalconDataTypes.FALCON_PIPELINE.getName()),
                    pipeline);
            referenceables.add(reference);
        }

        return referenceables;
    }

    private static List<Referenceable> getGroupReferences(final List<String> groups) {
        if (groups == null || groups.size() <= 0) {
            return null;
        }

        List<Referenceable> referenceables = new ArrayList<>();
        for (String group : groups) {
            Referenceable reference = createSimpleReferenceable(new Referenceable(FalconDataTypes.FALCON_GROUP.getName()),
                    group);
            referenceables.add(reference);
        }

        return referenceables;
    }

    private static Map<String, String> getProcessEntityWFProperties(final Workflow workflow,
                                                                    final String processName) {
        Map<String, String> wfProperties = new HashMap<>();
        wfProperties.put(WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(),
                ProcessHelper.getProcessWorkflowName(workflow.getName(), processName));
        wfProperties.put(WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(),
                workflow.getVersion());
        wfProperties.put(WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(),
                workflow.getEngine().value());

        return wfProperties;
    }

    private static String getFeedDatasetName(final String feedName, final String clusterName) {
        return String.format("%s@%s", feedName + DATASET_STR, clusterName);
    }

    private static String normalize(final String str) {
        if (StringUtils.isBlank(str)) {
            return null;
        }
        return str.toLowerCase().trim();
    }
}
