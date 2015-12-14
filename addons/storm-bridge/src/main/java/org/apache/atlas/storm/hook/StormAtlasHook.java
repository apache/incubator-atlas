/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.storm.hook;

import backtype.storm.ISubmitterHook;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.storm.model.StormDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * StormAtlasHook sends storm topology metadata information to Atlas
 * via a Kafka Broker for durability.
 * <p/>
 * This is based on the assumption that the same topology name is used
 * for the various lifecycle stages.
 */
public class StormAtlasHook extends AtlasHook implements ISubmitterHook {

    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(StormAtlasHook.class);

    private static final String CONF_PREFIX = "atlas.hook.storm.";
    private static final String HOOK_NUM_RETRIES = CONF_PREFIX + "numRetries";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return HOOK_NUM_RETRIES;
    }

    /**
     * This is the client-side hook that storm fires when a topology is added.
     *
     * @param topologyInfo topology info
     * @param stormConf configuration
     * @param stormTopology a storm topology
     * @throws IllegalAccessException
     */
    @Override
    public void notify(TopologyInfo topologyInfo, Map stormConf,
                       StormTopology stormTopology) throws IllegalAccessException {
        LOG.info("Collecting metadata for a new storm topology: {}", topologyInfo.get_name());
        try {
            Referenceable topologyReferenceable = createTopologyInstance(topologyInfo);
            addTopologyDataSets(stormTopology, topologyReferenceable,
                    topologyInfo.get_owner(), stormConf);

            // create the graph for the topology
            ArrayList<Referenceable> graphNodes = createTopologyGraph(
                    stormTopology, stormTopology.get_spouts(), stormTopology.get_bolts());
            // add the connection from topology to the graph
            topologyReferenceable.set("nodes", graphNodes);

            ArrayList<Referenceable> entities = new ArrayList<>();
            entities.add(topologyReferenceable);

            LOG.debug("notifying entities, size = {}", entities.size());
            notifyEntity(entities);
        } catch (Exception e) {
            throw new RuntimeException("Atlas hook is unable to process the topology.", e);
        }
    }

    private Referenceable createTopologyInstance(TopologyInfo topologyInfo) throws Exception {
        Referenceable topologyReferenceable = new Referenceable(
                StormDataTypes.STORM_TOPOLOGY.getName());
        topologyReferenceable.set("id", topologyInfo.get_id());
        topologyReferenceable.set("name", topologyInfo.get_name());
        topologyReferenceable.set("owner", topologyInfo.get_owner());
        topologyReferenceable.set("startTime", System.currentTimeMillis());
        topologyReferenceable.set(CLUSTER_NAME, getClusterName());

        return topologyReferenceable;
    }

    private void addTopologyDataSets(StormTopology stormTopology,
                                     Referenceable topologyReferenceable,
                                     String topologyOwner,
                                     Map stormConf) throws Exception {
        // add each spout as an input data set
        addTopologyInputs(topologyReferenceable,
                stormTopology.get_spouts(), stormConf, topologyOwner);
        // add the appropriate bolts as output data sets
        addTopologyOutputs(topologyReferenceable, stormTopology, topologyOwner, stormConf);
    }

    private void addTopologyInputs(Referenceable topologyReferenceable,
                                   Map<String, SpoutSpec> spouts,
                                   Map stormConf,
                                   String topologyOwner) throws IllegalAccessException {
        final ArrayList<Referenceable> inputDataSets = new ArrayList<>();
        for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            Serializable instance = Utils.javaDeserialize(
                    entry.getValue().get_spout_object().get_serialized_java(), Serializable.class);

            inputDataSets.add(createDataSet(entry.getKey(), topologyOwner, instance, stormConf));
        }

        topologyReferenceable.set("inputs", inputDataSets);
    }

    private void addTopologyOutputs(Referenceable topologyReferenceable,
                                    StormTopology stormTopology, String topologyOwner,
                                    Map stormConf) throws Exception {
        final ArrayList<Referenceable> outputDataSets = new ArrayList<>();

        Map<String, Bolt> bolts = stormTopology.get_bolts();
        Set<String> terminalBoltNames = StormTopologyUtil.getTerminalUserBoltNames(stormTopology);
        for (String terminalBoltName : terminalBoltNames) {
            Serializable instance = Utils.javaDeserialize(bolts.get(terminalBoltName)
                    .get_bolt_object().get_serialized_java(), Serializable.class);

            outputDataSets.add(createDataSet(terminalBoltName, topologyOwner, instance, stormConf));
        }

        topologyReferenceable.set("outputs", outputDataSets);
    }

    private Referenceable createDataSet(String name, String topologyOwner,
                                        Serializable instance,
                                        Map stormConf) throws IllegalAccessException {
        Map<String, String> config = StormTopologyUtil.getFieldValues(instance, true);

        Referenceable dataSetReferenceable;
        // todo: need to redo this with a config driven approach
        switch (name) {
            case "KafkaSpout":
                dataSetReferenceable = new Referenceable(StormDataTypes.KAFKA_TOPIC.getName());
                dataSetReferenceable.set("topic", config.get("KafkaSpout._spoutConfig.topic"));
                dataSetReferenceable.set("uri",
                        config.get("KafkaSpout._spoutConfig.hosts.brokerZkStr"));
                dataSetReferenceable.set("owner", topologyOwner);

                break;

            case "HBaseBolt":
                dataSetReferenceable = new Referenceable(StormDataTypes.HBASE_TABLE.getName());
                dataSetReferenceable.set("uri", stormConf.get("hbase.rootdir"));
                dataSetReferenceable.set("tableName", config.get("HBaseBolt.tableName"));
                dataSetReferenceable.set("owner", "storm.kerberos.principal");
                break;

            case "HdfsBolt":
                dataSetReferenceable = new Referenceable(StormDataTypes.HDFS_DATA_SET.getName());
                String hdfsUri = config.get("HdfsBolt.rotationActions") == null
                        ? config.get("HdfsBolt.fileNameFormat.path")
                        : config.get("HdfsBolt.rotationActions");
                dataSetReferenceable.set("pathURI", config.get("HdfsBolt.fsUrl") + hdfsUri);
                dataSetReferenceable.set("owner", stormConf.get("hdfs.kerberos.principal"));
                break;

            case "HiveBolt":
                // todo: verify if hive table has everything needed to retrieve existing table
                Referenceable dbReferenceable = new Referenceable("hive_db");
                String databaseName = config.get("HiveBolt.options.databaseName");
                dbReferenceable.set("name", getEntityQualifiedName(getClusterName(), databaseName));

                dataSetReferenceable = new Referenceable("hive_table");
                dataSetReferenceable.set("uri", config.get("HiveBolt.options.metaStoreURI"));
                dataSetReferenceable.set("db", dbReferenceable);
                dataSetReferenceable.set("tableName", getEntityQualifiedName(
                        getClusterName(), databaseName, config.get("HiveBolt.options.tableName")));
                dataSetReferenceable.set(CLUSTER_NAME, getClusterName());
                break;

            default:
                // custom node - create a base dataset class with name attribute
                dataSetReferenceable = new Referenceable(AtlasClient.DATA_SET_SUPER_TYPE);
        }

        // name is common across all types
        dataSetReferenceable.set("name", name);

        return dataSetReferenceable;
    }

    private ArrayList<Referenceable> createTopologyGraph(StormTopology stormTopology,
                                                         Map<String, SpoutSpec> spouts,
                                                         Map<String, Bolt> bolts) throws Exception {
        // Add graph of nodes in the topology
        final Map<String, Referenceable> nodeEntities = new HashMap<>();
        addSpouts(spouts, nodeEntities);
        addBolts(bolts, nodeEntities);

        addGraphConnections(stormTopology, nodeEntities);

        ArrayList<Referenceable> nodes = new ArrayList<>();
        nodes.addAll(nodeEntities.values());
        return nodes;
    }

    private void addSpouts(Map<String, SpoutSpec> spouts,
                           Map<String, Referenceable> nodeEntities) throws IllegalAccessException {
        for (Map.Entry<String, SpoutSpec> entry : spouts.entrySet()) {
            final String spoutName = entry.getKey();
            Referenceable spoutReferenceable = createSpoutInstance(
                    spoutName, entry.getValue());
            nodeEntities.put(spoutName, spoutReferenceable);
        }
    }

    private Referenceable createSpoutInstance(String spoutName,
                                              SpoutSpec stormSpout) throws IllegalAccessException {
        Referenceable spoutReferenceable = new Referenceable(
                StormDataTypes.STORM_SPOUT.getName(), "DataProducer");
        spoutReferenceable.set("name", spoutName);

        Serializable instance = Utils.javaDeserialize(
                stormSpout.get_spout_object().get_serialized_java(), Serializable.class);
        spoutReferenceable.set("driverClass", instance.getClass().getName());

        Map<String, String> flatConfigMap = StormTopologyUtil.getFieldValues(instance, true);
        spoutReferenceable.set("conf", flatConfigMap);

        return spoutReferenceable;
    }

    private void addBolts(Map<String, Bolt> bolts,
                          Map<String, Referenceable> nodeEntities) throws IllegalAccessException {
        for (Map.Entry<String, Bolt> entry : bolts.entrySet()) {
            Referenceable boltInstance = createBoltInstance(entry.getKey(), entry.getValue());
            nodeEntities.put(entry.getKey(), boltInstance);
        }
    }

    private Referenceable createBoltInstance(String boltName,
                                             Bolt stormBolt) throws IllegalAccessException {
        Referenceable boltReferenceable = new Referenceable(
                StormDataTypes.STORM_BOLT.getName(), "DataProcessor");

        boltReferenceable.set("name", boltName);

        Serializable instance = Utils.javaDeserialize(
                stormBolt.get_bolt_object().get_serialized_java(), Serializable.class);
        boltReferenceable.set("driverClass", instance.getClass().getName());

        Map<String, String> flatConfigMap = StormTopologyUtil.getFieldValues(instance, true);
        boltReferenceable.set("conf", flatConfigMap);

        return boltReferenceable;
    }

    private void addGraphConnections(StormTopology stormTopology,
                                     Map<String, Referenceable> nodeEntities) throws Exception {
        // adds connections between spouts and bolts
        Map<String, Set<String>> adjacencyMap =
                StormTopologyUtil.getAdjacencyMap(stormTopology, true);

        for (Map.Entry<String, Set<String>> entry : adjacencyMap.entrySet()) {
            String nodeName = entry.getKey();
            Set<String> adjacencyList = adjacencyMap.get(nodeName);
            if (adjacencyList == null || adjacencyList.isEmpty()) {
                continue;
            }

            // add outgoing links
            Referenceable node = nodeEntities.get(nodeName);
            ArrayList<String> outputs = new ArrayList<>(adjacencyList.size());
            outputs.addAll(adjacencyList);
            node.set("outputs", outputs);

            // add incoming links
            for (String adjacentNodeName : adjacencyList) {
                Referenceable adjacentNode = nodeEntities.get(adjacentNodeName);
                @SuppressWarnings("unchecked")
                ArrayList<String> inputs = (ArrayList<String>) adjacentNode.get("inputs");
                if (inputs == null) {
                    inputs = new ArrayList<>();
                }
                inputs.add(nodeName);
                adjacentNode.set("inputs", inputs);
            }
        }
    }
}
