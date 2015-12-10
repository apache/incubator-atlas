/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.storm.hook;

import backtype.storm.ILocalCluster;
import backtype.storm.generated.StormTopology;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.storm.model.StormDataModel;
import org.apache.atlas.storm.model.StormDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit tests for storm atlas hook. Constructs a sample topology and
 * maps it to atlas types.
 */
@Test
public class StormAtlasHookIT {

    public static final Logger LOG = LoggerFactory.getLogger(StormAtlasHookIT.class);

    private static final String ATLAS_URL = "http://localhost:21000/";
    private static final String TOPOLOGY_NAME = "word-count";

    private ILocalCluster stormCluster;
    private AtlasClient atlasClient;

    @BeforeClass
    public void setUp() throws Exception {
        // start a local storm cluster
        stormCluster = StormTestUtil.createLocalStormCluster();
        LOG.info("Created a storm local cluster");

        atlasClient = new AtlasClient(ATLAS_URL);
    }

    @AfterClass
    public void tearDown() throws Exception {
        LOG.info("Shutting down storm local cluster");
        stormCluster.shutdown();

        atlasClient = null;
    }

    @Test
    public void testCreateDataModel() throws Exception {
        StormDataModel.main(new String[]{});
        TypesDef stormTypesDef = StormDataModel.typesDef();

        String stormTypesAsJSON = TypesSerialization.toJson(stormTypesDef);
        LOG.info("stormTypesAsJSON = {}", stormTypesAsJSON);

        atlasClient.createType(stormTypesAsJSON);

        // verify types are registered
        for (StormDataTypes stormDataType : StormDataTypes.values()) {
            Assert.assertNotNull(atlasClient.getType(stormDataType.getName()));
        }
    }

    @Test (dependsOnMethods = "testCreateDataModel")
    public void testAddEntities() throws Exception {
        StormTopology stormTopology = StormTestUtil.createTestTopology();
        StormTestUtil.submitTopology(stormCluster, TOPOLOGY_NAME, stormTopology);
        LOG.info("Submitted topology {}", TOPOLOGY_NAME);

        // todo: test if topology metadata is registered in atlas
        String guid = getTopologyGUID();
        Assert.assertNotNull(guid);
        LOG.info("GUID is {}", guid);

        Referenceable topologyReferenceable = atlasClient.getEntity(guid);
        Assert.assertNotNull(topologyReferenceable);
    }

    private String getTopologyGUID() throws Exception {
        LOG.debug("Searching for topology {}", TOPOLOGY_NAME);
        String query = String.format("from %s where name = \"%s\"",
                StormDataTypes.STORM_TOPOLOGY.getName(), TOPOLOGY_NAME);

        JSONArray results = atlasClient.search(query);
        JSONObject row = results.getJSONObject(0);

        return row.has("__guid") ? row.getString("__guid") : null;
    }
}
