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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.storm.model.StormDataModel;
import org.apache.atlas.storm.model.StormDataTypes;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

/**
 * Unit tests for storm atlas hook. Constructs a sample topology and
 * maps it to atlas types.
 */
@Guice(modules = NotificationModule.class)
@Test
public class StormAtlasHookTest {

    public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(StormAtlasHookTest.class);

    private static final String TOPOLOGY_NAME = "word-count-test";

    private final TypeSystem typeSystem = TypeSystem.getInstance();
    private ILocalCluster stormCluster;

    @Inject
    private KafkaNotification kafka;

    @BeforeClass
    public void setUp() throws Exception {
        typeSystem.reset();
        createSuperTypes();

        // start an embedded kafka
        kafka.start();

        // start a local storm cluster
        stormCluster = StormTestUtil.createLocalStormCluster();
        LOG.info("Created kafka, storm local cluster");
    }

    private static final AttributeDefinition NAME_ATTRIBUTE =
            TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE);
    private static final AttributeDefinition DESCRIPTION_ATTRIBUTE =
            TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE);

    private void createSuperTypes() throws AtlasException {
        System.out.println("creating super types for testing....");
        HierarchicalTypeDefinition<ClassType> infraType =
                TypesUtil.createClassTypeDef(AtlasClient.INFRASTRUCTURE_SUPER_TYPE,
                        ImmutableList.<String>of(), NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE);
        createType(infraType);

        HierarchicalTypeDefinition<ClassType> datasetType =
                TypesUtil.createClassTypeDef(AtlasClient.DATA_SET_SUPER_TYPE,
                        ImmutableList.<String>of(), NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE);
        createType(datasetType);

        HierarchicalTypeDefinition<ClassType> processType =
                TypesUtil.createClassTypeDef(AtlasClient.PROCESS_SUPER_TYPE,
                        ImmutableList.<String>of(), NAME_ATTRIBUTE, DESCRIPTION_ATTRIBUTE,
                        new AttributeDefinition("inputs", DataTypes
                                .arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("outputs",
                                DataTypes.arrayTypeName(AtlasClient.DATA_SET_SUPER_TYPE),
                                Multiplicity.OPTIONAL, false, null));
        createType(processType);

        HierarchicalTypeDefinition<ClassType> referenceableType = TypesUtil
                .createClassTypeDef(AtlasClient.REFERENCEABLE_SUPER_TYPE, ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                                DataTypes.STRING_TYPE));
        createType(referenceableType);
    }

    private void createType(HierarchicalTypeDefinition<ClassType> type) throws AtlasException {
        if (!typeSystem.isRegistered(type.typeName)) {
            TypesDef typesDef = TypesUtil.getTypesDef(
                    ImmutableList.<EnumTypeDefinition>of(),
                    ImmutableList.<StructTypeDefinition>of(),
                    ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                    ImmutableList.of(type));
            createType(TypesSerialization.toJson(typesDef));
        }
    }

    private void createType(String typeDefinition) throws AtlasException {
        TypesDef typesDef = TypesSerialization.fromJson(typeDefinition);
        if (typesDef.isEmpty()) {
            throw new IllegalArgumentException("Invalid type definition");
        }

        typeSystem.defineTypes(typesDef);
    }

    @AfterClass
    public void tearDown() throws Exception {
        LOG.info("Shutting down storm local cluster & kafka");
        stormCluster.shutdown();

        kafka.stop();

        typeSystem.reset();
    }

    @Test
    public void testCreateDataModel() throws Exception {
        StormDataModel.main(new String[]{});
        TypesDef stormTypesDef = StormDataModel.typesDef();

        String stormTypesAsJSON = TypesSerialization.toJson(stormTypesDef);
        System.out.println("stormTypesAsJSON = " + stormTypesAsJSON);

        // add storm types to type system
        typeSystem.defineTypes(stormTypesDef);

        // verify types are registered
        for (StormDataTypes stormDataType : StormDataTypes.values()) {
            Assert.assertTrue(typeSystem.isRegistered(stormDataType.getName()));
        }
    }

    @Test (dependsOnMethods = "testCreateDataModel")
    public void testAddEntities() throws Exception {
        StormTopology stormTopology = StormTestUtil.createTestTopology();
        StormTestUtil.submitTopology(stormCluster, TOPOLOGY_NAME, stormTopology);
        LOG.info("Submitted topology {}", TOPOLOGY_NAME);
    }
}
