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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.addons.ModelDefinitionDump;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility that generates falcon data model.
 */
public class FalconDataModelGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(FalconDataModelGenerator.class);

    private final Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions;
    private final Map<String, EnumTypeDefinition> enumTypeDefinitionMap;
    private final Map<String, StructTypeDefinition> structTypeDefinitionMap;

    public static final String NAME = "name";
    public static final String FEED_NAME = "feedName";
    public static final String PROCESS_NAME = "processName";
    public static final String TIMESTAMP = "timestamp";
    public static final String COLO = "collocated";
    public static final String USER = "owned-by";
    public static final String TAGS = "tag-classification";
    public static final String GROUPS = "grouped-as";
    public static final String PIPELINES = "pipeline";
    public static final String WFPROPERTIES = "wf-properties";
    public static final String RUNSON = "runs-on";
    public static final String STOREDIN = "stored-in";

    // multiple inputs and outputs for process
    public static final String INPUTS = "inputs";
    public static final String OUTPUTS = "outputs";

    // WF properties
    public static final String USERWFENGINE = "userWorkflowEngine";
    public static final String USERWFNAME = "userWorkflowName";
    public static final String USERWFVERSION = "userWorkflowVersion";
    public static final String WFID = "workflowId";
    public static final String WFRUNID = "runId";
    public static final String WFSTATUS = "status";
    public static final String WFENGINEURL = "workflowEngineUrl";
    public static final String WFSUBFLOWID = "subflowId";


    public FalconDataModelGenerator() {
        classTypeDefinitions = new HashMap<>();
        enumTypeDefinitionMap = new HashMap<>();
        structTypeDefinitionMap = new HashMap<>();
    }

    public void createDataModel() throws AtlasException {
        LOG.info("Generating the Falcon Data Model");

        // structs
        createWFPropertiesStruct();

        // classes
        createClusterEntityClass();
        createProcessEntityClass();
        createFeedEntityClass();
        createFeedDatasetClass();
        createReplicationFeedEntityClass();
        createUserClass();
        createColoClass();
        createGroupsClass();
        createPipelinesClass();
    }

    private TypesDef getTypesDef() {
        return TypesUtil.getTypesDef(getEnumTypeDefinitions(), getStructTypeDefinitions(), getTraitTypeDefinitions(),
                getClassTypeDefinitions());
    }

    public String getDataModelAsJSON() {
        return TypesSerialization.toJson(getTypesDef());
    }

    private ImmutableList<EnumTypeDefinition> getEnumTypeDefinitions() {
        return ImmutableList.copyOf(enumTypeDefinitionMap.values());
    }

    private ImmutableList<StructTypeDefinition> getStructTypeDefinitions() {
        return ImmutableList.copyOf(structTypeDefinitionMap.values());
    }

    private ImmutableList<HierarchicalTypeDefinition<ClassType>> getClassTypeDefinitions() {
        return ImmutableList.copyOf(classTypeDefinitions.values());
    }

    private ImmutableList<HierarchicalTypeDefinition<TraitType>> getTraitTypeDefinitions() {
        return ImmutableList.of();
    }

    private void createWFPropertiesStruct() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(USERWFENGINE, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(USERWFNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(USERWFVERSION, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                        null),
                new AttributeDefinition(WFID, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(WFRUNID, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(WFSTATUS, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(WFENGINEURL, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(WFSUBFLOWID, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),};

        StructTypeDefinition definition =
                new StructTypeDefinition(FalconDataTypes.FALCON_WORKFLOW_PROPERTIES.getName(), attributeDefinitions);
        structTypeDefinitionMap.put(FalconDataTypes.FALCON_WORKFLOW_PROPERTIES.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_WORKFLOW_PROPERTIES.getName());
    }


    private void createClusterEntityClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition(TIMESTAMP, DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(COLO, FalconDataTypes.FALCON_COLO.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(USER, FalconDataTypes.FALCON_USER.getName(), Multiplicity.REQUIRED, false,
                        null),
                // map of tags
                new AttributeDefinition(TAGS, DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, FalconDataTypes.FALCON_CLUSTER_ENTITY.getName(), null,
                        ImmutableSet.of(AtlasClient.INFRASTRUCTURE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(FalconDataTypes.FALCON_CLUSTER_ENTITY.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_CLUSTER_ENTITY.getName());
    }

    private void createFeedEntityClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(FEED_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition(TIMESTAMP, DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(STOREDIN, FalconDataTypes.FALCON_CLUSTER_ENTITY.getName(), Multiplicity.REQUIRED,
                        false, null),
                new AttributeDefinition(USER, FalconDataTypes.FALCON_USER.getName(), Multiplicity.REQUIRED, false,
                        null)};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, FalconDataTypes.FALCON_FEED_ENTITY.getName(), null,
                        ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(FalconDataTypes.FALCON_FEED_ENTITY.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_FEED_ENTITY.getName());
    }

    private void createFeedDatasetClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(FEED_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition(TIMESTAMP, DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(STOREDIN, FalconDataTypes.FALCON_CLUSTER_ENTITY.getName(), Multiplicity.REQUIRED,
                        false, null),
                new AttributeDefinition(USER, FalconDataTypes.FALCON_USER.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(GROUPS, DataTypes.arrayTypeName(FalconDataTypes.FALCON_GROUP.getName()),
                        Multiplicity.OPTIONAL, false, null),
                // map of tags
                new AttributeDefinition(TAGS, DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, FalconDataTypes.FALCON_FEED_DATASET.getName(), null,
                        ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(FalconDataTypes.FALCON_FEED_DATASET.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_FEED_DATASET.getName());
    }


    private void createReplicationFeedEntityClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(FEED_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition(TIMESTAMP, DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(USER, FalconDataTypes.FALCON_USER.getName(), Multiplicity.REQUIRED, false,
                        null)};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class,
                        FalconDataTypes.FALCON_REPLICATION_FEED_ENTITY.getName(), null,
                        ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(FalconDataTypes.FALCON_REPLICATION_FEED_ENTITY.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_REPLICATION_FEED_ENTITY.getName());
    }

    private void createProcessEntityClass() throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(PROCESS_NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),
                new AttributeDefinition(TIMESTAMP, DataTypes.LONG_TYPE.getName(), Multiplicity.REQUIRED, false,
                        null),
                new AttributeDefinition(RUNSON, FalconDataTypes.FALCON_CLUSTER_ENTITY.getName(), Multiplicity.REQUIRED,
                        false, null),
                new AttributeDefinition(USER, FalconDataTypes.FALCON_USER.getName(), Multiplicity.REQUIRED, false,
                        null),
                // map of tags
                new AttributeDefinition(TAGS, DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                        Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(PIPELINES, DataTypes.arrayTypeName(FalconDataTypes.FALCON_PIPELINE.getName()),
                        Multiplicity.OPTIONAL, false, null),
                // wf properties
                new AttributeDefinition(WFPROPERTIES, FalconDataTypes.FALCON_WORKFLOW_PROPERTIES.getName(),
                        Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, FalconDataTypes.FALCON_PROCESS_ENTITY.getName(), null,
                        ImmutableSet.of(AtlasClient.PROCESS_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(FalconDataTypes.FALCON_PROCESS_ENTITY.getName(), definition);
        LOG.debug("Created definition for {}", FalconDataTypes.FALCON_PROCESS_ENTITY.getName());
    }

    private void createUserClass() throws AtlasException {
        addGenericClass(FalconDataTypes.FALCON_USER.getName());
    }

    private void createColoClass() throws AtlasException {
        addGenericClass(FalconDataTypes.FALCON_COLO.getName());
    }

    private void createPipelinesClass() throws AtlasException {
        addGenericClass(FalconDataTypes.FALCON_PIPELINE.getName());
    }

    private void createGroupsClass() throws AtlasException {
        addGenericClass(FalconDataTypes.FALCON_GROUP.getName());
    }

    private void addGenericClass(String className) throws AtlasException {
        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
                new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, true,
                        null),};

        HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, className, null,
                        ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        classTypeDefinitions.put(className, definition);
        LOG.debug("Created definition for {}", className);
    }


    public String getModelAsJson() throws AtlasException {
        createDataModel();
        return getDataModelAsJSON();
    }

    public static void main(String[] args) throws Exception {
        FalconDataModelGenerator falconDataModelGenerator = new FalconDataModelGenerator();
        String modelAsJson = falconDataModelGenerator.getModelAsJson();

        if (args.length == 1) {
            ModelDefinitionDump.dumpModelToFile(args[0], modelAsJson);
            return;
        }

        System.out.println("falconDataModelAsJSON = " + modelAsJson);

        TypesDef typesDef = falconDataModelGenerator.getTypesDef();
        for (EnumTypeDefinition enumType : typesDef.enumTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - values %s", enumType.name, EnumType.class.getSimpleName(),
                    Arrays.toString(enumType.enumValues)));
        }
        for (StructTypeDefinition structType : typesDef.structTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - attributes %s", structType.typeName, StructType.class.getSimpleName(),
                    Arrays.toString(structType.attributeDefinitions)));
        }
        for (HierarchicalTypeDefinition<ClassType> classType : typesDef.classTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - super types [%s] - attributes %s", classType.typeName, ClassType.class.getSimpleName(),
                    StringUtils.join(classType.superTypes, ","), Arrays.toString(classType.attributeDefinitions)));
        }
        for (HierarchicalTypeDefinition<TraitType> traitType : typesDef.traitTypesAsJavaList()) {
            System.out.println(String.format("%s(%s) - %s", traitType.typeName, TraitType.class.getSimpleName(),
                    Arrays.toString(traitType.attributeDefinitions)));
        }
    }
}
