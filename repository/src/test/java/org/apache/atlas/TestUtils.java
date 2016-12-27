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

package org.apache.atlas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Provider;

import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.typestore.GraphBackedTypeStore;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.services.DefaultMetadataService;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createTraitTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createUniqueRequiredAttrDef;

/**
 * Test utility class.
 */
public final class TestUtils {

    public static final long TEST_DATE_IN_LONG = 1418265358440L;

    private TestUtils() {
    }

    /**
     * Dumps the graph in GSON format in the path returned.
     *
     * @param graph handle to graph
     * @return path to the dump file
     * @throws Exception
     */
    public static String dumpGraph(AtlasGraph<?,?> graph) throws Exception {
        File tempFile = File.createTempFile("graph", ".gson");
        System.out.println("tempFile.getPath() = " + tempFile.getPath());
        GraphHelper.dumpToLog(graph);
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(tempFile);
            graph.exportToGson(os);
        }
        finally {
            if(os != null) {
                try {
                    os.close();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return tempFile.getPath();
    }

    /**
     * Class Hierarchy is:
     * Department(name : String, employees : Array[Person])
     * Person(name : String, department : Department, manager : Manager)
     * Manager(subordinates : Array[Person]) extends Person
     * <p/>
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    public static void defineDeptEmployeeTypes(TypeSystem ts) throws AtlasException {

        String _description = "_description";
        EnumTypeDefinition orgLevelEnum =
                new EnumTypeDefinition("OrgLevel", "OrgLevel"+_description, new EnumValue("L1", 1), new EnumValue("L2", 2));

        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", "Address"+_description, createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                        true, "department"));

        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", "Person"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                createOptionalAttrDef("orgLevel", "OrgLevel"),
                createOptionalAttrDef("address", "Address"),
                new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
                new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates"),
                new AttributeDefinition("mentor", "Person", Multiplicity.OPTIONAL, false, null),
                createOptionalAttrDef("birthday", DataTypes.DATE_TYPE),
                createOptionalAttrDef("hasPets", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("numberOfCars", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("houseNumber", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("carMileage", DataTypes.INT_TYPE),
                createOptionalAttrDef("shares", DataTypes.LONG_TYPE),
                createOptionalAttrDef("salary", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("age", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("numberOfStarsEstimate", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("approximationOfPi", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("isOrganDonor", DataTypes.BOOLEAN_TYPE)
                );

        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager", "Manager"+_description, ImmutableSet.of("Person"),
                new AttributeDefinition("subordinates", String.format("array<%s>", "Person"), Multiplicity.COLLECTION,
                        false, "manager"));

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                createTraitTypeDef("SecurityClearance", "SecurityClearance"+_description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, managerTypeDef));
    }

    public static final String DEPARTMENT_TYPE = "Department";
    public static final String PERSON_TYPE = "Person";

    public static ITypedReferenceableInstance createDeptEg1(TypeSystem ts) throws AtlasException {
        Referenceable hrDept = new Referenceable(DEPARTMENT_TYPE);
        Referenceable john = new Referenceable(PERSON_TYPE);

        Referenceable jane = new Referenceable("Manager", "SecurityClearance");
        Referenceable johnAddr = new Referenceable("Address");
        Referenceable janeAddr = new Referenceable("Address");
        Referenceable julius = new Referenceable("Manager");
        Referenceable juliusAddr = new Referenceable("Address");
        Referenceable max = new Referenceable("Person");
        Referenceable maxAddr = new Referenceable("Address");

        hrDept.set("name", "hr");
        john.set("name", "John");
        john.set("department", hrDept);
        johnAddr.set("street", "Stewart Drive");
        johnAddr.set("city", "Sunnyvale");
        john.set("address", johnAddr);

        john.set("birthday",new Date(1950, 5, 15));
        john.set("isOrganDonor", true);
        john.set("hasPets", true);
        john.set("numberOfCars", 1);
        john.set("houseNumber", 153);
        john.set("carMileage", 13364);
        john.set("shares", 15000);
        john.set("salary", 123345.678);
        john.set("age", 50);
        john.set("numberOfStarsEstimate", new BigInteger("1000000000000000000000"));
        john.set("approximationOfPi", new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307816406286"));

        jane.set("name", "Jane");
        jane.set("department", hrDept);
        janeAddr.set("street", "Great America Parkway");
        janeAddr.set("city", "Santa Clara");
        jane.set("address", janeAddr);
        janeAddr.set("street", "Great America Parkway");

        julius.set("name", "Julius");
        julius.set("department", hrDept);
        juliusAddr.set("street", "Madison Ave");
        juliusAddr.set("city", "Newtonville");
        julius.set("address", juliusAddr);
        julius.set("subordinates", ImmutableList.<Referenceable>of());

        max.set("name", "Max");
        max.set("department", hrDept);
        maxAddr.set("street", "Ripley St");
        maxAddr.set("city", "Newton");
        max.set("address", maxAddr);
        max.set("manager", jane);
        max.set("mentor", julius);
        max.set("birthday",new Date(1979, 3, 15));
        max.set("isOrganDonor", true);
        max.set("hasPets", true);
        max.set("age", 36);
        max.set("numberOfCars", 2);
        max.set("houseNumber", 17);
        max.set("carMileage", 13);
        max.set("shares", Long.MAX_VALUE);
        max.set("salary", Double.MAX_VALUE);
        max.set("numberOfStarsEstimate", new BigInteger("1000000000000000000000000000000"));
        max.set("approximationOfPi", new BigDecimal("3.1415926535897932"));

        john.set("manager", jane);
        john.set("mentor", max);
        hrDept.set("employees", ImmutableList.of(john, jane, julius, max));

        jane.set("subordinates", ImmutableList.of(john, max));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);
        Assert.assertNotNull(hrDept2);

        return hrDept2;
    }

    public static final String DATABASE_TYPE = "hive_database";
    public static final String DATABASE_NAME = "foo";
    public static final String TABLE_TYPE = "hive_table";
    public static final String PROCESS_TYPE = "hive_process";
    public static final String COLUMN_TYPE = "column_type";
    public static final String TABLE_NAME = "bar";
    public static final String CLASSIFICATION = "classification";
    public static final String PII = "PII";
    public static final String SUPER_TYPE_NAME = "Base";
    public static final String STORAGE_DESC_TYPE = "hive_storagedesc";
    public static final String PARTITION_STRUCT_TYPE = "partition_struct_type";
    public static final String PARTITION_CLASS_TYPE = "partition_class_type";
    public static final String SERDE_TYPE = "serdeType";
    public static final String COLUMNS_MAP = "columnsMap";
    public static final String COLUMNS_ATTR_NAME = "columns";

    public static final String NAME = "name";

    public static TypesDef simpleType(){
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("s_type", "structType",
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE)});

        HierarchicalTypeDefinition<TraitType> traitTypeDefinition =
                createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        EnumValue values[] = {new EnumValue("ONE", 1),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("e_type", "enumType", values);
        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition));
    }

    public static TypesDef simpleTypeUpdated(){
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> newSuperTypeDefinition =
                createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("s_type", "structType",
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE)});

        HierarchicalTypeDefinition<TraitType> traitTypeDefinition =
                createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        EnumValue values[] = {new EnumValue("ONE", 1),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("e_type", "enumType", values);
        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition, newSuperTypeDefinition));
    }

    public static TypesDef simpleTypeUpdatedDiff() {
        HierarchicalTypeDefinition<ClassType> newSuperTypeDefinition =
                createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.of(newSuperTypeDefinition));
    }

    public static TypesDef defineHiveTypes() {
        String _description = "_description";
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef(SUPER_TYPE_NAME, ImmutableSet.<String>of(),
                        createOptionalAttrDef("namespace", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("cluster", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("colo", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE + _description,ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));


        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType", "serdeType" + _description,
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                    createRequiredAttrDef("serde", DataTypes.STRING_TYPE),
                    createOptionalAttrDef("description", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", "tableType" + _description, values);

        HierarchicalTypeDefinition<ClassType> columnsDefinition =
                createClassTypeDef(COLUMN_TYPE, ImmutableSet.<String>of(),
                        createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition = new StructTypeDefinition("partition_struct_type", "partition_struct_type" + _description,
                new AttributeDefinition[]{createRequiredAttrDef("name", DataTypes.STRING_TYPE),});

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
            new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.REQUIRED, false,
                null),
            new AttributeDefinition("numBuckets", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            };

        HierarchicalTypeDefinition<ClassType> storageDescClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, STORAGE_DESC_TYPE, STORAGE_DESC_TYPE + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), attributeDefinitions);

        AttributeDefinition[] partClsAttributes = new AttributeDefinition[]{
            new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("table", TABLE_TYPE, Multiplicity.REQUIRED, false, null),
            new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true,
                null),
            new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
                Multiplicity.OPTIONAL, true, null),
            new AttributeDefinition("parameters", new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> partClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, "partition_class_type", "partition_class_type" + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), partClsAttributes);

        HierarchicalTypeDefinition<ClassType> processClsType =
                new HierarchicalTypeDefinition<>(ClassType.class, PROCESS_TYPE, PROCESS_TYPE + _description,
                        ImmutableSet.<String>of(), new AttributeDefinition[]{
                        new AttributeDefinition("outputs", "array<" + TABLE_TYPE + ">", Multiplicity.OPTIONAL, false, null)
                });

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE, TABLE_TYPE + _description, ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        // enum
                        new AttributeDefinition("tableType", "tableType", Multiplicity.REQUIRED, false, null),
                        // array of strings
                        new AttributeDefinition("columnNames",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()), Multiplicity.OPTIONAL,
                                false, null),
                        // array of classes
                        new AttributeDefinition("columns", String.format("array<%s>", COLUMN_TYPE),
                                Multiplicity.OPTIONAL, true, null),
                        // array of structs
                        new AttributeDefinition("partitions", String.format("array<%s>", "partition_struct_type"),
                                Multiplicity.OPTIONAL, true, null),
                        // map of primitives
                        new AttributeDefinition("parametersMap",
                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                                Multiplicity.OPTIONAL, true, null),
                        //map of classes -
                        new AttributeDefinition(COLUMNS_MAP,
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                COLUMN_TYPE),
                                                        Multiplicity.OPTIONAL, true, null),
                         //map of structs
                        new AttributeDefinition("partitionsMap",
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                "partition_struct_type"),
                                                        Multiplicity.OPTIONAL, true, null),
                        // struct reference
                        new AttributeDefinition("serde1", "serdeType", Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("serde2", "serdeType", Multiplicity.OPTIONAL, false, null),
                        // class reference
                        new AttributeDefinition("database", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                        //class reference as composite
                        new AttributeDefinition("databaseComposite", DATABASE_TYPE, Multiplicity.OPTIONAL, true, null));

        HierarchicalTypeDefinition<TraitType> piiTypeDefinition =
                createTraitTypeDef(PII, PII + _description, ImmutableSet.<String>of());

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef(CLASSIFICATION, CLASSIFICATION + _description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> fetlClassificationTypeDefinition =
                createTraitTypeDef("fetl" + CLASSIFICATION, "fetl" + CLASSIFICATION + _description, ImmutableSet.of(CLASSIFICATION),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition, fetlClassificationTypeDefinition, piiTypeDefinition),
                ImmutableList.of(superTypeDefinition, databaseTypeDefinition, columnsDefinition, tableTypeDefinition,
                        storageDescClsDef, partClsDef, processClsType));
    }

    public static Collection<IDataType> createHiveTypes(TypeSystem typeSystem) throws Exception {
        if (!typeSystem.isRegistered(TABLE_TYPE)) {
            TypesDef typesDef = defineHiveTypes();
            return typeSystem.defineTypes(typesDef).values();
        }
        return new ArrayList<>();
    }

    public static final String randomString() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    public static Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, dbName);
        entity.set("description", "us db");
        return entity;
    }

    public static Referenceable createTableEntity(String dbId) {
        Referenceable entity = new Referenceable(TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, tableName);
        entity.set("description", "random table");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", new Id(dbId, 0, DATABASE_TYPE));
        entity.set("created", new Date());
        return entity;
    }

    public static Referenceable createColumnEntity() {
        Referenceable entity = new Referenceable(COLUMN_TYPE);
        entity.set(NAME, RandomStringUtils.randomAlphanumeric(10));
        entity.set("type", "VARCHAR(32)");
        return entity;
    }

    public static String createInstance(MetadataService metadataService, Referenceable entity) throws Exception {
        RequestContext.createContext();

        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        List<String> guids = metadataService.createEntities(entitiesJson.toString());
        if (guids != null && guids.size() > 0) {
            return guids.get(guids.size() - 1);
        }
        return null;
    }
    
    public static void resetRequestContext() {      
        RequestContext.createContext();
    }
    
    public static void setupGraphProvider(MetadataRepository repo) throws AtlasException {
        TypeCache typeCache = null;
        try {
            typeCache = AtlasRepositoryConfiguration.getTypeCache().newInstance();
        }
        catch(Throwable t) {
            typeCache = new DefaultTypeCache();
        }
        final GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(new AtlasTypeRegistry());
        Provider<TypesChangeListener> indexerProvider = new Provider<TypesChangeListener>() {

            @Override
            public TypesChangeListener get() {
                return indexer;
            }
        };

        Configuration config = ApplicationProperties.get();
        ITypeStore typeStore = new GraphBackedTypeStore();
        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(repo,
                typeStore,
                Collections.singletonList(indexerProvider),
                new ArrayList<Provider<EntityChangeListener>>(), TypeSystem.getInstance(), config, typeCache);

        //commit the created types
        getGraph().commit();

    }
    
    public static AtlasGraph getGraph() {

        return AtlasGraphProvider.getGraphInstance();
       
    }
}
