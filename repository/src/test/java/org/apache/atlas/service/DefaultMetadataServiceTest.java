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

package org.apache.atlas.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Guice(modules = RepositoryMetadataModule.class)
public class DefaultMetadataServiceTest {
    @Inject
    private MetadataService metadataService;
    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    private Referenceable db = createDBEntity();

    private Id dbId;

    private Referenceable table;

    private Id tableId;


    @BeforeTest
    public void setUp() throws Exception {
        TypesDef typesDef = TestUtils.defineHiveTypes();
        try {
            metadataService.getTypeDefinition(TestUtils.TABLE_TYPE);
        } catch (TypeNotFoundException e) {
            metadataService.createType(TypesSerialization.toJson(typesDef));
        }

        String dbGUid = createInstance(db);
        dbId = new Id(dbGUid, 0, TestUtils.DATABASE_TYPE);

        table = createTableEntity(dbId);
        String tableGuid = createInstance(table);
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        table = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        tableId = new Id(tableGuid, 0, TestUtils.TABLE_TYPE);
    }

    @AfterTest
    public void shutdown() {
        TypeSystem.getInstance().reset();
        try {
            //TODO - Fix failure during shutdown while using BDB
            graphProvider.get().shutdown();
        } catch(Exception e) {
            e.printStackTrace();
        }
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private String createInstance(Referenceable entity) throws Exception {
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.createEntities(entitiesJson.toString());
        return new JSONArray(response).getString(0);
    }

    private String updateInstance(Referenceable entity) throws Exception {
        ParamChecker.notNull(entity, "Entity");
        ParamChecker.notNull(entity.getId(), "Entity");
        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        String response = metadataService.updateEntities(entitiesJson.toString());
        return new JSONArray(response).getString(0);
    }

    private Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(TestUtils.DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set("name", dbName);
        entity.set("description", "us db");
        return entity;
    }

    private Referenceable createTableEntity(Id dbId) {
        Referenceable entity = new Referenceable(TestUtils.TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set("name", tableName);
        entity.set("description", "random table");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", dbId);
        entity.set("created", new Date());
        return entity;
    }

    @Test
    public void testCreateEntityWithUniqueAttribute() throws Exception {
        //name is the unique attribute
        Referenceable entity = createDBEntity();
        String id = createInstance(entity);

        //using the same name should succeed, but not create another entity
        String newId = createInstance(entity);
        Assert.assertEquals(newId, id);

        //Same entity, but different qualified name should succeed
        entity.set("name", TestUtils.randomString());
        newId = createInstance(entity);
        Assert.assertNotEquals(newId, id);
    }

    @Test
    public void testCreateEntityWithUniqueAttributeWithReference() throws Exception {
        Referenceable db = createDBEntity();
        String dbId = createInstance(db);

        Referenceable table = new Referenceable(TestUtils.TABLE_TYPE);
        table.set("name", TestUtils.randomString());
        table.set("description", "random table");
        table.set("type", "type");
        table.set("tableType", "MANAGED");
        table.set("database", new Id(dbId, 0, TestUtils.DATABASE_TYPE));
        table.set("databaseComposite", db);
        createInstance(table);

        //table create should re-use the db instance created earlier
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Referenceable actualDb = (Referenceable) tableDefinition.get("databaseComposite");
        Assert.assertEquals(actualDb.getId().id, dbId);
    }

    @Test
    public void testUpdateEntityByUniqueAttribute() throws Exception {
        final List<String> colNameList = ImmutableList.of("col1", "col2");
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", colNameList);
        }});
        metadataService.updateEntityByUniqueAttribute(table.getTypeName(), "name", (String) table.get("name"), tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<String> actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, colNameList);
    }

    @Test
    public void testUpdateEntityWithMap() throws Exception {

        final Map<String, Struct> partsMap = new HashMap<>();
        partsMap.put("part0", new Struct("partition_type",
            new HashMap<String, Object>() {{
                put("name", "test");
            }}));

        table.set("partitionsMap", partsMap);

        updateInstance(table);
        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertTrue(partsMap.get("part0").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0")));

        //update map - add a map key
        partsMap.put("part1", new Struct("partition_type",
            new HashMap<String, Object>() {{
                put("name", "test1");
            }}));
        table.set("partitionsMap", partsMap);

        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertTrue(partsMap.get("part1").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part1")));

        //update map - remove a key and add another key
        partsMap.remove("part0");
        partsMap.put("part2", new Struct("partition_type",
            new HashMap<String, Object>() {{
                put("name", "test2");
            }}));
        table.set("partitionsMap", partsMap);

        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part2")));


        //update struct value for existing map key
        Struct partition2 = (Struct)partsMap.get("part2");
        partition2.set("name", "test2Updated");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(((Map<String, Struct>)tableDefinition.get("partitionsMap")).size(), 2);
        Assert.assertNull(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part0"));
        Assert.assertTrue(partsMap.get("part2").equalsContents(((Map<String, Struct>)tableDefinition.get("partitionsMap")).get("part2")));
    }

    @Test
    public void testUpdateEntityAddAndUpdateArrayAttr() throws Exception {
        //Update entity, add new array attribute
        //add array of primitives
        final List<String> colNameList = ImmutableList.of("col1", "col2");
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", colNameList);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<String> actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, colNameList);

        //update array of primitives
        final List<String> updatedColNameList = ImmutableList.of("col2", "col3");
        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columnNames", updatedColNameList);
        }});
        metadataService.updateEntityPartialByGuid(tableId.getId()._getId(), tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        actualColumns = (List) tableDefinition.get("columnNames");
        Assert.assertEquals(actualColumns, updatedColNameList);
    }

    @Test
    public void testUpdateEntityArrayOfClass() throws Exception {
        //test array of class with id
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put("name", "col1");
        values.put("type", "type");
        Referenceable ref = new Referenceable("column_type", values);
        columns.add(ref);
        Referenceable tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columns", columns);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        final List<Referenceable> arrClsColumns = (List) tableDefinition.get("columns");
        Assert.assertTrue(arrClsColumns.get(0).equalsContents(columns.get(0)));

        //Partial update. Add col5 But also update col1
        Map<String, Object> valuesCol5 = new HashMap<>();
        valuesCol5.put("name", "col5");
        valuesCol5.put("type", "type");
        ref = new Referenceable("column_type", valuesCol5);
        //update col1
        arrClsColumns.get(0).set("type", "type1");

        //add col5
        final List<Referenceable> updateColumns = new ArrayList<>(arrClsColumns);
        updateColumns.add(ref);

        tableUpdated = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("columns", updateColumns);
        }});
        metadataService.updateEntityPartialByGuid(tableId._getId(), tableUpdated);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        List<Referenceable> arrColumnsList = (List) tableDefinition.get("columns");
        Assert.assertEquals(arrColumnsList.size(), 2);
        Assert.assertTrue(arrColumnsList.get(0).equalsContents(updateColumns.get(0)));
        Assert.assertTrue(arrColumnsList.get(1).equalsContents(updateColumns.get(1)));

        //Complete update. Add  array elements - col3,4
        Map<String, Object> values1 = new HashMap<>();
        values1.put("name", "col3");
        values1.put("type", "type");
        Referenceable ref1 = new Referenceable("column_type", values1);
        columns.add(ref1);

        Map<String, Object> values2 = new HashMap<>();
        values2.put("name", "col4");
        values2.put("type", "type");
        Referenceable ref2 = new Referenceable("column_type", values2);
        columns.add(ref2);

        table.set("columns", columns);
        updateInstance(table);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        arrColumnsList = (List) tableDefinition.get("columns");
        Assert.assertEquals(arrColumnsList.size(), columns.size());
        Assert.assertTrue(arrColumnsList.get(1).equalsContents(columns.get(1)));
        Assert.assertTrue(arrColumnsList.get(2).equalsContents(columns.get(2)));


        //Remove a class reference/Id and insert another reference
        //Also covers isComposite case since columns is a composite
        values.clear();
        columns.clear();

        values.put("name", "col2");
        values.put("type", "type");
        ref = new Referenceable("column_type", values);
        columns.add(ref);
        table.set("columns", columns);
        updateInstance(table);

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        arrColumnsList = (List) tableDefinition.get("columns");
        Assert.assertEquals(arrColumnsList.size(), columns.size());
        Assert.assertTrue(arrColumnsList.get(0).equalsContents(columns.get(0)));

        //Update array column to null
        table.setNull("columns");
        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNull(tableDefinition.get("columns"));
    }


    @Test
    public void testStructs() throws Exception {
        Struct serdeInstance = new Struct(TestUtils.SERDE_TYPE);
        serdeInstance.set("name", "serde1Name");
        serdeInstance.set("serde", "test");
        serdeInstance.set("description", "testDesc");
        table.set("serde1", serdeInstance);

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNotNull(tableDefinition.get("serde1"));
        Assert.assertTrue(serdeInstance.equalsContents(tableDefinition.get("serde1")));

        //update struct attribute
        serdeInstance.set("serde", "testUpdated");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertTrue(serdeInstance.equalsContents(tableDefinition.get("serde1")));

        //set to null
        serdeInstance.setNull("description");
        updateInstance(table);
        tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNull(((Struct)tableDefinition.get("serde1")).get("description"));
    }

    @Test
    public void testClassUpdate() throws Exception {
        //Create new db instance
        final Referenceable databaseInstance = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance.set("name", TestUtils.randomString());
        databaseInstance.set("description", "new database");

        String dbId = createInstance(databaseInstance);

        /*Update reference property with Id */
        metadataService.updateEntityAttributeByGuid(tableId._getId(), "database", dbId);

        String tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(dbId, (((Id)tableDefinition.get("database"))._getId()));

        /* Update with referenceable - TODO - Fails . Need to fix this */
        /*final String dbName = TestUtils.randomString();
        final Referenceable databaseInstance2 = new Referenceable(TestUtils.DATABASE_TYPE);
        databaseInstance2.set("name", dbName);
        databaseInstance2.set("description", "new database 2");

        Referenceable updateTable = new Referenceable(TestUtils.TABLE_TYPE, new HashMap<String, Object>() {{
            put("database", databaseInstance2);
        }});
        metadataService.updateEntityAttributeByGuid(tableId._getId(), updateTable);

        tableDefinitionJson =
            metadataService.getEntityDefinition(tableId._getId());
        Referenceable tableDefinitionActual = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        String dbDefJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", dbName);
        Referenceable dbDef = InstanceSerialization.fromJsonReferenceable(dbDefJson, true);

        Assert.assertNotEquals(dbId, (((Id) tableDefinitionActual.get("database"))._getId()));
        Assert.assertEquals(dbDef.getId()._getId(), (((Id) tableDefinitionActual.get("database"))._getId())); */

    }

    @Test
    public void testArrayOfStructs() throws Exception {
        //Add array of structs
        TestUtils.dumpGraph(graphProvider.get());

        final Struct partition1 = new Struct(TestUtils.PARTITION_TYPE);
        partition1.set("name", "part1");

        final Struct partition2 = new Struct(TestUtils.PARTITION_TYPE);
        partition2.set("name", "part2");

        List<Struct> partitions = new ArrayList<Struct>(){{ add(partition1); add(partition2); }};
        table.set("partitions", partitions);

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        List<Struct> partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));

        //add a new element to array of struct
        final Struct partition3 = new Struct(TestUtils.PARTITION_TYPE);
        partition3.set("name", "part3");
        partitions.add(partition3);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 3);
        Assert.assertTrue(partitions.get(2).equalsContents(partitionsActual.get(2)));

        //remove one of the struct values
        partitions.remove(1);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));
        Assert.assertTrue(partitions.get(1).equalsContents(partitionsActual.get(1)));

        //Update struct value within array of struct
        partition1.set("name", "part4");
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 2);
        Assert.assertTrue(partitions.get(0).equalsContents(partitionsActual.get(0)));

        //add a repeated element to array of struct
        final Struct partition4 = new Struct(TestUtils.PARTITION_TYPE);
        partition4.set("name", "part4");
        partitions.add(partition4);
        table.set("partitions", partitions);
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNotNull(tableDefinition.get("partitions"));
        partitionsActual = (List<Struct>) tableDefinition.get("partitions");
        Assert.assertEquals(partitionsActual.size(), 3);
        Assert.assertEquals(partitionsActual.get(2).get("name"), "part4");
        Assert.assertEquals(partitionsActual.get(0).get("name"), "part4");
        Assert.assertTrue(partitions.get(2).equalsContents(partitionsActual.get(2)));


        // Remove all elements. Should set array attribute to null
        partitions.clear();
        newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertNull(tableDefinition.get("partitions"));
    }


    @Test(expectedExceptions = ValueConversionException.class)
    public void testUpdateRequiredAttrToNull() throws Exception {
        //Update required attribute
        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        Assert.assertEquals(tableDefinition.get("description"), "random table");
        table.setNull("description");

        updateInstance(table);
        Assert.fail("Expected exception while updating required attribute to null");
    }

    @Test
    public void testUpdateOptionalAttrToNull() throws Exception {

        String tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);

        //Update optional Attribute
        Assert.assertNotNull(tableDefinition.get("created"));
        //Update optional attribute
        table.setNull("created");

        String newtableId = updateInstance(table);
        Assert.assertEquals(newtableId, tableId._getId());

        tableDefinitionJson =
            metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        Assert.assertNull(tableDefinition.get("created"));
    }

    @Test
    public void testCreateEntityWithEnum() throws Exception {
        String tableDefinitionJson =
                metadataService.getEntityDefinition(TestUtils.TABLE_TYPE, "name", (String) table.get("name"));
        Referenceable tableDefinition = InstanceSerialization.fromJsonReferenceable(tableDefinitionJson, true);
        EnumValue tableType = (EnumValue) tableDefinition.get("tableType");

        Assert.assertEquals(tableType, new EnumValue("MANAGED", 1));
    }

    @Test
    public void testGetEntityByUniqueAttribute() throws Exception {
        Referenceable entity = createDBEntity();
        createInstance(entity);

        //get entity by valid qualified name
        String entityJson = metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name",
                (String) entity.get("name"));
        Assert.assertNotNull(entityJson);
        Referenceable referenceable = InstanceSerialization.fromJsonReferenceable(entityJson, true);
        Assert.assertEquals(referenceable.get("name"), entity.get("name"));

        //get entity by invalid qualified name
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "name", "random");
            Assert.fail("Expected EntityNotFoundException");
        } catch (EntityNotFoundException e) {
            //expected
        }

        //get entity by non-unique attribute
        try {
            metadataService.getEntityDefinition(TestUtils.DATABASE_TYPE, "description",
                    (String) entity.get("description"));
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }
}
