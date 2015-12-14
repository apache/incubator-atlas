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

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.InstanceSerialization$;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityJerseyResourceIT.class);

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
    private static final String ENTITIES = "api/atlas/entities";
    private static final String TRAITS = "traits";

    private Referenceable tableInstance;
    private Id tableId;
    private String traitName;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        createTypeDefinitions();
    }

    @Test
    public void testSubmitEntity() throws Exception {
        tableInstance = createHiveTableInstance(DATABASE_NAME, TABLE_NAME);
        tableId = createInstance(tableInstance);

        final String guid = tableId._getId();
        try {
            Assert.assertNotNull(UUID.fromString(guid));
        } catch (IllegalArgumentException e) {
            Assert.fail("Response is not a guid, " + guid);
        }
    }

    @Test
    public void testSubmitSingleEntity() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", randomString());
        databaseInstance.set("description", randomString());

        ClientResponse clientResponse =
                service.path(ENTITIES).accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class,
                                InstanceSerialization.toJson(databaseInstance, true));
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get(AtlasClient.GUID));
    }

    @Test
    public void testEntityDefinitionAcrossTypeUpdate() throws Exception {
        //create type
        HierarchicalTypeDefinition<ClassType> typeDefinition = TypesUtil
                .createClassTypeDef(randomString(), ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE));
        serviceClient.createType(TypesSerialization.toJson(typeDefinition, false));

        //create entity for the type
        Referenceable instance = new Referenceable(typeDefinition.typeName);
        instance.set("name", randomString());
        String guid = serviceClient.createEntity(instance).getString(0);

        //update type - add attribute
        typeDefinition = TypesUtil.createClassTypeDef(typeDefinition.typeName, ImmutableList.<String>of(),
                TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE));
        TypesDef typeDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(typeDefinition));
        serviceClient.updateType(typeDef);

        //Get definition after type update - new attributes should be null
        Referenceable entity = serviceClient.getEntity(guid);
        Assert.assertNull(entity.get("description"));
        Assert.assertEquals(entity.get("name"), instance.get("name"));
    }

    @DataProvider
    public Object[][] invalidAttrValues() {
        return new Object[][]{{null}, {""}};
    }

    @Test(dataProvider = "invalidAttrValues")
    public void testEntityInvalidValue(String value) throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", randomString());
        databaseInstance.set("description", value);

        try {
            createInstance(databaseInstance);
            Assert.fail("Expected AtlasServiceException");
        } catch (AtlasServiceException e) {
            Assert.assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST);
        }
    }

    @Test
    public void testGetEntityByAttribute() throws Exception {
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        String dbName = randomString();
        databaseInstance.set("name", dbName);
        databaseInstance.set("description", "foo database");
        createInstance(databaseInstance);

        //get entity by attribute
        Referenceable referenceable = serviceClient.getEntity(DATABASE_TYPE, "name", dbName);
        Assert.assertEquals(referenceable.getTypeName(), DATABASE_TYPE);
        Assert.assertEquals(referenceable.get("name"), dbName);
    }

    @Test
    public void testSubmitEntityWithBadDateFormat() throws Exception {
        try {
            Referenceable tableInstance = createHiveTableInstance("db" + randomString(), "table" + randomString());
            tableInstance.set("lastAccessTime", "2014-07-11");
            tableId = createInstance(tableInstance);
            Assert.fail("Was expecting an  exception here ");
        } catch (AtlasServiceException e) {
            Assert.assertTrue(
                    e.getMessage().contains("\"error\":\"Cannot convert value '2014-07-11' to datatype date\""));
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddProperty() throws Exception {
        final String guid = tableId._getId();
        //add property
        String description = "bar table - new desc";
        addProperty(guid, "description", description);

        String entityRef = getEntityDefinition(getEntityDefinition(guid));
        Assert.assertNotNull(entityRef);

        tableInstance.set("description", description);

        //invalid property for the type
        try {
            addProperty(guid, "invalid_property", "bar table");
            Assert.fail("Expected AtlasServiceException");
        } catch (AtlasServiceException e) {
            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
        }

        //non-string property, update
        String currentTime = String.valueOf(System.currentTimeMillis());
        addProperty(guid, "createTime", currentTime);

        entityRef = getEntityDefinition(getEntityDefinition(guid));
        Assert.assertNotNull(entityRef);

        tableInstance.set("createTime", currentTime);
    }

    @Test(dependsOnMethods = "testSubmitEntity", expectedExceptions = IllegalArgumentException.class)
    public void testAddNullProperty() throws Exception {
        final String guid = tableId._getId();
        //add property
        addProperty(guid, null, "foo bar");
        Assert.fail();
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddNullPropertyValue() throws Exception {
        final String guid = tableId._getId();
        //add property
        try {
            addProperty(guid, "description", null);
            Assert.fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            Assert.assertEquals(e.getStatus().getStatusCode(), Response.Status.BAD_REQUEST.getStatusCode());
        }
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testAddReferenceProperty() throws Exception {
        //Create new db instance
        Referenceable databaseInstance = new Referenceable(DATABASE_TYPE);
        databaseInstance.set("name", randomString());
        databaseInstance.set("description", "new database");

        Id dbInstance = createInstance(databaseInstance);
        String dbId = dbInstance._getId();

        //Add reference property
        final String guid = tableId._getId();
        addProperty(guid, "db", dbId);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityDefinition() throws Exception {
        final String guid = tableId._getId();
        ClientResponse clientResponse = getEntityDefinition(guid);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final String definition = response.getString(AtlasClient.DEFINITION);
        Assert.assertNotNull(definition);
        LOG.debug("tableInstanceAfterGet = " + definition);
        InstanceSerialization.fromJsonReferenceable(definition, true);
    }

    private void addProperty(String guid, String property, String value) throws AtlasServiceException {
        serviceClient.updateEntityAttribute(guid, property, value);
    }

    private ClientResponse getEntityDefinition(String guid) {
        WebResource resource = service.path(ENTITIES).path(guid);
        return resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
    }

    private String getEntityDefinition(ClientResponse clientResponse) throws Exception {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        JSONObject response = new JSONObject(clientResponse.getEntity(String.class));
        final String definition = response.getString(AtlasClient.DEFINITION);
        Assert.assertNotNull(definition);

        return definition;
    }

    @Test
    public void testGetInvalidEntityDefinition() throws Exception {
        WebResource resource = service.path(ENTITIES).path("blah");

        ClientResponse clientResponse = resource.accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.ERROR));
        Assert.assertNotNull(response.get(AtlasClient.STACKTRACE));
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetEntityList() throws Exception {
        List<String> entities = serviceClient.listEntities(HIVE_TABLE_TYPE);
        Assert.assertNotNull(entities);
        Assert.assertTrue(entities.contains(tableId._getId()));
    }

    @Test
    public void testGetEntityListForBadEntityType() throws Exception {
        ClientResponse clientResponse =
                service.path(ENTITIES).queryParam("type", "blah").accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.ERROR));
        Assert.assertNotNull(response.get(AtlasClient.STACKTRACE));
    }


    @Test
    public void testGetEntityListForNoInstances() throws Exception {
        String typeName = addNewType();

        ClientResponse clientResponse =
                service.path(ENTITIES).queryParam("type", typeName).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(list.length(), 0);
    }

    private String addNewType() throws Exception {
        String typeName = "test" + randomString();
        HierarchicalTypeDefinition<ClassType> testTypeDefinition = TypesUtil
                .createClassTypeDef(typeName, ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("description", DataTypes.STRING_TYPE));

        String typesAsJSON = TypesSerialization.toJson(testTypeDefinition, false);
        createType(typesAsJSON);
        return typeName;
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testGetTraitNames() throws Exception {
        final String guid = tableId._getId();
        ClientResponse clientResponse =
                service.path(ENTITIES).path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.GET, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get("GUID"));

        final JSONArray list = response.getJSONArray(AtlasClient.RESULTS);
        Assert.assertEquals(list.length(), 7);
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTrait() throws Exception {
        traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableList.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = " + traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        final String guid = tableId._getId();
        ClientResponse clientResponse =
                service.path(ENTITIES).path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get(AtlasClient.GUID));
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testAddExistingTrait() throws Exception {
        final String traitName = "PII_Trait" + randomString();

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        final String guid = tableId._getId();
        ClientResponse clientResponse =
                service.path(ENTITIES).path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test(dependsOnMethods = "testGetTraitNames")
    public void testAddTraitWithAttribute() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait = TypesUtil
                .createTraitTypeDef(traitName, ImmutableList.<String>of(),
                        TypesUtil.createRequiredAttrDef("type", DataTypes.STRING_TYPE));
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = " + traitDefinitionAsJSON);
        createType(traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        traitInstance.set("type", "SSN");
        String traitInstanceAsJSON = InstanceSerialization.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        final String guid = tableId._getId();
        ClientResponse clientResponse =
                service.path(ENTITIES).path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get(AtlasClient.GUID));

        // verify the response
        clientResponse = getEntityDefinition(guid);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);
        response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));

        final String definition = response.getString(AtlasClient.DEFINITION);
        Assert.assertNotNull(definition);
        Referenceable entityRef = InstanceSerialization.fromJsonReferenceable(definition, true);
        IStruct traitRef = entityRef.getTrait(traitName);
        String type = (String) traitRef.get("type");
        Assert.assertEquals(type, "SSN");
    }

    @Test
    public void testAddTraitWithNoRegistration() throws Exception {
        final String traitName = "PII_Trait" + randomString();
        HierarchicalTypeDefinition<TraitType> piiTrait =
                TypesUtil.createTraitTypeDef(traitName, ImmutableList.<String>of());
        String traitDefinitionAsJSON = TypesSerialization$.MODULE$.toJson(piiTrait, true);
        LOG.debug("traitDefinitionAsJSON = " + traitDefinitionAsJSON);

        Struct traitInstance = new Struct(traitName);
        String traitInstanceAsJSON = InstanceSerialization$.MODULE$.toJson(traitInstance, true);
        LOG.debug("traitInstanceAsJSON = " + traitInstanceAsJSON);

        ClientResponse clientResponse =
                service.path(ENTITIES).path("random").path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = tableId._getId();

        ClientResponse clientResponse = service.path(ENTITIES).path(guid).path(TRAITS).path(traitName)
                .accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.DELETE, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get("GUID"));
        Assert.assertNotNull(response.get("traitName"));
    }

    @Test
    public void testDeleteTraitNonExistent() throws Exception {
        final String traitName = "blah_trait";

        ClientResponse clientResponse = service.path(ENTITIES).path("random").path(TRAITS).path(traitName)
                .accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                .method(HttpMethod.DELETE, ClientResponse.class);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.ERROR));
        Assert.assertEquals(response.getString(AtlasClient.ERROR),
                "trait=" + traitName + " should be defined in type system before it can be deleted");
        Assert.assertNotNull(response.get(AtlasClient.STACKTRACE));
    }

    private String random() {
        return RandomStringUtils.random(10);
    }

    @Test
    public void testUTF8() throws Exception {
        String classType = random();
        String attrName = random();
        String attrValue = random();

        HierarchicalTypeDefinition<ClassType> classTypeDefinition = TypesUtil
                .createClassTypeDef(classType, ImmutableList.<String>of(),
                        TypesUtil.createUniqueRequiredAttrDef(attrName, DataTypes.STRING_TYPE));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(classTypeDefinition));
        createType(typesDef);

        Referenceable instance = new Referenceable(classType);
        instance.set(attrName, attrValue);
        Id guid = createInstance(instance);

        ClientResponse response = getEntityDefinition(guid._getId());
        String definition = getEntityDefinition(response);
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(definition, true);
        Assert.assertEquals(getReferenceable.get(attrName), attrValue);
    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testPartialUpdate() throws Exception {
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values = new HashMap<>();
        values.put("name", "col1");
        values.put("dataType", "string");
        values.put("comment", "col1 comment");

        Referenceable ref = new Referenceable(BaseResourceIT.COLUMN_TYPE, values);
        columns.add(ref);
        Referenceable tableUpdated = new Referenceable(BaseResourceIT.HIVE_TABLE_TYPE, new HashMap<String, Object>() {{
            put("columns", columns);
        }});

        LOG.debug("Updating entity= " + tableUpdated);
        serviceClient.updateEntity(tableId._getId(), tableUpdated);

        ClientResponse response = getEntityDefinition(tableId._getId());
        String definition = getEntityDefinition(response);
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(definition, true);
        List<Referenceable> refs = (List<Referenceable>) getReferenceable.get("columns");

        Assert.assertTrue(refs.get(0).equalsContents(columns.get(0)));

        //Update by unique attribute
        values.put("dataType", "int");
        ref = new Referenceable(BaseResourceIT.COLUMN_TYPE, values);
        columns.set(0, ref);
        tableUpdated = new Referenceable(BaseResourceIT.HIVE_TABLE_TYPE, new HashMap<String, Object>() {{
            put("columns", columns);
        }});

        LOG.debug("Updating entity= " + tableUpdated);
        serviceClient.updateEntity(BaseResourceIT.HIVE_TABLE_TYPE, "name", (String) tableInstance.get("name"),
                tableUpdated);

        response = getEntityDefinition(tableId._getId());
        definition = getEntityDefinition(response);
        getReferenceable = InstanceSerialization.fromJsonReferenceable(definition, true);
        refs = (List<Referenceable>) getReferenceable.get("columns");

        Assert.assertTrue(refs.get(0).equalsContents(columns.get(0)));
        Assert.assertEquals(refs.get(0).get("dataType"), "int");

    }

    @Test(dependsOnMethods = "testSubmitEntity")
    public void testCompleteUpdate() throws Exception {
        final List<Referenceable> columns = new ArrayList<>();
        Map<String, Object> values1 = new HashMap<>();
        values1.put("name", "col3");
        values1.put("dataType", "string");
        values1.put("comment", "col3 comment");

        Map<String, Object> values2 = new HashMap<>();
        values2.put("name", "col4");
        values2.put("dataType", "string");
        values2.put("comment", "col4 comment");

        Referenceable ref1 = new Referenceable(BaseResourceIT.COLUMN_TYPE, values1);
        Referenceable ref2 = new Referenceable(BaseResourceIT.COLUMN_TYPE, values2);
        columns.add(ref1);
        columns.add(ref2);
        tableInstance.set("columns", columns);

        LOG.debug("Replacing entity= " + tableInstance);
        serviceClient.updateEntities(tableInstance);

        ClientResponse response = getEntityDefinition(tableId._getId());
        String definition = getEntityDefinition(response);
        Referenceable getReferenceable = InstanceSerialization.fromJsonReferenceable(definition, true);
        List<Referenceable> refs = (List<Referenceable>) getReferenceable.get("columns");
        Assert.assertEquals(refs.size(), 2);

        Assert.assertTrue(refs.get(0).equalsContents(columns.get(0)));
        Assert.assertTrue(refs.get(1).equalsContents(columns.get(1)));
    }
}
