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
import org.apache.atlas.typesystem.types.TypeUtils;
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
import java.util.List;
import java.util.UUID;

/**
 * Integration tests for Entity Jersey Resource.
 */
public class EntityJerseyResourceIT extends BaseResourceIT {

    private static final Logger LOG = LoggerFactory.getLogger(EntityJerseyResourceIT.class);

    private final String DATABASE_NAME = "db" + randomString();
    private final String TABLE_NAME = "table" + randomString();
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
                service.path("api/atlas/entity").accept(Servlets.JSON_MEDIA_TYPE).type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class,
                                InstanceSerialization.toJson(databaseInstance, true));
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        String responseAsString = clientResponse.getEntity(String.class);
        Assert.assertNotNull(responseAsString);

        JSONObject response = new JSONObject(responseAsString);
        Assert.assertNotNull(response.get(AtlasClient.REQUEST_ID));
        Assert.assertNotNull(response.get(AtlasClient.GUID));
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
        ClientResponse clientResponse = addProperty(guid, "description", description);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

        String entityRef = getEntityDefinition(getEntityDefinition(guid));
        Assert.assertNotNull(entityRef);

        tableInstance.set("description", description);

        //invalid property for the type
        clientResponse = addProperty(guid, "invalid_property", "bar table");
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        //non-string property, update
        String currentTime = String.valueOf(System.currentTimeMillis());
        clientResponse = addProperty(guid, "createTime", currentTime);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());

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

    @Test(dependsOnMethods = "testSubmitEntity", expectedExceptions = IllegalArgumentException.class)
    public void testAddNullPropertyValue() throws Exception {
        final String guid = tableId._getId();
        //add property
        addProperty(guid, "description", null);
        Assert.fail();
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
        ClientResponse clientResponse = addProperty(guid, "db", dbId);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
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

    private ClientResponse addProperty(String guid, String property, String value) {
        WebResource resource = service.path("api/atlas/entity").path(guid);

        return resource.queryParam("property", property).queryParam("value", value).accept(Servlets.JSON_MEDIA_TYPE)
                .type(Servlets.JSON_MEDIA_TYPE).method(HttpMethod.PUT, ClientResponse.class);
    }

    private ClientResponse getEntityDefinition(String guid) {
        WebResource resource = service.path("api/atlas/entity").path(guid);
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
        WebResource resource = service.path("api/atlas/entity").path("blah");

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
                service.path("api/atlas/entity").queryParam("type", "blah").accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").queryParam("type", typeName).accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").path(guid).path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
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
                service.path("api/atlas/entity").path("random").path(TRAITS).accept(Servlets.JSON_MEDIA_TYPE)
                        .type(Servlets.JSON_MEDIA_TYPE)
                        .method(HttpMethod.POST, ClientResponse.class, traitInstanceAsJSON);
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test(dependsOnMethods = "testAddTrait")
    public void testDeleteTrait() throws Exception {
        final String guid = tableId._getId();

        ClientResponse clientResponse = service.path("api/atlas/entity").path(guid).path(TRAITS).path(traitName)
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

        ClientResponse clientResponse = service.path("api/atlas/entity").path("random").path(TRAITS).path(traitName)
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
        TypesDef typesDef = TypeUtils
                .getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
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
}
