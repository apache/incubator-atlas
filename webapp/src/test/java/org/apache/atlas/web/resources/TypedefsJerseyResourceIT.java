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

package org.apache.atlas.web.resources;

import com.google.common.collect.ImmutableSet;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.AtlasTypedefClientV2;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.Collections;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import static org.apache.atlas.type.AtlasTypeUtil.createClassTypeDef;
import static org.testng.Assert.*;

/**
 * Integration test for types jersey resource.
 */
public class TypedefsJerseyResourceIT extends BaseResourceIT {

    private AtlasTypesDef typeDefinitions;

    private AtlasTypedefClientV2 clientV2;

    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();

        typeDefinitions = createHiveTypes();

        if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
            clientV2 = new AtlasTypedefClientV2(atlasUrls, new String[]{"admin", "admin"});
        } else {
            clientV2 = new AtlasTypedefClientV2(atlasUrls);
        }
    }

    @AfterClass
    public void tearDown() throws Exception {
        emptyTypeDefs(typeDefinitions);
    }

    @Test
    public void testCreate() throws Exception {
        AtlasTypesDef atlasTypeDefs = clientV2.createAtlasTypeDefs(typeDefinitions);
        Assert.assertNotNull(atlasTypeDefs);
        assertFalse(atlasTypeDefs.isEmpty());
    }

    @Test
    public void testDuplicateCreate() throws Exception {
        AtlasEntityDef type = createClassTypeDef(randomString(),
                ImmutableSet.<String>of(), AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"));
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(type);

        AtlasTypesDef created = clientV2.createAtlasTypeDefs(typesDef);
        assertNotNull(created);

        try {
            created = clientV2.createAtlasTypeDefs(typesDef);
            fail("Expected 409");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.CONFLICT.getStatusCode());
        }
    }

    @Test
    public void testUpdate() throws Exception {
        String entityType = randomString();
        AtlasEntityDef typeDefinition =
                createClassTypeDef(entityType, ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"));

        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(typeDefinition);

        AtlasTypesDef createdTypeDefs = clientV2.createAtlasTypeDefs(atlasTypesDef);
        assertNotNull(createdTypeDefs);
        assertEquals(createdTypeDefs.getEntityDefs().size(), atlasTypesDef.getEntityDefs().size());

        //Add attribute description
        typeDefinition = createClassTypeDef(typeDefinition.getName(),
                ImmutableSet.<String>of(),
                AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                AtlasTypeUtil.createOptionalAttrDef("description", "string"));

        emptyTypeDefs(atlasTypesDef);

        atlasTypesDef.getEntityDefs().add(typeDefinition);

        AtlasTypesDef updatedTypeDefs = clientV2.updateAtlasTypeDefs(atlasTypesDef);
        assertNotNull(updatedTypeDefs);
        assertEquals(updatedTypeDefs.getEntityDefs().size(), atlasTypesDef.getEntityDefs().size());
        assertEquals(updatedTypeDefs.getEntityDefs().get(0).getName(), atlasTypesDef.getEntityDefs().get(0).getName());

        MultivaluedMap<String, String> filterParams = new MultivaluedMapImpl();
        filterParams.add(SearchFilter.PARAM_TYPE, "ENTITY");
        AtlasTypesDef allTypeDefs = clientV2.getAllTypeDefs(new SearchFilter(filterParams));
        assertNotNull(allTypeDefs);
        Boolean entityDefFound = false;
        for (AtlasEntityDef atlasEntityDef : allTypeDefs.getEntityDefs()){
            if (atlasEntityDef.getName().equals(typeDefinition.getName())) {
                assertEquals(atlasEntityDef.getAttributeDefs().size(), 2);
                entityDefFound = true;
                break;
            }
        }
        assertTrue(entityDefFound, "Required entityDef not found.");
    }

    @Test(dependsOnMethods = "testCreate")
    public void testGetDefinition() throws Exception {
        if (CollectionUtils.isNotEmpty(typeDefinitions.getEnumDefs())) {
            for (AtlasEnumDef atlasEnumDef : typeDefinitions.getEnumDefs()) {
                verifyByNameAndGUID(atlasEnumDef);
            }
        }

        if (CollectionUtils.isNotEmpty(typeDefinitions.getStructDefs())) {
            for (AtlasStructDef structDef : typeDefinitions.getStructDefs()) {
                verifyByNameAndGUID(structDef);
            }
        }

        if (CollectionUtils.isNotEmpty(typeDefinitions.getClassificationDefs())) {
            for (AtlasClassificationDef classificationDef : typeDefinitions.getClassificationDefs()) {
                verifyByNameAndGUID(classificationDef);
            }
        }

        if (CollectionUtils.isNotEmpty(typeDefinitions.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typeDefinitions.getEntityDefs()) {
                verifyByNameAndGUID(entityDef);
            }
        }
    }

    @Test
    public void testInvalidGets() throws Exception {
        try {
            AtlasEnumDef byName = clientV2.getEnumByName("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasEnumDef byGuid = clientV2.getEnumByGuid("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasStructDef byName = clientV2.getStructByName("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasStructDef byGuid = clientV2.getStructByGuid("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasClassificationDef byName = clientV2.getClassificationByName("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasClassificationDef byGuid = clientV2.getClassificationByGuid("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasEntityDef byName = clientV2.getEntityByName("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }

        try {
            AtlasEntityDef byGuid = clientV2.getEntityByGuid("blah");
            fail("Get for invalid name should have reported a failure");
        } catch (AtlasServiceException e) {
            assertEquals(e.getStatus().getStatusCode(), Response.Status.NOT_FOUND.getStatusCode(),
                    "Should've returned a 404");
        }


    }

    @Test
    public void testListTypesByFilter() throws Exception {
        AtlasStructDef.AtlasAttributeDef attr = AtlasTypeUtil.createOptionalAttrDef("attr", "string");
        AtlasEntityDef classDefA = AtlasTypeUtil.createClassTypeDef("A" + randomString(), ImmutableSet.<String>of(), attr);
        AtlasEntityDef classDefA1 = AtlasTypeUtil.createClassTypeDef("A1" + randomString(), ImmutableSet.of(classDefA.getName()), attr);
        AtlasEntityDef classDefB = AtlasTypeUtil.createClassTypeDef("B" + randomString(), ImmutableSet.<String>of(), attr);
        AtlasEntityDef classDefC = AtlasTypeUtil.createClassTypeDef("C" + randomString(), ImmutableSet.of(classDefB.getName(), classDefA.getName()), attr);

        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        atlasTypesDef.getEntityDefs().add(classDefA);
        atlasTypesDef.getEntityDefs().add(classDefA1);
        atlasTypesDef.getEntityDefs().add(classDefB);
        atlasTypesDef.getEntityDefs().add(classDefC);

        AtlasTypesDef created = clientV2.createAtlasTypeDefs(atlasTypesDef);
        assertNotNull(created);
        assertEquals(created.getEntityDefs().size(), atlasTypesDef.getEntityDefs().size());

        MultivaluedMap<String, String> searchParams = new MultivaluedMapImpl();
        searchParams.add(SearchFilter.PARAM_TYPE, "CLASS");
        searchParams.add(SearchFilter.PARAM_SUPERTYPE, classDefA.getName());
        SearchFilter searchFilter = new SearchFilter(searchParams);
        AtlasTypesDef searchDefs = clientV2.getAllTypeDefs(searchFilter);
        assertNotNull(searchDefs);
        assertEquals(searchDefs.getEntityDefs().size(), 2);

        searchParams.add(SearchFilter.PARAM_NOT_SUPERTYPE, classDefB.getName());
        searchFilter = new SearchFilter(searchParams);
        searchDefs = clientV2.getAllTypeDefs(searchFilter);
        assertNotNull(searchDefs);
        assertEquals(searchDefs.getEntityDefs().size(), 1);
    }

    private AtlasTypesDef createHiveTypes() throws Exception {
        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();

        AtlasEntityDef databaseTypeDefinition =
                createClassTypeDef("database", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"));
        atlasTypesDef.getEntityDefs().add(databaseTypeDefinition);

        AtlasEntityDef tableTypeDefinition =
                createClassTypeDef("table", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createUniqueRequiredAttrDef("name", "string"),
                        AtlasTypeUtil.createRequiredAttrDef("description", "string"),
                        AtlasTypeUtil.createOptionalAttrDef("columnNames", DataTypes.arrayTypeName("string")),
                        AtlasTypeUtil.createOptionalAttrDef("created", "date"),
                        AtlasTypeUtil.createOptionalAttrDef("parameters",
                                DataTypes.mapTypeName("string", "string")),
                        AtlasTypeUtil.createRequiredAttrDef("type", "string"),
                        new AtlasAttributeDef("database", "database",
                                false,
                                Cardinality.SINGLE, 1, 1,
                                true, true,
                                Collections.<AtlasStructDef.AtlasConstraintDef>emptyList()));
        atlasTypesDef.getEntityDefs().add(tableTypeDefinition);

        AtlasClassificationDef fetlTypeDefinition = AtlasTypeUtil
                .createTraitTypeDef("fetl", ImmutableSet.<String>of(),
                        AtlasTypeUtil.createRequiredAttrDef("level", "int"));
        atlasTypesDef.getClassificationDefs().add(fetlTypeDefinition);

        return atlasTypesDef;
    }

    private void verifyByNameAndGUID(AtlasBaseTypeDef typeDef) {
        try {
            AtlasBaseTypeDef byName = null;
            if (typeDef.getCategory() == TypeCategory.ENUM) {
                byName = clientV2.getEnumByName(typeDef.getName());
            } else if (typeDef.getCategory() == TypeCategory.ENTITY) {
                byName = clientV2.getEntityByName(typeDef.getName());
            } else if (typeDef.getCategory() == TypeCategory.CLASSIFICATION) {
                byName = clientV2.getClassificationByName(typeDef.getName());
            } else if (typeDef.getCategory() == TypeCategory.STRUCT) {
                byName = clientV2.getStructByName(typeDef.getName());
            }
            assertNotNull(byName);
        } catch (AtlasServiceException e) {
            fail("Get byName should've succeeded", e);
        }
        if (StringUtils.isNotBlank(typeDef.getGuid())) {
            try {
                AtlasBaseTypeDef byGuid = null;
                if (typeDef.getCategory() == TypeCategory.ENUM) {
                    byGuid = clientV2.getEnumByGuid(typeDef.getGuid());
                } else if (typeDef.getCategory() == TypeCategory.ENTITY) {
                    byGuid = clientV2.getEntityByGuid(typeDef.getGuid());
                } else if (typeDef.getCategory() == TypeCategory.CLASSIFICATION) {
                    byGuid = clientV2.getClassificationByGuid(typeDef.getGuid());
                } else if (typeDef.getCategory() == TypeCategory.STRUCT) {
                    byGuid = clientV2.getStructByGuid(typeDef.getGuid());
                }
                assertNotNull(byGuid);
            } catch (AtlasServiceException e) {
                fail("Get byGuid should've succeeded", e);
            }
        }
    }

    private void emptyTypeDefs(AtlasTypesDef def) {
        def.getEnumDefs().clear();
        def.getStructDefs().clear();
        def.getClassificationDefs().clear();
        def.getEntityDefs().clear();
    }
}
