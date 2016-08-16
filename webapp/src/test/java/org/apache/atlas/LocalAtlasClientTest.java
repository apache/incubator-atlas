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

package org.apache.atlas;

import com.google.inject.Inject;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.resources.EntityResource;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.atlas.AtlasClient.ENTITIES;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules= RepositoryMetadataModule.class)
public class LocalAtlasClientTest {
    @Mock
    private EntityResource mockEntityResource;

    @Inject
    private EntityResource entityResource;

    @Mock
    private ServiceState serviceState;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateEntity() throws Exception {
        Response response = mock(Response.class);
        when(mockEntityResource.submit(any(HttpServletRequest.class))).thenReturn(response);
        final String guid = random();
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(ENTITIES, new JSONObject(
                    new AtlasClient.EntityResult(Arrays.asList(guid), null, null).toString()).get(ENTITIES));
        }});

        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, mockEntityResource);
        List<String> results = atlasClient.createEntity(new Referenceable(random()));
        assertEquals(results.size(), 1);
        assertEquals(results.get(0), guid);
    }

    @Test
    public void testException() throws Exception {
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, mockEntityResource);

        Response response = mock(Response.class);
        when(mockEntityResource.submit(any(HttpServletRequest.class))).thenThrow(new WebApplicationException(response));
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put("stackTrace", "stackTrace");
        }});
        when(response.getStatus()).thenReturn(Response.Status.BAD_REQUEST.getStatusCode());
        try {
            atlasClient.createEntity(new Referenceable(random()));
            fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.BAD_REQUEST);
        }

        when(mockEntityResource.updateByUniqueAttribute(anyString(), anyString(), anyString(),
                any(HttpServletRequest.class))).thenThrow(new WebApplicationException(response));
        when(response.getStatus()).thenReturn(Response.Status.NOT_FOUND.getStatusCode());
        try {
            atlasClient.updateEntity(random(), random(), random(), new Referenceable(random()));
            fail("Expected AtlasServiceException");
        } catch(AtlasServiceException e) {
            assertEquals(e.getStatus(), ClientResponse.Status.NOT_FOUND);
        }

    }

    @Test
    public void testIsServerReady() throws Exception {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, mockEntityResource);
        assertTrue(atlasClient.isServerReady());

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.BECOMING_ACTIVE);
        assertFalse(atlasClient.isServerReady());
    }

    @Test
    public void testUpdateEntity() throws Exception {
        final String guid = random();
        Response response = mock(Response.class);
        when(mockEntityResource.updateByUniqueAttribute(anyString(), anyString(), anyString(),
                any(HttpServletRequest.class))).thenReturn(response);
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(ENTITIES, new JSONObject(
                    new AtlasClient.EntityResult(null, Arrays.asList(guid), null).toString()).get(ENTITIES));
        }});

        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, mockEntityResource);
        AtlasClient.EntityResult
                entityResult = atlasClient.updateEntity(random(), random(), random(), new Referenceable(random()));
        assertEquals(entityResult.getUpdateEntities(), Arrays.asList(guid));
    }

    @Test
    public void testDeleteEntity() throws Exception {
        final String guid = random();
        Response response = mock(Response.class);
        when(response.getEntity()).thenReturn(new JSONObject() {{
            put(ENTITIES, new JSONObject(
                    new AtlasClient.EntityResult(null, null, Arrays.asList(guid)).toString()).get(ENTITIES));
        }});

        when(mockEntityResource.deleteEntities(anyListOf(String.class), anyString(), anyString(), anyString())).thenReturn(response);
        LocalAtlasClient atlasClient = new LocalAtlasClient(serviceState, mockEntityResource);
        AtlasClient.EntityResult entityResult = atlasClient.deleteEntity(random(), random(), random());
        assertEquals(entityResult.getDeletedEntities(), Arrays.asList(guid));
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    @Test
    @Inject
    public void testGetLocationURI() {
        final String guid = "123";
        URI uri = entityResource.getLocationURI(new ArrayList<String>() {{ add(guid); }});
        uri.getRawPath().equals(AtlasConstants.DEFAULT_ATLAS_REST_ADDRESS + "/" + AtlasClient.API.GET_ENTITY.getPath() + "/" + guid);
    }
}
