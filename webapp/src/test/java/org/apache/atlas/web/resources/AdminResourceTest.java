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

import org.apache.atlas.web.service.ServiceState;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AdminResourceTest {

    @Mock
    private ServiceState serviceState;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testStatusOfActiveServerIsReturned() throws JSONException {

        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        AdminResource adminResource = new AdminResource(serviceState);
        Response response = adminResource.getStatus();
        assertEquals(response.getStatus(), HttpServletResponse.SC_OK);
        JSONObject entity = (JSONObject) response.getEntity();
        assertEquals(entity.get("Status"), "ACTIVE");
    }

    @Test
    public void testResourceGetsValueFromServiceState() throws JSONException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        AdminResource adminResource = new AdminResource(serviceState);
        Response response = adminResource.getStatus();

        verify(serviceState).getState();
        JSONObject entity = (JSONObject) response.getEntity();
        assertEquals(entity.get("Status"), "PASSIVE");

    }
}