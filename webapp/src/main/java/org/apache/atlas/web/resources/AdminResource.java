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

import com.google.inject.Inject;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.aspect.Monitored;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.filters.AtlasCSRFPreventionFilter;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Jersey Resource for admin operations.
 */
@Path("admin")
@Singleton
public class AdminResource {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.AdminResource");

    private static final String isCSRF_ENABLED = "atlas.rest-csrf.enabled";
    private static final String BROWSER_USER_AGENT_PARAM = "atlas.rest-csrf.browser-useragents-regex";
    private static final String CUSTOM_METHODS_TO_IGNORE_PARAM = "atlas.rest-csrf.methods-to-ignore";
    private static final String CUSTOM_HEADER_PARAM = "atlas.rest-csrf.custom-header";
    private static final String isTaxonomyEnabled = "atlas.feature.taxonomy.enable";
    
    private Response version;
    private ServiceState serviceState;

    @Inject
    public AdminResource(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    /**
     * Fetches the thread stack dump for this application.
     *
     * @return json representing the thread stack dump.
     */
    @Monitored
    @GET
    @Path("stack")
    @Produces(MediaType.TEXT_PLAIN)
    public String getThreadDump() {
        ThreadGroup topThreadGroup = Thread.currentThread().getThreadGroup();

        while (topThreadGroup.getParent() != null) {
            topThreadGroup = topThreadGroup.getParent();
        }
        Thread[] threads = new Thread[topThreadGroup.activeCount()];

        int nr = topThreadGroup.enumerate(threads);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < nr; i++) {
            builder.append(threads[i].getName()).append("\nState: ").
                    append(threads[i].getState()).append("\n");
            String stackTrace = StringUtils.join(threads[i].getStackTrace(), "\n");
            builder.append(stackTrace);
        }
        return builder.toString();
    }

    /**
     * Fetches the version for this application.
     *
     * @return json representing the version.
     */
    @Monitored
    @GET
    @Path("version")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getVersion() {
        if (version == null) {
            try {
                PropertiesConfiguration configProperties = new PropertiesConfiguration("atlas-buildinfo.properties");

                JSONObject response = new JSONObject();
                response.put("Version", configProperties.getString("build.version", "UNKNOWN"));
                response.put("Name", configProperties.getString("project.name", "apache-atlas"));
                response.put("Description", configProperties.getString("project.description",
                        "Metadata Management and Data Governance Platform over Hadoop"));

                // todo: add hadoop version?
                // response.put("Hadoop", VersionInfo.getVersion() + "-r" + VersionInfo.getRevision());
                version = Response.ok(response).build();
            } catch (JSONException | ConfigurationException e) {
                throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
            }
        }

        return version;
    }

    @Monitored
    @GET
    @Path("status")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getStatus() {
        JSONObject responseData = new JSONObject();
        try {
            responseData.put(AtlasClient.STATUS, serviceState.getState().toString());
            Response response = Response.ok(responseData).build();
            return response;
        } catch (JSONException e) {
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    @Monitored
    @GET
    @Path("session")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getUserProfile() {
        JSONObject responseData = new JSONObject();
        Boolean enableTaxonomy = null;
        try {
            PropertiesConfiguration configProperties = new PropertiesConfiguration("atlas-application.properties");
            enableTaxonomy = configProperties.getBoolean(isTaxonomyEnabled, false);
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            String userName = null;
            Set<String> groups = new HashSet<>();
            if (auth != null) {
                userName = auth.getName();
                Collection<? extends GrantedAuthority> authorities = auth.getAuthorities();
                for (GrantedAuthority c : authorities) {
                    groups.add(c.getAuthority());
                }
            } 
            
            responseData.put(isCSRF_ENABLED,  AtlasCSRFPreventionFilter.isCSRF_ENABLED);
            responseData.put(BROWSER_USER_AGENT_PARAM, AtlasCSRFPreventionFilter.BROWSER_USER_AGENTS_DEFAULT);
            responseData.put(CUSTOM_METHODS_TO_IGNORE_PARAM, AtlasCSRFPreventionFilter.METHODS_TO_IGNORE_DEFAULT);
            responseData.put(CUSTOM_HEADER_PARAM, AtlasCSRFPreventionFilter.HEADER_DEFAULT);
            
            responseData.put(isTaxonomyEnabled, enableTaxonomy);
            
            responseData.put("userName", userName);
            responseData.put("groups", groups);
            return Response.ok(responseData).build();
        } catch (JSONException | ConfigurationException e) {
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
