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
package org.apache.atlas;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.atlas.security.SecureClientUtils;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.security.SecurityProperties.TLS_ENABLED;

public abstract class AtlasBaseClient {
    public static final String BASE_URI = "api/atlas/";
    public static final String TYPES = "types";
    public static final String ADMIN_VERSION = "admin/version";
    public static final String ADMIN_STATUS = "admin/status";
    public static final String HTTP_AUTHENTICATION_ENABLED = "atlas.http.authentication.enabled";
    //Admin operations
    public static final APIInfo VERSION = new APIInfo(BASE_URI + ADMIN_VERSION, HttpMethod.GET, Response.Status.OK);
    public static final APIInfo STATUS = new APIInfo(BASE_URI + ADMIN_STATUS, HttpMethod.GET, Response.Status.OK);
    static final String JSON_MEDIA_TYPE = MediaType.APPLICATION_JSON + "; charset=UTF-8";
    static final String UNKNOWN_STATUS = "Unknown status";
    static final String ATLAS_CLIENT_HA_RETRIES_KEY = "atlas.client.ha.retries";
    // Setting the default value based on testing failovers while client code like quickstart is running.
    static final int DEFAULT_NUM_RETRIES = 4;
    static final String ATLAS_CLIENT_HA_SLEEP_INTERVAL_MS_KEY = "atlas.client.ha.sleep.interval.ms";
    // Setting the default value based on testing failovers while client code like quickstart is running.
    // With number of retries, this gives a total time of about 20s for the server to start.
    static final int DEFAULT_SLEEP_BETWEEN_RETRIES_MS = 5000;
    private static final Logger LOG = LoggerFactory.getLogger(AtlasBaseClient.class);
    protected WebResource service;
    protected Configuration configuration;
    private String basicAuthUser;
    private String basicAuthPassword;
    private AtlasClientContext atlasClientContext;
    private boolean retryEnabled = false;

    protected AtlasBaseClient() {}

    protected AtlasBaseClient(String[] baseUrl, String[] basicAuthUserNamePassword) {
        if (basicAuthUserNamePassword != null) {
            if (basicAuthUserNamePassword.length > 0) {
                this.basicAuthUser = basicAuthUserNamePassword[0];
            }
            if (basicAuthUserNamePassword.length > 1) {
                this.basicAuthPassword = basicAuthUserNamePassword[1];
            }
        }

        initializeState(baseUrl, null, null);
    }

    protected AtlasBaseClient(String... baseUrls) throws AtlasException {
        this(getCurrentUGI(), baseUrls);
    }

    protected AtlasBaseClient(UserGroupInformation ugi, String[] baseUrls) {
        this(ugi, ugi.getShortUserName(), baseUrls);
    }

    protected AtlasBaseClient(UserGroupInformation ugi, String doAsUser, String[] baseUrls) {
        initializeState(baseUrls, ugi, doAsUser);
    }

    @VisibleForTesting
    protected AtlasBaseClient(WebResource service, Configuration configuration) {
        this.service = service;
        this.configuration = configuration;
    }

    @VisibleForTesting
    protected AtlasBaseClient(Configuration configuration, String[] baseUrl, String[] basicAuthUserNamePassword) {
        if (basicAuthUserNamePassword != null) {
            if (basicAuthUserNamePassword.length > 0) {
                this.basicAuthUser = basicAuthUserNamePassword[0];
            }
            if (basicAuthUserNamePassword.length > 1) {
                this.basicAuthPassword = basicAuthUserNamePassword[1];
            }
        }

        initializeState(configuration, baseUrl, null, null);
    }

    protected static UserGroupInformation getCurrentUGI() throws AtlasException {
        try {
            return UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            throw new AtlasException(e);
        }
    }

    void initializeState(String[] baseUrls, UserGroupInformation ugi, String doAsUser) {
        initializeState(getClientProperties(), baseUrls, ugi, doAsUser);
    }

    void initializeState(Configuration configuration, String[] baseUrls, UserGroupInformation ugi, String doAsUser) {
        this.configuration = configuration;
        Client client = getClient(configuration, ugi, doAsUser);

        if ((!AuthenticationUtil.isKerberosAuthenticationEnabled()) && basicAuthUser != null && basicAuthPassword != null) {
            final HTTPBasicAuthFilter authFilter = new HTTPBasicAuthFilter(basicAuthUser, basicAuthPassword);
            client.addFilter(authFilter);
        }

        String activeServiceUrl = determineActiveServiceURL(baseUrls, client);
        atlasClientContext = new AtlasClientContext(baseUrls, client, ugi, doAsUser);
        service = client.resource(UriBuilder.fromUri(activeServiceUrl).build());
    }

    @VisibleForTesting
    protected Client getClient(Configuration configuration, UserGroupInformation ugi, String doAsUser) {
        DefaultClientConfig config = new DefaultClientConfig();
        // Enable POJO mapping feature
        config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        int readTimeout = configuration.getInt("atlas.client.readTimeoutMSecs", 60000);;
        int connectTimeout = configuration.getInt("atlas.client.connectTimeoutMSecs", 60000);
        if (configuration.getBoolean(TLS_ENABLED, false)) {
            // create an SSL properties configuration if one doesn't exist.  SSLFactory expects a file, so forced
            // to create a
            // configuration object, persist it, then subsequently pass in an empty configuration to SSLFactory
            try {
                SecureClientUtils.persistSSLClientConfiguration(configuration);
            } catch (Exception e) {
                LOG.info("Error processing client configuration.", e);
            }
        }

        final URLConnectionClientHandler handler;

        if ((!AuthenticationUtil.isKerberosAuthenticationEnabled()) && basicAuthUser != null && basicAuthPassword != null) {
            if (configuration.getBoolean(TLS_ENABLED, false)) {
                handler = SecureClientUtils.getUrlConnectionClientHandler();
            } else {
                handler = new URLConnectionClientHandler();
            }
        } else {
            handler = SecureClientUtils.getClientConnectionHandler(config, configuration, doAsUser, ugi);
        }
        Client client = new Client(handler, config);
        client.setReadTimeout(readTimeout);
        client.setConnectTimeout(connectTimeout);
        return client;
    }

    @VisibleForTesting
    protected String determineActiveServiceURL(String[] baseUrls, Client client) {
        if (baseUrls.length == 0) {
            throw new IllegalArgumentException("Base URLs cannot be null or empty");
        }
        final String baseUrl;
        AtlasServerEnsemble atlasServerEnsemble = new AtlasServerEnsemble(baseUrls);
        if (atlasServerEnsemble.hasSingleInstance()) {
            baseUrl = atlasServerEnsemble.firstURL();
            LOG.info("Client has only one service URL, will use that for all actions: {}", baseUrl);
        } else {
            try {
                baseUrl = selectActiveServerAddress(client, atlasServerEnsemble);
            } catch (AtlasServiceException e) {
                LOG.error("None of the passed URLs are active: {}", atlasServerEnsemble, e);
                throw new IllegalArgumentException("None of the passed URLs are active " + atlasServerEnsemble, e);
            }
        }
        return baseUrl;
    }

    private String selectActiveServerAddress(Client client, AtlasServerEnsemble serverEnsemble)
            throws AtlasServiceException {
        List<String> serverInstances = serverEnsemble.getMembers();
        String activeServerAddress = null;
        for (String serverInstance : serverInstances) {
            LOG.info("Trying with address {}", serverInstance);
            activeServerAddress = getAddressIfActive(client, serverInstance);
            if (activeServerAddress != null) {
                LOG.info("Found service {} as active service.", serverInstance);
                break;
            }
        }
        if (activeServerAddress != null)
            return activeServerAddress;
        else
            throw new AtlasServiceException(STATUS, new RuntimeException("Could not find any active instance"));
    }

    private String getAddressIfActive(Client client, String serverInstance) {
        String activeServerAddress = null;
        for (int i = 0; i < getNumberOfRetries(); i++) {
            try {
                service = client.resource(UriBuilder.fromUri(serverInstance).build());
                String adminStatus = getAdminStatus();
                if (StringUtils.equals(adminStatus, "ACTIVE")) {
                    activeServerAddress = serverInstance;
                    break;
                } else {
                    LOG.info("attempt #{}: Service {} - is not active. status={}", (i+1), serverInstance, adminStatus);
                }
            } catch (Exception e) {
                LOG.error("attempt #{}: Service {} - could not get status", (i+1), serverInstance, e);
            }
            sleepBetweenRetries();
        }
        return activeServerAddress;
    }

    protected Configuration getClientProperties() {
        try {
            if (configuration == null) {
                configuration = ApplicationProperties.get();
            }
        } catch (AtlasException e) {
            LOG.error("Exception while loading configuration.", e);
        }
        return configuration;
    }

    public boolean isServerReady() throws AtlasServiceException {
        WebResource resource = getResource(VERSION.getPath());
        try {
            callAPIWithResource(VERSION, resource, null, JSONObject.class);
            return true;
        } catch (ClientHandlerException che) {
            return false;
        } catch (AtlasServiceException ase) {
            if (ase.getStatus() != null && ase.getStatus().equals(ClientResponse.Status.SERVICE_UNAVAILABLE)) {
                LOG.warn("Received SERVICE_UNAVAILABLE, server is not yet ready");
                return false;
            }
            throw ase;
        }
    }

    protected WebResource getResource(String path, String... pathParams) {
        return getResource(service, path, pathParams);
    }

    protected <T> T callAPIWithResource(APIInfo api, WebResource resource, Object requestObject, Class<T> responseType) throws AtlasServiceException {
        ClientResponse clientResponse = null;
        int i = 0;
        do {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling API [ {} : {} ] {}", api.getMethod(), api.getPath(), requestObject != null ? "<== " + requestObject : "");
            }
            clientResponse = resource
                    .accept(JSON_MEDIA_TYPE)
                    .type(JSON_MEDIA_TYPE)
                    .method(api.getMethod(), ClientResponse.class, requestObject);

            if (LOG.isDebugEnabled()) {
                LOG.debug("API {} returned status {}", resource.getURI(), clientResponse.getStatus());
            }

            if (clientResponse.getStatus() == api.getExpectedStatus().getStatusCode()) {
                if (null == responseType) {
                    LOG.warn("No response type specified, returning null");
                    return null;
                }
                try {
                    if (responseType == JSONObject.class) {
                        String stringEntity = clientResponse.getEntity(String.class);
                        try {
                            JSONObject jsonObject = new JSONObject(stringEntity);
                            LOG.info("Response = {}", jsonObject);
                            return (T) jsonObject;
                        } catch (JSONException e) {
                            throw new AtlasServiceException(api, e);
                        }
                    } else {
                        T entity = clientResponse.getEntity(responseType);
                        LOG.info("Response = {}", AtlasType.toJson(entity));
                        return entity;
                    }
                } catch (ClientHandlerException e) {
                    throw new AtlasServiceException(api, e);
                }
            } else if (clientResponse.getStatus() != ClientResponse.Status.SERVICE_UNAVAILABLE.getStatusCode()) {
                break;
            } else {
                LOG.error("Got a service unavailable when calling: {}, will retry..", resource);
                sleepBetweenRetries();
            }

            i++;
        } while (i < getNumberOfRetries());

        throw new AtlasServiceException(api, clientResponse);
    }

    private WebResource getResource(WebResource service, String path, String... pathParams) {
        WebResource resource = service.path(path);
        resource = appendPathParams(resource, pathParams);
        return resource;
    }

    void sleepBetweenRetries() {
        try {
            Thread.sleep(getSleepBetweenRetriesMs());
        } catch (InterruptedException e) {
            LOG.error("Interrupted from sleeping between retries.", e);
        }
    }

    int getNumberOfRetries() {
        return configuration.getInt(AtlasBaseClient.ATLAS_CLIENT_HA_RETRIES_KEY, AtlasBaseClient.DEFAULT_NUM_RETRIES);
    }

    private int getSleepBetweenRetriesMs() {
        return configuration.getInt(AtlasBaseClient.ATLAS_CLIENT_HA_SLEEP_INTERVAL_MS_KEY, AtlasBaseClient.DEFAULT_SLEEP_BETWEEN_RETRIES_MS);
    }

    /**
     * Return status of the service instance the client is pointing to.
     *
     * @return One of the values in ServiceState.ServiceStateValue or {@link #UNKNOWN_STATUS} if
     * there is a JSON parse exception
     * @throws AtlasServiceException if there is a HTTP error.
     */
    public String getAdminStatus() throws AtlasServiceException {
        String result = AtlasBaseClient.UNKNOWN_STATUS;
        WebResource resource = getResource(service, STATUS.getPath());
        JSONObject response = callAPIWithResource(STATUS, resource, null, JSONObject.class);
        try {
            result = response.getString("Status");
        } catch (JSONException e) {
            LOG.error("Exception while parsing admin status response. Returned response {}", response.toString(), e);
        }
        return result;
    }

    boolean isRetryableException(ClientHandlerException che) {
        return che.getCause().getClass().equals(IOException.class)
                || che.getCause().getClass().equals(ConnectException.class);
    }

    void handleClientHandlerException(ClientHandlerException che) {
        if (isRetryableException(che)) {
            atlasClientContext.getClient().destroy();
            LOG.warn("Destroyed current context while handling ClientHandlerEception.");
            LOG.warn("Will retry and create new context.");
            sleepBetweenRetries();
            initializeState(atlasClientContext.getBaseUrls(), atlasClientContext.getUgi(),
                    atlasClientContext.getDoAsUser());
            return;
        }
        throw che;
    }

    @VisibleForTesting
    JSONObject callAPIWithRetries(APIInfo api, Object requestObject, ResourceCreator resourceCreator)
            throws AtlasServiceException {
        for (int i = 0; i < getNumberOfRetries(); i++) {
            WebResource resource = resourceCreator.createResource();
            try {
                LOG.debug("Using resource {} for {} times", resource.getURI(), i);
                return callAPIWithResource(api, resource, requestObject, JSONObject.class);
            } catch (ClientHandlerException che) {
                if (i == (getNumberOfRetries() - 1)) {
                    throw che;
                }
                LOG.warn("Handled exception in calling api {}", api.getPath(), che);
                LOG.warn("Exception's cause: {}", che.getCause().getClass());
                handleClientHandlerException(che);
            }
        }
        throw new AtlasServiceException(api, new RuntimeException("Could not get response after retries."));
    }

    public <T> T callAPI(APIInfo api, Object requestObject, Class<T> responseType, String... params)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, params), requestObject, responseType);
    }

    public <T> T callAPI(APIInfo api, Object requestBody, Class<T> responseType,
                         MultivaluedMap<String, String> queryParams, String... params) throws AtlasServiceException {
        WebResource resource = getResource(api, queryParams, params);
        return callAPIWithResource(api, resource, requestBody, responseType);
    }

    public <T> T callAPI(APIInfo api, Class<T> responseType, MultivaluedMap<String, String> queryParams, String... params)
            throws AtlasServiceException {
        WebResource resource = getResource(api, queryParams, params);
        return callAPIWithResource(api, resource, null, responseType);
    }

    protected WebResource getResource(APIInfo api, String... pathParams) {
        return getResource(service, api, pathParams);
    }

    // Modify URL to include the path params
    private WebResource getResource(WebResource service, APIInfo api, String... pathParams) {
        WebResource resource = service.path(api.getPath());
        resource = appendPathParams(resource, pathParams);
        return resource;
    }

    public <T> T callAPI(APIInfo api, Class<T> responseType, MultivaluedMap<String, String> queryParams)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, queryParams), null, responseType);
    }

    public <T> T callAPI(APIInfo api, Class<T> responseType, String queryParamKey, List<String> queryParamValues)
            throws AtlasServiceException {
        return callAPIWithResource(api, getResource(api, queryParamKey, queryParamValues), null, responseType);
    }

    private WebResource getResource(APIInfo api, String queryParamKey, List<String> queryParamValues) {
        WebResource resource = service.path(api.getPath());
        for (String queryParamValue : queryParamValues) {
            if (StringUtils.isNotBlank(queryParamKey) && StringUtils.isNotBlank(queryParamValue)) {
                resource = resource.queryParam(queryParamKey, queryParamValue);
            }
        }
        return resource;
    }

    protected WebResource getResource(APIInfo api, MultivaluedMap<String, String> queryParams, String ... pathParams) {
        WebResource resource = service.path(api.getPath());
        resource = appendPathParams(resource, pathParams);
        resource = appendQueryParams(queryParams, resource);
        return resource;
    }

    private WebResource appendPathParams(WebResource resource, String[] pathParams) {
        if (pathParams != null) {
            for (String pathParam : pathParams) {
                resource = resource.path(pathParam);
            }
        }
        return resource;
    }

    protected WebResource getResource(APIInfo api, MultivaluedMap<String, String> queryParams) {
        return getResource(service, api, queryParams);
    }

    // Modify URL to include the query params
    private WebResource getResource(WebResource service, APIInfo api, MultivaluedMap<String, String> queryParams) {
        WebResource resource = service.path(api.getPath());
        resource = appendQueryParams(queryParams, resource);
        return resource;
    }

    private WebResource appendQueryParams(MultivaluedMap<String, String> queryParams, WebResource resource) {
        if (null != queryParams && !queryParams.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
                for (String value : entry.getValue()) {
                    if (StringUtils.isNotBlank(value)) {
                        resource = resource.queryParam(entry.getKey(), value);
                    }
                }
            }
        }
        return resource;
    }

    protected APIInfo formatPathForPathParams(APIInfo apiInfo, String ... params) {
        return new APIInfo(String.format(apiInfo.getPath(), params), apiInfo.getMethod(), apiInfo.getExpectedStatus());
    }

    @VisibleForTesting
    void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @VisibleForTesting
    void setService(WebResource resource) {
        this.service = resource;
    }


    public static class APIInfo {
        private final String method;
        private final String path;
        private final Response.Status status;

        public APIInfo(String path, String method, Response.Status status) {
            this.path = path;
            this.method = method;
            this.status = status;
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }

        public Response.Status getExpectedStatus() {
            return status;
        }
    }

    /**
     * A class to capture input state while creating the client.
     *
     * The information here will be reused when the client is re-initialized on switch-over
     * in case of High Availability.
     */
    private class AtlasClientContext {
        private String[] baseUrls;
        private Client client;
        private String doAsUser;
        private UserGroupInformation ugi;

        public AtlasClientContext(String[] baseUrls, Client client, UserGroupInformation ugi, String doAsUser) {
            this.baseUrls = baseUrls;
            this.client = client;
            this.ugi = ugi;
            this.doAsUser = doAsUser;
        }

        public Client getClient() {
            return client;
        }

        public String[] getBaseUrls() {
            return baseUrls;
        }

        public String getDoAsUser() {
            return doAsUser;
        }

        public UserGroupInformation getUgi() {
            return ugi;
        }
    }
}
