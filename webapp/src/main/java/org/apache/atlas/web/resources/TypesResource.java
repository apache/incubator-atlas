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

package org.apache.atlas.web.resources;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.core.ResourceContext;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.aspect.Monitored;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.util.RestUtils;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.rest.TypesREST;
import org.apache.atlas.web.util.Servlets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * This class provides RESTful API for Types.
 *
 * A type is the description of any representable item;
 * e.g. a Hive table
 *
 * You could represent any meta model representing any domain using these types.
 */
@Path("types")
@Singleton
public class TypesResource {
    private static final Logger LOG = LoggerFactory.getLogger(TypesResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.TypesResource");
    private static AtlasTypeRegistry typeRegistry;

    @Inject
    public TypesResource(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Context
    private ResourceContext resourceContext;

    /**
     * Submits a type definition corresponding to a given type representing a meta model of a
     * domain. Could represent things like Hive Database, Hive Table, etc.
     */
    @Monitored
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response submit(@Context HttpServletRequest request) {
        TypesREST typesRest = resourceContext.getResource(TypesREST.class);
        JSONArray typesResponse = new JSONArray();

        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.info("Creating type with definition {} ", typeDefinition);

            AtlasTypesDef createTypesDef  = RestUtils.toAtlasTypesDef(typeDefinition, typeRegistry);
            AtlasTypesDef createdTypesDef = typesRest.createAtlasTypeDefs(createTypesDef);
            List<String>  typeNames       = RestUtils.getTypeNames(createdTypesDef);

            for (int i = 0; i < typeNames.size(); i++) {
                final String name = typeNames.get(i);
                typesResponse.put(new JSONObject() {{
                    put(AtlasClient.NAME, name);
                }});
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPES, typesResponse);
            return Response.status(ClientResponse.Status.CREATED).entity(response).build();
        } catch (AtlasBaseException e) {
            LOG.error("Type creation failed", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, e.getAtlasErrorCode().getHttpCode()));
        } catch (IllegalArgumentException e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to persist types due to V2 API error", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Update of existing types - if the given type doesn't exist, creates new type
     * Allowed updates are:
     * 1. Add optional attribute
     * 2. Change required to optional attribute
     * 3. Add super types - super types shouldn't contain any required attributes
     * @param request
     * @return
     */
    @Monitored
    @PUT
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response update(@Context HttpServletRequest request) {
        TypesREST typesRest = resourceContext.getResource(TypesREST.class);
        JSONArray typesResponse = new JSONArray();
        try {
            final String typeDefinition = Servlets.getRequestPayload(request);
            LOG.info("Updating type with definition {} ", typeDefinition);

            AtlasTypesDef updateTypesDef  = RestUtils.toAtlasTypesDef(typeDefinition, typeRegistry);
            AtlasTypesDef updatedTypesDef = typesRest.updateAtlasTypeDefs(updateTypesDef);
            List<String>  typeNames       = RestUtils.getTypeNames(updatedTypesDef);

            for (int i = 0; i < typeNames.size(); i++) {
                final String name = typeNames.get(i);
                typesResponse.put(new JSONObject() {{
                    put(AtlasClient.NAME, name);
                }});
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPES, typesResponse);
            return Response.ok().entity(response).build();
        } catch (TypeExistsException e) {
            LOG.error("Type already exists", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (WebApplicationException e) {
            LOG.error("Unable to persist types due to V2 API error", e);
            throw e;
        } catch (Throwable e) {
            LOG.error("Unable to persist types", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetch the complete definition of a given type name which is unique.
     *
     * @param typeName name of a type which is unique.
     */
    @Monitored
    @GET
    @Path("{typeName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getDefinition(@Context HttpServletRequest request, @PathParam("typeName") String typeName) {
        TypesREST typesRest = resourceContext.getResource(TypesREST.class);
        JSONObject response = new JSONObject();

        try {
            TypeCategory typeCategory = typeRegistry.getType(typeName).getTypeCategory();
            TypesDef typesDef = null;

            if (typeCategory != null) {
                switch (typeCategory) {
                    case ENUM:
                        AtlasEnumDef enumDef = typesRest.getEnumDefByName(typeName);
                        typesDef = RestUtils.toTypesDef(enumDef);
                        break;
                    case STRUCT:
                        AtlasStructDef structDef = typesRest.getStructDefByName(typeName);
                        typesDef = RestUtils.toTypesDef(structDef, typeRegistry);
                        break;
                    case ENTITY:
                        AtlasEntityDef entityDef = typesRest.getEntityDefByName(typeName);
                        typesDef = RestUtils.toTypesDef(entityDef, typeRegistry);
                        break;
                    case CLASSIFICATION:
                        AtlasClassificationDef classificationDef = typesRest.getClassificationDefByName(typeName);
                        typesDef = RestUtils.toTypesDef(classificationDef, typeRegistry);
                        break;
                    default:
                        typesDef = new TypesDef();
                        break;
                }
            }

            final String typeDefinition = TypesSerialization.toJson(typesDef);
            response.put(AtlasClient.TYPENAME, typeName);
            response.put(AtlasClient.DEFINITION, new JSONObject(typeDefinition));
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (AtlasBaseException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (JSONException | IllegalArgumentException e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get type definition for type {}", typeName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Return the list of type names in the type system which match the specified filter.
     *
     * @return list of type names
     * @param typeCategory returns types whose category is the given typeCategory
     * @param supertype returns types which contain the given supertype
     * @param notsupertype returns types which do not contain the given supertype
     *
     * Its possible to specify combination of these filters in one request and the conditions are combined with AND
     * For example, typeCategory = TRAIT && supertype contains 'X' && supertype !contains 'Y'
     * If there is no filter, all the types are returned
     */
    @Monitored
    @GET
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTypesByFilter(@Context HttpServletRequest request, @QueryParam("type") String typeCategory,
                                     @QueryParam("supertype") String supertype,
                                     @QueryParam("notsupertype") String notsupertype) throws AtlasBaseException {
        TypesREST typesRest  = resourceContext.getResource(TypesREST.class);
        JSONObject response  = new JSONObject();
        try {
            List<String> result = RestUtils.getTypeNames(typesRest.getTypeDefHeaders());

            response.put(AtlasClient.RESULTS, new JSONArray(result));
            response.put(AtlasClient.COUNT, result.size());
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.ok(response).build();
        } catch (AtlasBaseException e) {
            LOG.error("Given search filter did not yield any results");
            throw new WebApplicationException(
                    Servlets.getErrorResponse(new Exception("Given search filter did not yield any results "), Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get types list", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }
}
