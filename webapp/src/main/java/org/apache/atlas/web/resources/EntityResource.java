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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.aspect.Monitored;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TraitNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.utils.ParamChecker;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Entity management operations as REST API.
 *
 * An entity is an "instance" of a Type.  Entities conform to the definition
 * of the Type they correspond with.
 */
@Path("entities")
@Singleton
public class EntityResource {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResource.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.EntityResource");

    private static final String TRAIT_NAME = "traitName";

    private final MetadataService metadataService;

    @Context
    UriInfo uriInfo;

    /**
     * Created by the Guice ServletModule and injected with the
     * configured MetadataService.
     *
     * @param metadataService metadata service handle
     */
    @Inject
    public EntityResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    /**
     * Submits the entity definitions (instances).
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @Monitored
    @POST
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response submit(@Context HttpServletRequest request) {
        String entityJson = null;
        try {
            String entities = Servlets.getRequestPayload(request);

            //Handle backward compatibility - if entities is not JSONArray, convert to JSONArray
            try {
                new JSONArray(entities);
            } catch (JSONException e) {
                final String finalEntities = entities;
                entities = new JSONArray() {{
                    put(finalEntities);
                }}.toString();
            }

            entityJson = AtlasClient.toString(new JSONArray(entities));
            LOG.info("submitting entities {} ", entityJson);

            final List<String> guids = metadataService.createEntities(entities);
            LOG.info("Created entities {}", guids);
            JSONObject response = getResponse(new AtlasClient.EntityResult(guids, null, null));

            URI locationURI = getLocationURI(guids);

            return Response.created(locationURI).entity(response).build();

        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation for entity entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a deserialization error entityDef={}", entityJson, ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }


    @VisibleForTesting
    public URI getLocationURI(List<String> guids) {
        URI locationURI = null;
        if (uriInfo != null) {
            UriBuilder ub = uriInfo.getAbsolutePathBuilder();
            locationURI = guids.isEmpty() ? null : ub.path(guids.get(0)).build();
        } else {
            String uriPath = AtlasClient.API.GET_ENTITY.getPath();
            locationURI = guids.isEmpty() ? null : UriBuilder
                .fromPath(AtlasConstants.DEFAULT_ATLAS_REST_ADDRESS)
                .path(uriPath).path(guids.get(0)).build();

        }
        return locationURI;
    }

    private JSONObject getResponse(AtlasClient.EntityResult entityResult) throws AtlasException, JSONException {
        JSONObject response = new JSONObject();
        response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
        response.put(AtlasClient.ENTITIES, new JSONObject(entityResult.toString()).get(AtlasClient.ENTITIES));
        String sampleEntityId = getSample(entityResult);
        if (sampleEntityId != null) {
            String entityDefinition = metadataService.getEntityDefinitionJson(sampleEntityId);
            response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
        }
        return response;
    }

    /**
     * Complete update of a set of entities - the values not specified will be replaced with null/removed
     * Adds/Updates given entities identified by its GUID or unique attribute
     * @return response payload as json
     */
    @Monitored
    @PUT
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntities(@Context HttpServletRequest request) {
        String entityJson = null;
        try {
            final String entities = Servlets.getRequestPayload(request);

            entityJson = AtlasClient.toString(new JSONArray(entities));
            LOG.info("updating entities {} ", entityJson);

            AtlasClient.EntityResult entityResult = metadataService.updateEntities(entities);
            LOG.info("Updated entities: {}", entityResult);

            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation for entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a deserialization error entityDef={}", entityJson, ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to persist entity instance entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to persist entity instance entityDef={}", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private String getSample(AtlasClient.EntityResult entityResult) {
        String sample = getSample(entityResult.getCreatedEntities());
        if (sample == null) {
            sample = getSample(entityResult.getUpdateEntities());
        }
        return sample;
    }


    private String getSample(List<String> list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

    /**
     * Adds/Updates given entity identified by its unique attribute( entityType, attributeName and value)
     * Updates support only partial update of an entity - Adds/updates any new values specified
     * Updates do not support removal of attribute values
     *
     * @param entityType the entity type
     * @param attribute the unique attribute used to identify the entity
     * @param value the unique attributes value
     * @param request The updated entity json
     * @return response payload as json
     * The body contains the JSONArray of entity json. The service takes care of de-duping the entities based on any
     * unique attribute for the give type.
     */
    @Monitored
    @POST
    @Path("qualifiedName")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateByUniqueAttribute(@QueryParam("type") String entityType,
                                            @QueryParam("property") String attribute,
                                            @QueryParam("value") String value, @Context HttpServletRequest request) {
        String entityJson = null;
        try {
            entityJson = Servlets.getRequestPayload(request);

            LOG.info("Partially updating entity by unique attribute {} {} {} {} ", entityType, attribute, value, entityJson);

            Referenceable updatedEntity =
                InstanceSerialization.fromJsonReferenceable(entityJson, true);

            AtlasClient.EntityResult entityResult =
                    metadataService.updateEntityByUniqueAttribute(entityType, attribute, value, updatedEntity);
            LOG.info("Updated entities: {}", entityResult);

            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (ValueConversionException ve) {
            LOG.error("Unable to persist entity instance due to a deserialization error {} ", entityJson, ve);
            throw new WebApplicationException(Servlets.getErrorResponse(ve.getCause(), Response.Status.BAD_REQUEST));
        } catch(EntityExistsException e) {
            LOG.error("Unique constraint violation for entity {} ", entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.CONFLICT));
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with type={} and qualifiedName={} does not exist {} ", entityType, value, entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to partially update entity {} {}:{}.{}", entityJson, entityType, attribute, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to partially update entity {} {}:{}.{}", entityJson, entityType, attribute, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Updates entity identified by its GUID
     * Support Partial update of an entity - Adds/updates any new values specified
     * Does not support removal of attribute values
     *
     * @param guid
     * @param request The updated entity json
     * @return
     */
    @POST
    @Path("{guid}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response updateEntityByGuid(@PathParam("guid") String guid, @QueryParam("property") String attribute,
                                       @Context HttpServletRequest request) {
        if (StringUtils.isEmpty(attribute)) {
            return updateEntityPartialByGuid(guid, request);
        } else {
            return updateEntityAttributeByGuid(guid, attribute, request);
        }
    }

    @Monitored
    private Response updateEntityPartialByGuid(String guid, HttpServletRequest request) {
        String entityJson = null;
        try {
            guid = ParamChecker.notEmpty(guid, "Guid property cannot be null");
            entityJson = Servlets.getRequestPayload(request);
            LOG.info("partially updating entity for guid {} : {} ", guid, entityJson);

            Referenceable updatedEntity =
                    InstanceSerialization.fromJsonReferenceable(entityJson, true);

            AtlasClient.EntityResult entityResult = metadataService.updateEntityPartialByGuid(guid, updatedEntity);
            LOG.info("Updated entities: {}", entityResult);

            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist {} ", guid, entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to update entity by GUID {} {}", guid, entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to update entity by GUID {} {} ", guid, entityJson, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Supports Partial updates
     * Adds/Updates given entity specified by its GUID
     * Supports updation of only simple primitive attributes like strings, ints, floats, enums, class references and
     * does not support updation of complex types like arrays, maps
     * @param guid entity id
     * @param property property to add
     * @postbody property's value
     * @return response payload as json
     */
    @Monitored
    private Response updateEntityAttributeByGuid(String guid, String property, HttpServletRequest request) {
        String value = null;
        try {
            Preconditions.checkNotNull(property, "Entity property cannot be null");
            value = Servlets.getRequestPayload(request);
            Preconditions.checkNotNull(value, "Entity value cannot be null");

            LOG.info("Updating entity {} for property {} = {}", guid, property, value);
            AtlasClient.EntityResult entityResult = metadataService.updateEntityAttributeByGuid(guid, property, value);
            LOG.info("Updated entities: {}", entityResult);

            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist {} ", guid, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add property {} to entity id {} {} ", property, guid, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add property {} to entity id {} {} ", property, guid, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Delete entities from the repository identified by their guids (including their composite references)
     * or
     * Deletes a single entity identified by its type and unique attribute value from the repository (including their composite references)
     * 
     * @param guids list of deletion candidate guids
     *              or
     * @param entityType the entity type
     * @param attribute the unique attribute used to identify the entity
     * @param value the unique attribute value used to identify the entity
     * @return response payload as json - including guids of entities(including composite references from that entity) that were deleted
     */
    @Monitored
    @DELETE
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteEntities(@QueryParam("guid") List<String> guids,
        @QueryParam("type") String entityType,
        @QueryParam("property") String attribute,
        @QueryParam("value") String value) {
        try {
            AtlasClient.EntityResult entityResult;
            if (guids != null && !guids.isEmpty()) {
                LOG.info("Deleting entities {}", guids);
                entityResult = metadataService.deleteEntities(guids);
            } else {
                LOG.info("Deleting entity type={} with property {}={}", entityType, attribute, value);
                entityResult = metadataService.deleteEntityByUniqueAttribute(entityType, attribute, value);
            }
            LOG.info("Deleted entity result: {}", entityResult);
            JSONObject response = getResponse(entityResult);
            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            if(guids != null && !guids.isEmpty()) {
                LOG.error("An entity with GUID={} does not exist ", guids, e);
            } else {
                LOG.error("An entity with qualifiedName {}-{}-{} does not exist", entityType, attribute, value, e);
            }
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        }  catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to delete entities {} {} {} {} ", guids, entityType, attribute, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to delete entities {} {} {} {} ", guids, entityType, attribute, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetch the complete definition of an entity given its GUID.
     *
     * @param guid GUID for the entity
     */
    @Monitored
    @GET
    @Path("{guid}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntityDefinition(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching entity definition for guid={} ", guid);
            guid = ParamChecker.notEmpty(guid, "guid cannot be null");
            final String entityDefinition = metadataService.getEntityDefinitionJson(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                status = Response.Status.OK;
            } else {
                response.put(AtlasClient.ERROR,
                        Servlets.escapeJsonString(String.format("An entity with GUID={%s} does not exist", guid)));
            }

            return Response.status(status).entity(response).build();

        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist ", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Bad GUID={} ", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get instance definition for GUID {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Gets the list of entities for a given entity type.
     *
     * @param entityType name of a type which is unique
     */
    @Monitored
    public Response getEntityListByType(String entityType) {
        try {
            Preconditions.checkNotNull(entityType, "Entity type cannot be null");

            LOG.debug("Fetching entity list for type={} ", entityType);
            final List<String> entityList = metadataService.getEntityList(entityType);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.TYPENAME, entityType);
            response.put(AtlasClient.RESULTS, new JSONArray(entityList));
            response.put(AtlasClient.COUNT, entityList.size());

            return Response.ok(response).build();
        } catch (NullPointerException e) {
            LOG.error("Entity type cannot be null", e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get entity list for type {}", entityType, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    @GET
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getEntity(@QueryParam("type") String entityType,
                              @QueryParam("property") String attribute,
                              @QueryParam("value") String value) {
        if (StringUtils.isEmpty(attribute)) {
            //List API
            return getEntityListByType(entityType);
        } else {
            //Get entity by unique attribute
            return getEntityDefinitionByAttribute(entityType, attribute, value);
        }
    }

    /**
     * Fetch the complete definition of an entity given its qualified name.
     *
     * @param entityType
     * @param attribute
     * @param value
     */
    @Monitored
    public Response getEntityDefinitionByAttribute(String entityType, String attribute, String value) {
        try {
            LOG.debug("Fetching entity definition for type={}, qualified name={}", entityType, value);
            entityType = ParamChecker.notEmpty(entityType, "Entity type cannot be null");
            attribute  = ParamChecker.notEmpty(attribute, "attribute name cannot be null");
            value      = ParamChecker.notEmpty(value, "attribute value cannot be null");

            final String entityDefinition = metadataService.getEntityDefinition(entityType, attribute, value);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            Response.Status status = Response.Status.NOT_FOUND;
            if (entityDefinition != null) {
                response.put(AtlasClient.DEFINITION, new JSONObject(entityDefinition));
                status = Response.Status.OK;
            } else {
                response.put(AtlasClient.ERROR, Servlets.escapeJsonString(String.format("An entity with type={%s}, " +
                        "qualifiedName={%s} does not exist", entityType, value)));
            }

            return Response.status(status).entity(response).build();

        } catch (EntityNotFoundException e) {
            LOG.error("An entity with type={} and qualifiedName={} does not exist", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Bad type={}, qualifiedName={}", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get instance definition for type={}, qualifiedName={}", entityType, value, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }


    // Trait management functions

    /**
     * Gets the list of trait names for a given entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     * @return a list of trait names for the given entity guid
     */
    @Monitored
    @GET
    @Path("{guid}/traits")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTraitNames(@PathParam("guid") String guid) {
        try {
            LOG.debug("Fetching trait names for entity={}", guid);
            final List<String> traitNames = metadataService.getTraitNames(guid);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONArray(traitNames));
            response.put(AtlasClient.COUNT, traitNames.size());

            return Response.ok(response).build();
        } catch (EntityNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get trait names for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetches the trait definitions of all the traits associated to the given entity
     * @param guid globally unique identifier for the entity
     */
    @Monitored
    @GET
    @Path("{guid}/traitDefinitions")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTraitDefinitionsForEntity(@PathParam("guid") String guid){
        try {
            LOG.debug("Fetching all trait definitions for entity={}", guid);
            final String entityDefinition = metadataService.getEntityDefinitionJson(guid);

            Referenceable entity = InstanceSerialization.fromJsonReferenceable(entityDefinition, true);
            JSONArray traits = new JSONArray();
            for (String traitName : entity.getTraits()) {
                IStruct trait = entity.getTrait(traitName);
                traits.put(new JSONObject(InstanceSerialization.toJson(trait, true)));
            }

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, traits);
            response.put(AtlasClient.COUNT, traits.length());

            return Response.ok(response).build();
        } catch (EntityNotFoundException e){
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get trait definitions for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get trait definitions for entity {}", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Fetches the trait definition for an entity given its guid and trait name
     *
     * @param guid globally unique identifier for the entity
     * @param traitName name of the trait
     */
    @Monitored
    @GET
    @Path("{guid}/traitDefinitions/{traitName}")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getTraitDefinitionForEntity(@PathParam("guid") String guid, @PathParam("traitName") String traitName){
        try {
            LOG.debug("Fetching trait definition for entity {} and trait name {}", guid, traitName);
            final IStruct traitDefinition = metadataService.getTraitDefinition(guid, traitName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.RESULTS, new JSONObject(InstanceSerialization.toJson(traitDefinition, true)));

            return Response.ok(response).build();

        } catch (EntityNotFoundException e){
            LOG.error("An entity with GUID={} does not exist", guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get trait definition for entity {} and trait {}", guid, traitName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get trait definition for entity {} and trait {}", guid, traitName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Adds a new trait to an existing entity represented by a guid.
     *
     * @param guid globally unique identifier for the entity
     */
    @Monitored
    @POST
    @Path("{guid}/traits")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response addTrait(@Context HttpServletRequest request, @PathParam("guid") final String guid) {
        String traitDefinition = null;
        try {
            traitDefinition = Servlets.getRequestPayload(request);
            LOG.info("Adding trait={} for entity={} ", traitDefinition, guid);
            metadataService.addTrait(guid, traitDefinition);

            URI locationURI = getLocationURI(new ArrayList<String>() {{
                add(guid);
            }});

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());

            return Response.created(locationURI).entity(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist traitDef={} ", guid, traitDefinition, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to add trait for entity={} traitDef={}", guid, traitDefinition, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to add trait for entity={} traitDef={}", guid, traitDefinition, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Deletes a given trait from an existing entity represented by a guid.
     *
     * @param guid      globally unique identifier for the entity
     * @param traitName name of the trait
     */
    @Monitored
    @DELETE
    @Path("{guid}/traits/{traitName}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response deleteTrait(@Context HttpServletRequest request, @PathParam("guid") String guid,
            @PathParam(TRAIT_NAME) String traitName) {
        LOG.info("Deleting trait={} from entity={} ", traitName, guid);
        try {
            metadataService.deleteTrait(guid, traitName);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(TRAIT_NAME, traitName);

            return Response.ok(response).build();
        } catch (EntityNotFoundException | TypeNotFoundException e) {
            LOG.error("An entity with GUID={} does not exist traitName={} ", guid, traitName, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (TraitNotFoundException e) {
            LOG.error("The trait name={} for entity={} does not exist.", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.NOT_FOUND));
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to delete trait name={} for entity={}", traitName, guid, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Returns the entity audit events for a given entity id. The events are returned in the decreasing order of timestamp.
     * @param guid entity id
     * @param startKey used for pagination. Startkey is inclusive, the returned results contain the event with the given startkey.
     *                  First time getAuditEvents() is called for an entity, startKey should be null,
     *                  with count = (number of events required + 1). Next time getAuditEvents() is called for the same entity,
     *                  startKey should be equal to the entityKey of the last event returned in the previous call.
     * @param count number of events required
     * @return
     */
    @Monitored
    @GET
    @Path("{guid}/audit")
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public Response getAuditEvents(@PathParam("guid") String guid, @QueryParam("startKey") String startKey,
                                   @QueryParam("count") @DefaultValue("100") short count) {
        LOG.debug("Audit events request for entity {}, start key {}, number of results required {}", guid, startKey,
                count);
        try {
            List<EntityAuditEvent> events = metadataService.getAuditEvents(guid, startKey, count);

            JSONObject response = new JSONObject();
            response.put(AtlasClient.REQUEST_ID, Servlets.getRequestId());
            response.put(AtlasClient.EVENTS, getJSONArray(events));
            return Response.ok(response).build();
        } catch (AtlasException | IllegalArgumentException e) {
            LOG.error("Unable to get audit events for entity guid={} startKey={}", guid, startKey, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.BAD_REQUEST));
        } catch (Throwable e) {
            LOG.error("Unable to get audit events for entity guid={} startKey={}", guid, startKey, e);
            throw new WebApplicationException(Servlets.getErrorResponse(e, Response.Status.INTERNAL_SERVER_ERROR));
        }
    }

    private <T> JSONArray getJSONArray(Collection<T> elements) throws JSONException {
        JSONArray jsonArray = new JSONArray();
        for(T element : elements) {
            jsonArray.put(new JSONObject(element.toString()));
        }
        return jsonArray;
    }
}
