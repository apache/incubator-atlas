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

package org.apache.atlas.repository.graph;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.aspect.Monitored;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Utility class for graph operations.
 */
public final class GraphHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphHelper.class);
    public static final String EDGE_LABEL_PREFIX = "__";

    private static final TypeSystem typeSystem = TypeSystem.getInstance();

    public static final String RETRY_COUNT = "atlas.graph.storage.num.retries";
    public static final String RETRY_DELAY = "atlas.graph.storage.retry.sleeptime.ms";

    private static volatile GraphHelper INSTANCE;

    private AtlasGraph graph;
    private static int maxRetries;
    public static long retrySleepTimeMillis;

    @VisibleForTesting
    GraphHelper(AtlasGraph graph) {
        this.graph = graph;
        try {
            maxRetries = ApplicationProperties.get().getInt(RETRY_COUNT, 3);
            retrySleepTimeMillis = ApplicationProperties.get().getLong(RETRY_DELAY, 1000);
        } catch (AtlasException e) {
            LOG.error("Could not load configuration. Setting to default value for " + RETRY_COUNT, e);
        }
    }

    public static GraphHelper getInstance() {
        if ( INSTANCE == null) {
            synchronized (GraphHelper.class) {
                if (INSTANCE == null) {
                    INSTANCE = new GraphHelper(AtlasGraphProvider.getGraphInstance());
                }
            }
        }
        return INSTANCE;
    }

    @VisibleForTesting
    static GraphHelper getInstance(AtlasGraph graph) {
        if ( INSTANCE == null) {
            synchronized (GraphHelper.class) {
                if (INSTANCE == null) {
                    INSTANCE = new GraphHelper(graph);
                }
            }
        }
        return INSTANCE;
    }


    public AtlasVertex createVertexWithIdentity(ITypedReferenceableInstance typedInstance, Set<String> superTypeNames) {
        final String guid = UUID.randomUUID().toString();

        final AtlasVertex vertexWithIdentity = createVertexWithoutIdentity(typedInstance.getTypeName(),
                new Id(guid, 0, typedInstance.getTypeName()), superTypeNames);

        // add identity
        setProperty(vertexWithIdentity, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        setProperty(vertexWithIdentity, Constants.VERSION_PROPERTY_KEY, typedInstance.getId().version);

        return vertexWithIdentity;
    }

    @Monitored
    public AtlasVertex createVertexWithoutIdentity(String typeName, Id typedInstanceId, Set<String> superTypeNames) {
        LOG.debug("Creating AtlasVertex for type {} id {}", typeName,
                typedInstanceId != null ? typedInstanceId._getId() : null);
        final AtlasVertex vertexWithoutIdentity = graph.addVertex();

        // add type information
        setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);


        // add super types
        for (String superTypeName : superTypeNames) {
            addProperty(vertexWithoutIdentity, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        // add state information
        setProperty(vertexWithoutIdentity, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

        // add timestamp information
        setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());

        setProperty(vertexWithoutIdentity, Constants.CREATED_BY_KEY, RequestContext.get().getUser());
        setProperty(vertexWithoutIdentity, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());
        return vertexWithoutIdentity;
    }

    @Monitored
    private AtlasEdge addEdge(AtlasVertex fromVertex, AtlasVertex toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> label {} -> {}", string(fromVertex), edgeLabel, string(toVertex));
        AtlasEdge edge = graph.addEdge(fromVertex, toVertex, edgeLabel);

        setProperty(edge, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        setProperty(edge, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(edge, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(edge, Constants.CREATED_BY_KEY, RequestContext.get().getUser());
        setProperty(edge, Constants.MODIFIED_BY_KEY, RequestContext.get().getUser());

        LOG.debug("Added {}", string(edge));
        return edge;
    }

    public AtlasEdge getOrCreateEdge(AtlasVertex outVertex, AtlasVertex inVertex, String edgeLabel) throws RepositoryException {
        for (int numRetries = 0; numRetries < maxRetries; numRetries++) {
            try {
                LOG.debug("Running edge creation attempt {}", numRetries);
                Iterator<AtlasEdge> edges = getAdjacentEdgesByLabel(inVertex, AtlasEdgeDirection.IN, edgeLabel);

                while (edges.hasNext()) {
                    AtlasEdge edge = edges.next();
                    if (edge.getOutVertex().equals(outVertex)) {
                        Id.EntityState edgeState = getState(edge);
                        if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                            return edge;
                        }
                    }
                }

                return addEdge(outVertex, inVertex, edgeLabel);
            } catch (Exception e) {
                LOG.warn(String.format("Exception while trying to create edge from %s to %s with label %s. Retrying",
                        vertexString(outVertex), vertexString(inVertex), edgeLabel), e);
                if (numRetries == (maxRetries - 1)) {
                    LOG.error("Max retries exceeded for edge creation {} {} {} ", outVertex, inVertex, edgeLabel, e);
                    throw new RepositoryException("Edge creation failed after retries", e);
                }

                try {
                    LOG.info("Retrying with delay of {} ms ", retrySleepTimeMillis);
                    Thread.sleep(retrySleepTimeMillis);
                } catch(InterruptedException ie) {
                    LOG.warn("Retry interrupted during edge creation ");
                    throw new RepositoryException("Retry interrupted during edge creation", ie);
                }
            }
        }
        return null;
    }

    @Monitored
    public AtlasEdge getEdgeByEdgeId(AtlasVertex outVertex, String edgeLabel, String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return graph.getEdge(edgeId);

        //TODO get edge id is expensive. Use this logic. But doesn't work for now
        /**
        Iterable<AtlasEdge> edges = outVertex.getEdges(Direction.OUT, edgeLabel);
        for (AtlasEdge edge : edges) {
            if (edge.getId().toString().equals(edgeId)) {
                return edge;
            }
        }
        return null;
         **/
    }

    /**
     * Args of the format prop1, key1, prop2, key2...
     * Searches for a AtlasVertex with prop1=key1 && prop2=key2
     * @param args
     * @return AtlasVertex with the given property keys
     * @throws EntityNotFoundException
     */
    @Monitored
    public AtlasVertex findVertex(Object... args) throws EntityNotFoundException {
        StringBuilder condition = new StringBuilder();
        AtlasGraphQuery query = graph.query();
        for (int i = 0 ; i < args.length; i+=2) {
            query = query.has((String) args[i], args[i+1]);
            condition.append(args[i]).append(" = ").append(args[i+1]).append(", ");
        }
        String conditionStr = condition.toString();
        LOG.debug("Finding AtlasVertex with {}", conditionStr);

        Iterator<AtlasVertex> results = query.vertices().iterator();
        // returning one since entityType, qualifiedName should be unique
        AtlasVertex vertex = results.hasNext() ? results.next() : null;

        if (vertex == null) {
            LOG.debug("Could not find a vertex with {}", condition.toString());
            throw new EntityNotFoundException("Could not find an entity in the repository with " + conditionStr);
        } else {
            LOG.debug("Found a vertex {} with {}", string(vertex), conditionStr);
        }

        return vertex;
    }

    //In some cases of parallel APIs, the edge is added, but get edge by label doesn't return the edge. ATLAS-1104
    //So traversing all the edges
    @Monitored
    public Iterator<AtlasEdge> getAdjacentEdgesByLabel(AtlasVertex instanceVertex, AtlasEdgeDirection direction, final String edgeLabel) {
        LOG.debug("Finding edges for {} with label {}", string(instanceVertex), edgeLabel);
        if(instanceVertex != null && edgeLabel != null) {
            final Iterator<AtlasEdge> iterator = instanceVertex.getEdges(direction).iterator();
            return new Iterator<AtlasEdge>() {
                private AtlasEdge edge = null;

                @Override
                public boolean hasNext() {
                    while (edge == null && iterator.hasNext()) {
                        AtlasEdge localEdge = iterator.next();
                        if (localEdge.getLabel().equals(edgeLabel)) {
                            edge = localEdge;
                        }
                    }
                    return edge != null;
                }

                @Override
                public AtlasEdge next() {
                    if (hasNext()) {
                        AtlasEdge localEdge = edge;
                        edge = null;
                        return localEdge;
                    }
                    return null;
                }

                @Override
                public void remove() {
                    throw new IllegalStateException("Not handled");
                }
            };
        }
        return null;
    }

    public Iterator<AtlasEdge> getOutGoingEdgesByLabel(AtlasVertex instanceVertex, String edgeLabel) {
        return getAdjacentEdgesByLabel(instanceVertex, AtlasEdgeDirection.OUT, edgeLabel);
    }

    /**
     * Returns the active edge for the given edge label.
     * If the vertex is deleted and there is no active edge, it returns the latest deleted edge
     * @param vertex
     * @param edgeLabel
     * @return
     */
    @Monitored
    public AtlasEdge getEdgeForLabel(AtlasVertex vertex, String edgeLabel) {
        Iterator<AtlasEdge> iterator = getAdjacentEdgesByLabel(vertex, AtlasEdgeDirection.OUT, edgeLabel);
        AtlasEdge latestDeletedEdge = null;
        long latestDeletedEdgeTime = Long.MIN_VALUE;

        while (iterator != null && iterator.hasNext()) {
            AtlasEdge edge = iterator.next();
            Id.EntityState edgeState = getState(edge);
            if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                LOG.debug("Found {}", string(edge));
                return edge;
            } else {
                Long modificationTime = edge.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
                if (modificationTime != null && modificationTime >= latestDeletedEdgeTime) {
                    latestDeletedEdgeTime = modificationTime;
                    latestDeletedEdge = edge;
                }
            }
        }
        //If the vertex is deleted, return latest deleted edge
        LOG.debug("Found {}", latestDeletedEdge == null ? "null" : string(latestDeletedEdge));
        return latestDeletedEdge;
    }

    @Monitored
    public static String vertexString(final AtlasVertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            Collection<?> propertyValues = vertex.getPropertyValues(propertyKey, Object.class);
            properties.append(propertyKey).append("=").append(propertyValues.toString()).append(", ");
        }

        return "v[" + vertex.getIdForDisplay() + "], Properties[" + properties + "]";
    }

    @Monitored
    public static String edgeString(final AtlasEdge edge) {
        return "e[" + edge.getLabel() + "], [" + edge.getOutVertex() + " -> " + edge.getLabel() + " -> "
                + edge.getInVertex() + "]";
    }

    @Monitored
    public static <T extends AtlasElement> void setProperty(T element, String propertyName, Object value) {
        String elementStr = string(element);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Setting property {} = \"{}\" to {}", actualPropertyName, value, elementStr);
        Object existValue = element.getProperty(actualPropertyName, Object.class);
        if(value == null || (value instanceof Collection && ((Collection) value).isEmpty())) {
            if(existValue != null) {
                LOG.info("Removing property - {} value from {}", actualPropertyName, elementStr);
                element.removeProperty(actualPropertyName);
            }
        } else {
            if (!value.equals(existValue)) {
                element.setProperty(actualPropertyName, value);
                LOG.debug("Set property {} = \"{}\" to {}", actualPropertyName, value, elementStr);
            }
        }
    }

    /**
     * Gets the value of a property that is stored in the graph as a single property value.  If
     * a multi-property such as {@link Constants#TRAIT_NAMES_PROPERTY_KEY} or {@link Constants#SUPER_TYPES_PROPERTY_KEY}
     * is used, an exception will be thrown.
     * 
     * @param element
     * @param propertyName
     * @param clazz
     * @return
     */
    @Monitored
    public static <T> T getSingleValuedProperty(AtlasElement element, String propertyName, Class<T> clazz) {
        String elementStr = string(element);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Reading property {} from {}", actualPropertyName, elementStr);    
       
        return element.getProperty(actualPropertyName, clazz);
    }


    @Monitored
    public static Object getProperty(AtlasVertex<?,?> vertex, String propertyName) {
        String elementStr = string(vertex);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Reading property {} from {}", actualPropertyName, elementStr);    

        if(AtlasGraphProvider.getGraphInstance().isMultiProperty(actualPropertyName)) {
            return vertex.getPropertyValues(actualPropertyName, String.class);
        }
        else {
            return vertex.getProperty(actualPropertyName, Object.class);
        }
    }

    @Monitored
    public static Object getProperty(AtlasEdge<?,?> edge, String propertyName) {
        String elementStr = string(edge);
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Reading property {} from {}", actualPropertyName, elementStr);      
        return edge.getProperty(actualPropertyName, Object.class);
    }    
    
    private static <T extends AtlasElement> String string(T element) {
        if (element instanceof AtlasVertex) {
            return string((AtlasVertex) element);
        } else if (element instanceof AtlasEdge) {
            return string((AtlasEdge)element);
        }
        return element.toString();
    }
    
    /**
     * Adds an additional value to a multi-property.
     * 
     * @param vertex
     * @param propertyName
     * @param value
     */
    @Monitored
    public static void addProperty(AtlasVertex vertex, String propertyName, Object value) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        LOG.debug("Adding property {} = \"{}\" to vertex {}", actualPropertyName, value, string(vertex));
        vertex.addProperty(actualPropertyName, value);
    }

    /**
     * Remove the specified edge from the graph.
     *
     * @param edge
     */
    @Monitored
    public void removeEdge(AtlasEdge edge) {
        String edgeString = string(edge);
        LOG.debug("Removing {}", edgeString);
        graph.removeEdge(edge);
        LOG.info("Removed {}", edgeString);
    }

    /**
     * Remove the specified AtlasVertex from the graph.
     *
     * @param vertex
     */
    @Monitored
    public void removeVertex(AtlasVertex vertex) {
        String vertexString = string(vertex);
        LOG.debug("Removing {}", vertexString);
        graph.removeVertex(vertex);
        LOG.info("Removed {}", vertexString);
    }

    public AtlasVertex getVertexForGUID(String guid) throws EntityNotFoundException {
        return findVertex(Constants.GUID_PROPERTY_KEY, guid);
    }

    public static String getQualifiedNameForMapKey(String prefix, String key) {
        return prefix + "." + key;
    }

    public static String getQualifiedFieldName(ITypedInstance typedInstance, AttributeInfo attributeInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getQualifiedFieldName(dataType, attributeInfo.name);
    }

    public static String getQualifiedFieldName(IDataType dataType, String attributeName) throws AtlasException {
        return dataType.getTypeCategory() == DataTypes.TypeCategory.STRUCT ? dataType.getName() + "." + attributeName
            // else class or trait
            : ((HierarchicalType) dataType).getQualifiedName(attributeName);
    }

    public static String getTraitLabel(String typeName, String attrName) {
        return attrName;
    }

    public static List<String> getTraitNames(AtlasVertex<?,?> entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        Collection<String> propertyValues = entityVertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        for(String value : propertyValues) {
            traits.add(value);
        }
        return traits;
    }

    public static String getEdgeLabel(ITypedInstance typedInstance, AttributeInfo aInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getEdgeLabel(dataType, aInfo);
    }

    public static String getEdgeLabel(IDataType dataType, AttributeInfo aInfo) throws AtlasException {
        return GraphHelper.EDGE_LABEL_PREFIX + getQualifiedFieldName(dataType, aInfo.name);
    }

    public static Id getIdFromVertex(String dataTypeName, AtlasVertex vertex) {
        return new Id(getIdFromVertex(vertex),
            vertex.getProperty(Constants.VERSION_PROPERTY_KEY, Integer.class), dataTypeName, getStateAsString(vertex));
    }

    public static String getIdFromVertex(AtlasVertex vertex) {
        return vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
    }

    public static String getTypeName(AtlasVertex instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public static Id.EntityState getState(AtlasElement element) {
        String state = getStateAsString(element);
        return state == null ? null : Id.EntityState.valueOf(state);
    }

    public static String getStateAsString(AtlasElement element) {
        return element.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
    }


    //Added conditions in fetching system attributes to handle test failures in GremlinTest where these properties are not set
    public static String getCreatedByAsString(AtlasElement element){
        return element.getProperty(Constants.CREATED_BY_KEY, String.class);
    }

    public static String getModifiedByAsString(AtlasElement element){
        return element.getProperty(Constants.MODIFIED_BY_KEY, String.class);
    }

    public static long getCreatedTime(AtlasElement element){
        return element.getProperty(Constants.TIMESTAMP_PROPERTY_KEY, Long.class);
    }

    public static long getModifiedTime(AtlasElement element){
        return element.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
    }

    /**
     * For the given type, finds an unique attribute and checks if there is an existing instance with the same
     * unique value
     *
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    public AtlasVertex getVertexForInstanceByUniqueAttribute(ClassType classType, IReferenceableInstance instance)
        throws AtlasException {
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        AtlasVertex result = null;
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    result = findVertex(propertyKey, instance.get(attributeInfo.name),
                            Constants.ENTITY_TYPE_PROPERTY_KEY, classType.getName(),
                            Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
                    LOG.debug("Found vertex by unique attribute : {}={}", propertyKey, instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }

        return result;
    }

    /**
     * Guid and AtlasVertex combo
     */
    public static class VertexInfo {
        private String guid;
        private AtlasVertex vertex;
        private String typeName;

        public VertexInfo(String guid, AtlasVertex vertex, String typeName) {
            this.guid = guid;
            this.vertex = vertex;
            this.typeName = typeName;
        }

        public String getGuid() {
            return guid;
        }
        public AtlasVertex getVertex() {
            return vertex;
        }
        public String getTypeName() {
            return typeName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VertexInfo that = (VertexInfo) o;
            return Objects.equals(guid, that.guid) &&
                    Objects.equals(vertex, that.vertex) &&
                    Objects.equals(typeName, that.typeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(guid, vertex, typeName);
        }
    }

    /**
     * Get the GUIDs and vertices for all composite entities owned/contained by the specified root entity AtlasVertex.
     * The graph is traversed from the root entity through to the leaf nodes of the containment graph.
     *
     * @param entityVertex the root entity vertex
     * @return set of VertexInfo for all composite entities
     * @throws AtlasException
     */
    public Set<VertexInfo> getCompositeVertices(AtlasVertex entityVertex) throws AtlasException {
        Set<VertexInfo> result = new HashSet<>();
        Stack<AtlasVertex> vertices = new Stack<>();
        vertices.push(entityVertex);
        while (vertices.size() > 0) {
            AtlasVertex vertex = vertices.pop();
            String typeName = GraphHelper.getTypeName(vertex);
            String guid = GraphHelper.getIdFromVertex(vertex);
            Id.EntityState state = GraphHelper.getState(vertex);
            if (state == Id.EntityState.DELETED) {
                //If the reference vertex is marked for deletion, skip it
                continue;
            }
            result.add(new VertexInfo(guid, vertex, typeName));
            ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
            for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
                if (!attributeInfo.isComposite) {
                    continue;
                }
                String edgeLabel = GraphHelper.getEdgeLabel(classType, attributeInfo);
                switch (attributeInfo.dataType().getTypeCategory()) {
                    case CLASS:
                        AtlasEdge edge = getEdgeForLabel(vertex, edgeLabel);
                        if (edge != null && GraphHelper.getState(edge) == Id.EntityState.ACTIVE) {
                            AtlasVertex compositeVertex = edge.getInVertex();
                            vertices.push(compositeVertex);
                        }
                        break;
                    case ARRAY:
                        IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
                        DataTypes.TypeCategory elementTypeCategory = elementType.getTypeCategory();
                        if (elementTypeCategory != TypeCategory.CLASS) {
                            continue;
                        }
                        Iterator<AtlasEdge> edges = getOutGoingEdgesByLabel(vertex, edgeLabel);
                        if (edges != null) {
                            while (edges.hasNext()) {
                                edge = edges.next();
                                if (edge != null && GraphHelper.getState(edge) == Id.EntityState.ACTIVE) {
                                    AtlasVertex compositeVertex = edge.getInVertex();
                                    vertices.push(compositeVertex);
                                }
                            }
                        }
                        break;
                    case MAP:
                        DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
                        DataTypes.TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                        if (valueTypeCategory != TypeCategory.CLASS) {
                            continue;
                        }
                        String propertyName = GraphHelper.getQualifiedFieldName(classType, attributeInfo.name);
                        List<String> keys = vertex.getProperty(propertyName, List.class);
                        if (keys != null) {
                            for (String key : keys) {
                                String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                                edge = getEdgeForLabel(vertex, mapEdgeLabel);
                                if (edge != null && GraphHelper.getState(edge) == Id.EntityState.ACTIVE) {
                                    AtlasVertex compositeVertex = edge.getInVertex();
                                    vertices.push(compositeVertex);
                                }
                            }
                        }
                        break;
                    default:
                }
            }
        }
        return result;
    }

    public static ITypedReferenceableInstance[] deserializeClassInstances(TypeSystem typeSystem, String entityInstanceDefinition)
    throws AtlasException {
        try {
            JSONArray referableInstances = new JSONArray(entityInstanceDefinition);
            ITypedReferenceableInstance[] instances = new ITypedReferenceableInstance[referableInstances.length()];
            for (int index = 0; index < referableInstances.length(); index++) {
                Referenceable entityInstance =
                        InstanceSerialization.fromJsonReferenceable(referableInstances.getString(index), true);
                ITypedReferenceableInstance typedInstrance = getTypedReferenceableInstance(typeSystem, entityInstance);
                instances[index] = typedInstrance;
            }
            return instances;
        } catch(ValueConversionException | TypeNotFoundException  e) {
            throw e;
        } catch (Exception e) {  // exception from deserializer
            LOG.error("Unable to deserialize json={}", entityInstanceDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json", e);
        }
    }

    public static ITypedReferenceableInstance getTypedReferenceableInstance(TypeSystem typeSystem, Referenceable entityInstance)
            throws AtlasException {
        final String entityTypeName = ParamChecker.notEmpty(entityInstance.getTypeName(), "Entity type cannot be null");
    
        ClassType entityType = typeSystem.getDataType(ClassType.class, entityTypeName);
    
        //Both assigned id and values are required for full update
        //classtype.convert() will remove values if id is assigned. So, set temp id, convert and
        // then replace with original id
        Id origId = entityInstance.getId();
        entityInstance.replaceWithNewId(new Id(entityInstance.getTypeName()));
        ITypedReferenceableInstance typedInstrance = entityType.convert(entityInstance, Multiplicity.REQUIRED);
        ((ReferenceableInstance)typedInstrance).replaceWithNewId(origId);
        return typedInstrance;
    }

    public static boolean isReference(IDataType type) {

        return type.getTypeCategory() == DataTypes.TypeCategory.STRUCT ||
                type.getTypeCategory() == DataTypes.TypeCategory.CLASS;

    }

    @Monitored
    public static void setArrayElementsProperty(IDataType elementType, AtlasVertex instanceVertex, String propertyName, List<Object> values) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if(GraphHelper.isReference(elementType)) {
            setListPropertyFromElementIds(instanceVertex, actualPropertyName, (List)values);
        }
        else {
            setProperty(instanceVertex, actualPropertyName, values);
        }
    }

    @Monitored
    public static void setMapValueProperty(IDataType elementType, AtlasVertex instanceVertex, String propertyName, Object value) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if(GraphHelper.isReference(elementType)) {
            instanceVertex.setPropertyFromElementId(actualPropertyName, (AtlasEdge)value);
        }
        else {
            instanceVertex.setProperty(actualPropertyName, value);
        }
    }

    @Monitored
    public static Object getMapValueProperty(IDataType elementType, AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if(GraphHelper.isReference(elementType)) {
            return instanceVertex.getProperty(actualPropertyName, AtlasEdge.class);
        }
        else {
            return instanceVertex.getProperty(actualPropertyName, Object.class).toString();
        }
    }

    @Monitored
    public static List<Object> getArrayElementsProperty(IDataType elementType, AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if(GraphHelper.isReference(elementType)) {
            return (List)instanceVertex.getListProperty(actualPropertyName, AtlasEdge.class);
        }
        else {
            return (List)instanceVertex.getListProperty(actualPropertyName);
        }
    }

    
    public static void dumpToLog(final AtlasGraph<?,?> graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (AtlasVertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (AtlasEdge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }

    public static String string(ITypedReferenceableInstance instance) {
        return String.format("entity[type=%s guid=%s]", instance.getTypeName(), instance.getId()._getId());
    }

    public static String string(AtlasVertex<?,?> vertex) {
        if(vertex == null) {
            return "vertex[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getVertexDetails(vertex);
            } else {
                return String.format("vertex[id=%s]", vertex.getIdForDisplay());
            }
        }
    }

    public static String getVertexDetails(AtlasVertex<?,?> vertex) {

        return String.format("vertex[id=%s type=%s guid=%s]", vertex.getIdForDisplay(), getTypeName(vertex),
                getIdFromVertex(vertex));
    }


    public static String string(AtlasEdge<?,?> edge) {
        if(edge == null) {
            return "edge[null]";
        } else {
            if (LOG.isDebugEnabled()) {
                return getEdgeDetails(edge);
            } else {
                return String.format("edge[id=%s]", edge.getIdForDisplay());
            }
        }
    }

    public static String getEdgeDetails(AtlasEdge<?,?> edge) {

        return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getIdForDisplay(), edge.getLabel(),
                string(edge.getOutVertex()), string(edge.getInVertex()));
    }

    @VisibleForTesting
    //Keys copied from com.thinkaurelius.titan.graphdb.types.StandardRelationTypeMaker
    //Titan checks that these chars are not part of any keys. So, encoding...
    public static BiMap<String, String> RESERVED_CHARS_ENCODE_MAP =
            HashBiMap.create(new HashMap<String, String>() {{
                put("{", "_o");
                put("}", "_c");
                put("\"", "_q");
                put("$", "_d");
                put("%", "_p");
            }});


    public static String encodePropertyKey(String key) {
        if (StringUtils.isBlank(key)) {
            return key;
        }

        for (String str : RESERVED_CHARS_ENCODE_MAP.keySet()) {
            key = key.replace(str, RESERVED_CHARS_ENCODE_MAP.get(str));
        }
        return key;
    }

    public static String decodePropertyKey(String key) {
        if (StringUtils.isBlank(key)) {
            return key;
        }

        for (String encodedStr : RESERVED_CHARS_ENCODE_MAP.values()) {
            key = key.replace(encodedStr, RESERVED_CHARS_ENCODE_MAP.inverse().get(encodedStr));
        }
        return key;
    }
    public static AttributeInfo getAttributeInfoForSystemAttributes(String field) {
        switch (field) {
        case Constants.STATE_PROPERTY_KEY:
        case Constants.GUID_PROPERTY_KEY:
        case Constants.CREATED_BY_KEY:
        case Constants.MODIFIED_BY_KEY:
                return TypesUtil.newAttributeInfo(field, DataTypes.STRING_TYPE);

        case Constants.TIMESTAMP_PROPERTY_KEY:
        case Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY:
            return TypesUtil.newAttributeInfo(field, DataTypes.DATE_TYPE);
        }
        return null;
    }
    
    public static boolean elementExists(AtlasElement v) {
        return v != null && v.exists();
    }

    public static void setListPropertyFromElementIds(AtlasVertex<?, ?> instanceVertex, String propertyName,
            List<AtlasElement> elements) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        instanceVertex.setPropertyFromElementsIds(actualPropertyName, elements);

    }

    public static void setPropertyFromElementId(AtlasVertex<?, ?> instanceVertex, String propertyName,
            AtlasElement value) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        instanceVertex.setPropertyFromElementId(actualPropertyName, value);

    }

    public static void setListProperty(AtlasVertex instanceVertex, String propertyName, ArrayList<String> value) throws AtlasException {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        instanceVertex.setListProperty(actualPropertyName, value);        
    }
    
    public static List<String> getListProperty(AtlasVertex instanceVertex, String propertyName) throws AtlasException {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        return instanceVertex.getListProperty(actualPropertyName);    
    }
}
