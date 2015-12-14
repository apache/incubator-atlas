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

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.utils.MD5Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class TypedInstanceToGraphMapper {

    private static final Logger LOG = LoggerFactory.getLogger(TypedInstanceToGraphMapper.class);
    private final Map<Id, Vertex> idToVertexMap = new HashMap<>();
    private final TypeSystem typeSystem = TypeSystem.getInstance();

    private final GraphToTypedInstanceMapper graphToTypedInstanceMapper;

    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final String SIGNATURE_HASH_PROPERTY_KEY = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "signature";

    public enum Operation {
        CREATE,
        UPDATE_PARTIAL,
        UPDATE_FULL,
        DELETE
    }

    public TypedInstanceToGraphMapper(GraphToTypedInstanceMapper graphToTypedInstanceMapper) {
        this.graphToTypedInstanceMapper = graphToTypedInstanceMapper;
    }

    String[] mapTypedInstanceToGraph(Operation operation, ITypedReferenceableInstance... typedInstances)
        throws AtlasException {
        List<String> guids = new ArrayList<>();
        for (ITypedReferenceableInstance typedInstance : typedInstances) {
            Collection<IReferenceableInstance> newInstances = walkClassInstances(typedInstance);
            Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> instancesPair =
                    createVerticesAndDiscoverInstances(newInstances);

            switch (operation) {
                case CREATE:
                    addOrUpdateAttributesAndTraits(operation, instancesPair.getLeft());
                    addFullTextProperty(instancesPair.getLeft());
                    break;

                case UPDATE_FULL:
                case UPDATE_PARTIAL:
                    List<ITypedReferenceableInstance> instancesForUpdate = instancesPair.getLeft();
                    instancesForUpdate.addAll(instancesPair.getRight());
                    addOrUpdateAttributesAndTraits(operation, instancesForUpdate);
                    addFullTextProperty(instancesForUpdate);
                    break;

                case DELETE:
                    throw new UnsupportedOperationException("Not handled - " + operation);
            }

            //Return guid for
            addToGuids(typedInstance, guids);
        }
        return guids.toArray(new String[guids.size()]);
    }

    private Collection<IReferenceableInstance> walkClassInstances(ITypedReferenceableInstance typedInstance)
            throws RepositoryException {

        EntityProcessor entityProcessor = new EntityProcessor();
        try {
            LOG.debug("Walking the object graph for instance {}", typedInstance.getTypeName());
            new ObjectGraphWalker(typeSystem, entityProcessor, typedInstance).walk();
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        entityProcessor.addInstanceIfNotExists(typedInstance);
        return entityProcessor.getInstances();
    }

    private void addOrUpdateAttributesAndTraits(Operation operation, List<ITypedReferenceableInstance> instances) throws AtlasException {
        for (ITypedReferenceableInstance instance : instances) {
            try {
                //new vertex, set all the properties
                addOrUpdateAttributesAndTraits(operation, instance);
            } catch (SchemaViolationException e) {
                throw new EntityExistsException(instance, e);
            }
        }
    }

    private void addOrUpdateAttributesAndTraits(Operation operation, ITypedReferenceableInstance typedInstance)
            throws AtlasException {
        LOG.debug("Adding/Updating typed instance {}", typedInstance.getTypeName());

        Id id = typedInstance.getId();
        if (id == null) { // oops
            throw new RepositoryException("id cannot be null");
        }

        Vertex instanceVertex = idToVertexMap.get(id);

        // add the attributes for the instance
        ClassType classType = typeSystem.getDataType(ClassType.class, typedInstance.getTypeName());
        final Map<String, AttributeInfo> fields = classType.fieldMapping().fields;

        mapInstanceToVertex(typedInstance, instanceVertex, fields, false, operation);

        if (Operation.CREATE.equals(operation)) {
            //TODO - Handle Trait updates
            addTraits(typedInstance, instanceVertex, classType);
        }
    }

    private void mapInstanceToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                     Map<String, AttributeInfo> fields, boolean mapOnlyUniqueAttributes, Operation operation)
            throws AtlasException {
        LOG.debug("Mapping instance {} of {} to vertex {}", typedInstance, typedInstance.getTypeName(),
                instanceVertex);
        for (AttributeInfo attributeInfo : fields.values()) {
            if (mapOnlyUniqueAttributes && !attributeInfo.isUnique) {
                continue;
            }
            mapAttributesToVertex(typedInstance, instanceVertex, attributeInfo, operation);
        }
    }

    void mapAttributesToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                               AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);
        LOG.debug("mapping attribute {} = {}", attributeInfo.name, attrValue);
        final String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);

        if (attrValue != null  || operation == Operation.UPDATE_FULL) {
            switch (attributeInfo.dataType().getTypeCategory()) {
                case PRIMITIVE:
                case ENUM:
                    mapPrimitiveOrEnumToVertex(typedInstance, instanceVertex, attributeInfo);
                    break;

                case ARRAY:
                    mapArrayCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                    break;

                case MAP:
                    mapMapCollectionToVertex(typedInstance, instanceVertex, attributeInfo, operation);
                    break;

                case STRUCT:
                case CLASS:
                    Iterator<Edge> outGoingEdgesIterator =
                            GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel).iterator();
                    String currentEntry =
                            outGoingEdgesIterator.hasNext() ? outGoingEdgesIterator.next().getId().toString() : null;
                    addOrUpdateCollectionEntry(instanceVertex, attributeInfo, attributeInfo.dataType(), attrValue,
                            currentEntry, propertyName, operation);
                    break;

                case TRAIT:
                    // do NOTHING - this is taken care of earlier
                    break;

                default:
                    throw new IllegalArgumentException("Unknown type category: " + attributeInfo.dataType().getTypeCategory());
            }
        }
    }

    private Pair<List<ITypedReferenceableInstance>, List<ITypedReferenceableInstance>> createVerticesAndDiscoverInstances(
            Collection<IReferenceableInstance> instances) throws AtlasException {

        List<ITypedReferenceableInstance> instancesToCreate = new ArrayList<>();
        List<ITypedReferenceableInstance> instancesToUpdate = new ArrayList<>();

        for (IReferenceableInstance instance : instances) {
            Id id = instance.getId();
            if (!idToVertexMap.containsKey(id)) {
                Vertex instanceVertex;
                if (id.isAssigned()) {  // has a GUID
                    instanceVertex = graphHelper.getVertexForGUID(id.id);
                    if (!(instance instanceof ReferenceableInstance)) {
                        throw new IllegalStateException(
                                String.format("%s is not of type ITypedReferenceableInstance", instance));
                    }
                    instancesToUpdate.add((ITypedReferenceableInstance) instance);
                } else {
                    //Check if there is already an instance with the same unique attribute value
                    ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());
                    instanceVertex = graphHelper.getVertexForInstanceByUniqueAttribute(classType, instance);

                    //no entity with the given unique attribute, create new
                    if (instanceVertex == null) {
                        ITypedReferenceableInstance newInstance = classType.convert(instance, Multiplicity.REQUIRED);
                        instanceVertex = graphHelper.createVertexWithIdentity(newInstance, classType.getAllSuperTypeNames());
                        instancesToCreate.add(newInstance);

                        //Map only unique attributes for cases of circular references
                        mapInstanceToVertex(newInstance, instanceVertex, classType.fieldMapping().fields, true, Operation.CREATE);
                    } else {
                        if (!(instance instanceof ReferenceableInstance)) {
                            throw new IllegalStateException(
                                    String.format("%s is not of type ITypedReferenceableInstance", instance));
                        }
                        instancesToUpdate.add((ITypedReferenceableInstance) instance);
                    }
                }

                idToVertexMap.put(id, instanceVertex);
            }
        }
        return Pair.of(instancesToCreate, instancesToUpdate);
    }

    private void addToGuids(ITypedReferenceableInstance typedInstance, List<String> guids) {
        Vertex instanceVertex = idToVertexMap.get(typedInstance.getId());
        String guid = instanceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
        guids.add(guid);
    }

    private void addFullTextProperty(List<ITypedReferenceableInstance> instances) throws AtlasException {
        FullTextMapper fulltextMapper = new FullTextMapper(graphToTypedInstanceMapper);
        for (ITypedReferenceableInstance typedInstance : instances) { // Traverse
            Vertex instanceVertex = getClassVertex(typedInstance);
            String fullText = fulltextMapper.mapRecursive(instanceVertex, true);
            GraphHelper.setProperty(instanceVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
        }
    }

    private void addTraits(ITypedReferenceableInstance typedInstance, Vertex instanceVertex, ClassType classType) throws AtlasException {
        for (String traitName : typedInstance.getTraits()) {
            LOG.debug("mapping trait {}", traitName);
            GraphHelper.addProperty(instanceVertex, Constants.TRAIT_NAMES_PROPERTY_KEY, traitName);
            ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

            // add the attributes for the trait instance
            mapTraitInstanceToVertex(traitInstance, classType, instanceVertex);
        }
    }

    /******************************************** STRUCT **************************************************/

    private Pair<Vertex, Edge> updateStructVertex(ITypedStruct structInstance, Edge relEdge, Operation operation) throws AtlasException {
        //Already existing vertex. Update
        Vertex structInstanceVertex = relEdge.getVertex(Direction.IN);

        // Update attributes
        final MessageDigest digester = MD5Utils.getDigester();
        String newSignature = structInstance.getSignatureHash(digester);
        String curSignature = structInstanceVertex.getProperty(SIGNATURE_HASH_PROPERTY_KEY);

        if (!newSignature.equals(curSignature)) {
            //Update struct vertex instance only if there is a change
            LOG.debug("Updating struct {} since signature has changed {} {} ", structInstance, curSignature, newSignature);
            mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false, operation);
            GraphHelper.setProperty(structInstanceVertex, SIGNATURE_HASH_PROPERTY_KEY, String.valueOf(newSignature));
        }
        return Pair.of(structInstanceVertex, relEdge);
    }

    private Pair<Vertex, Edge> addStructVertex(ITypedStruct structInstance, Vertex instanceVertex, AttributeInfo attributeInfo, String edgeLabel) throws AtlasException {
        // add a new vertex for the struct or trait instance
        Vertex structInstanceVertex = graphHelper.createVertexWithoutIdentity(structInstance.getTypeName(), null,
                Collections.<String>emptySet()); // no super types for struct type
        LOG.debug("created vertex {} for struct {} value {}", structInstanceVertex, attributeInfo.name, structInstance);

        // map all the attributes to this new vertex
        mapInstanceToVertex(structInstance, structInstanceVertex, structInstance.fieldMapping().fields, false, Operation.CREATE);
        // add an edge to the newly created vertex from the parent
        Edge relEdge = graphHelper.addEdge(instanceVertex, structInstanceVertex, edgeLabel);

        return Pair.of(structInstanceVertex, relEdge);
    }

    /******************************************** ARRAY **************************************************/

    private void mapArrayCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                attributeInfo.name);
        List newElements = (List) typedInstance.get(attributeInfo.name);
        boolean empty = (newElements == null || newElements.isEmpty());
        if (!empty  || operation == Operation.UPDATE_FULL) {
            String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
            List<String> currentEntries = instanceVertex.getProperty(propertyName);

            IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
            List<String> newEntries = new ArrayList<>();

            if (newElements != null && !newElements.isEmpty()) {
                int index = 0;
                for (; index < newElements.size(); index++) {
                    String currentEntry =
                            (currentEntries != null && index < currentEntries.size()) ? currentEntries.get(index) : null;
                    String newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                            newElements.get(index), currentEntry, propertyName, operation);
                    newEntries.add(newEntry);
                }

                //Remove extra entries in the list
                if (currentEntries != null) {
                    if (index < currentEntries.size()) {
                        for (; index < currentEntries.size(); index++) {
                            removeUnusedReference(currentEntries.get(index), attributeInfo, elementType);
                        }
                    }
                }
            }

            // for dereference on way out
            GraphHelper.setProperty(instanceVertex, propertyName, newEntries);
        }
    }

    /******************************************** MAP **************************************************/

    private void mapMapCollectionToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
        AttributeInfo attributeInfo, Operation operation) throws AtlasException {
        LOG.debug("Mapping instance {} to vertex {} for name {}", typedInstance.getTypeName(), instanceVertex,
                attributeInfo.name);
        @SuppressWarnings("unchecked") Map<Object, Object> collection =
            (Map<Object, Object>) typedInstance.get(attributeInfo.name);
        boolean empty = (collection == null || collection.isEmpty());
        if (!empty  || operation == Operation.UPDATE_FULL) {

            String propertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
            IDataType elementType = ((DataTypes.MapType) attributeInfo.dataType()).getValueType();

            if (!empty) {
                for (Map.Entry entry : collection.entrySet()) {
                    String myPropertyName = propertyName + "." + entry.getKey().toString();

                    String currentEntry = instanceVertex.getProperty(myPropertyName);
                    String newEntry = addOrUpdateCollectionEntry(instanceVertex, attributeInfo, elementType,
                            entry.getValue(), currentEntry, myPropertyName, operation);

                    //Add/Update/Remove property value
                    GraphHelper.setProperty(instanceVertex, myPropertyName, newEntry);
                }

                //Remove unused key references
                List<Object> origKeys = instanceVertex.getProperty(propertyName);
                if (origKeys != null) {
                    if (collection != null) {
                        origKeys.removeAll(collection.keySet());
                    }
                    for (Object unusedKey : origKeys) {
                        String edgeLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo) + "." + unusedKey;
                        if (instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().hasNext()) {
                            Edge edge = instanceVertex.getEdges(Direction.OUT, edgeLabel).iterator().next();
                            removeUnusedReference(edge.getId().toString(), attributeInfo,
                                    ((DataTypes.MapType) attributeInfo.dataType()).getValueType());
                        }
                    }
                }

            }

            // for dereference on way out
            GraphHelper.setProperty(instanceVertex, propertyName, collection == null ? null : new ArrayList(collection.keySet()));
        }
    }

    /******************************************** ARRAY & MAP **************************************************/

    private String addOrUpdateCollectionEntry(Vertex instanceVertex, AttributeInfo attributeInfo,
                                              IDataType elementType, Object newVal, String curVal, String propertyName,
                                              Operation operation)
        throws AtlasException {

        final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return newVal != null ? newVal.toString() : null;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            return null;

        case STRUCT:
            return addOrUpdateStruct(instanceVertex, attributeInfo, elementType, (ITypedStruct) newVal, curVal, edgeLabel, operation);

        case CLASS:
            return addOrUpdateClassVertex(instanceVertex, attributeInfo, elementType,
                    (ITypedReferenceableInstance) newVal, curVal, edgeLabel, operation);

        default:
            throw new IllegalArgumentException("Unknown type category: " + elementType.getTypeCategory());
        }
    }

    private String addOrUpdateStruct(Vertex instanceVertex, AttributeInfo attributeInfo, IDataType elementType,
                                     ITypedStruct structAttr, String curVal,
                                     String edgeLabel, Operation operation) throws AtlasException {
        Pair<Vertex, Edge> vertexEdgePair = null;
        if (curVal != null && structAttr == null) {
            //remove edge
            removeUnusedReference(curVal, attributeInfo, elementType);
        } else if (curVal != null && structAttr != null) {
            //update
            Edge edge = graphHelper.getOutGoingEdgeById(curVal);
            vertexEdgePair = updateStructVertex(structAttr, edge, operation);
        } else if (structAttr != null) {
            //add
            vertexEdgePair = addStructVertex(structAttr, instanceVertex, attributeInfo, edgeLabel);
        }

        return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;
    }

    private String addOrUpdateClassVertex(Vertex instanceVertex, AttributeInfo attributeInfo, IDataType elementType,
                                          ITypedReferenceableInstance newVal, String curVal,
                                          String edgeLabel, Operation operation) throws AtlasException {
        Vertex toVertex = getClassVertex(newVal);
        if(toVertex == null && newVal != null) {
            LOG.error("Could not find vertex for Class Reference " + newVal);
            throw new EntityNotFoundException("Could not find vertex for Class Reference " + newVal);
        }

        Pair<Vertex, Edge> vertexEdgePair = null;
        if (curVal != null && newVal == null) {
            //remove edge
            removeUnusedReference(curVal, attributeInfo, elementType);
        } else if (curVal != null && newVal != null) {
            Edge edge = graphHelper.getOutGoingEdgeById(curVal);
            Id classRefId = getId(newVal);
            vertexEdgePair = updateClassEdge(classRefId, newVal, instanceVertex, edge, toVertex, attributeInfo, elementType, edgeLabel, operation);
        } else if (newVal != null){
            vertexEdgePair = addClassEdge(instanceVertex, toVertex, edgeLabel);
        }

        return (vertexEdgePair != null) ? vertexEdgePair.getRight().getId().toString() : null;
    }

    /******************************************** CLASS **************************************************/

    private Pair<Vertex, Edge> addClassEdge(Vertex instanceVertex, Vertex toVertex, String edgeLabel) throws AtlasException {
            // add an edge to the class vertex from the instance
          Edge edge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
          return Pair.of(toVertex, edge);
    }

    private Vertex getClassVertex(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        Vertex referenceVertex = null;
        Id id = null;
        if (typedReference != null) {
            id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
            if (id.isAssigned()) {
                referenceVertex = graphHelper.getVertexForGUID(id.id);
            } else {
                referenceVertex = idToVertexMap.get(id);
            }
        }

        return referenceVertex;
    }

    private Id getId(ITypedReferenceableInstance typedReference) throws EntityNotFoundException {
        Id id = null;
        if (typedReference != null) {
            id = typedReference instanceof Id ? (Id) typedReference : typedReference.getId();
        }

        if (id.isUnassigned()) {
            Vertex classVertex = idToVertexMap.get(id);
            String guid = classVertex.getProperty(Constants.GUID_PROPERTY_KEY);
            id = new Id(guid, 0, typedReference.getTypeName());
        }
        return id;
    }


    private Pair<Vertex, Edge> updateClassEdge(Id id, final ITypedReferenceableInstance typedInstance,
                                               Vertex instanceVertex, Edge edge, Vertex toVertex,
                                               AttributeInfo attributeInfo, IDataType dataType,
                                               String edgeLabel, Operation operation) throws AtlasException {
        Pair<Vertex, Edge> result = Pair.of(toVertex, edge);
        Edge newEdge = edge;
        // Update edge if it exists
        Vertex invertex = edge.getVertex(Direction.IN);
        String currentGUID = invertex.getProperty(Constants.GUID_PROPERTY_KEY);
        Id currentId = new Id(currentGUID, 0, (String) invertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY));
        if (!currentId.equals(id)) {
             // add an edge to the class vertex from the instance
            if(toVertex != null) {
                newEdge = graphHelper.addEdge(instanceVertex, toVertex, edgeLabel);
                result = Pair.of(toVertex, newEdge);
            }
            removeUnusedReference(edge.getId().toString(), attributeInfo, dataType);
        }

        if (attributeInfo.isComposite) {
            //Update the attributes also if composite
            if (typedInstance.fieldMapping() != null) {
                //In case of Id instance, fieldMapping is null
                mapInstanceToVertex(typedInstance, toVertex, typedInstance.fieldMapping().fields , false, operation);
                //Update full text for the updated composite vertex
                addFullTextProperty(new ArrayList<ITypedReferenceableInstance>() {{ add(typedInstance); }});
            }
        }

        return result;
    }

    /******************************************** TRAITS ****************************************************/

    void mapTraitInstanceToVertex(ITypedStruct traitInstance, IDataType entityType, Vertex parentInstanceVertex)
        throws AtlasException {
        // add a new vertex for the struct or trait instance
        final String traitName = traitInstance.getTypeName();
        Vertex traitInstanceVertex = graphHelper.createVertexWithoutIdentity(traitInstance.getTypeName(), null,
                typeSystem.getDataType(TraitType.class, traitName).getAllSuperTypeNames());
        LOG.debug("created vertex {} for trait {}", traitInstanceVertex, traitName);

        // map all the attributes to this newly created vertex
        mapInstanceToVertex(traitInstance, traitInstanceVertex, traitInstance.fieldMapping().fields, false, Operation.CREATE);

        // add an edge to the newly created vertex from the parent
        String relationshipLabel = GraphHelper.getTraitLabel(entityType.getName(), traitName);
        graphHelper.addEdge(parentInstanceVertex, traitInstanceVertex, relationshipLabel);
    }

    /******************************************** PRIMITIVES **************************************************/

    private void mapPrimitiveOrEnumToVertex(ITypedInstance typedInstance, Vertex instanceVertex,
                                            AttributeInfo attributeInfo) throws AtlasException {
        Object attrValue = typedInstance.get(attributeInfo.name);

        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        Object propertyValue = null;

        if (attrValue == null ) {
            propertyValue = null;
        } else if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            propertyValue = typedInstance.getString(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            propertyValue = typedInstance.getShort(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            propertyValue = typedInstance.getInt(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            propertyValue = typedInstance.getBigInt(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            propertyValue = typedInstance.getBoolean(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            propertyValue = typedInstance.getByte(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            propertyValue = typedInstance.getLong(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            propertyValue = typedInstance.getFloat(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            propertyValue = typedInstance.getDouble(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            propertyValue = typedInstance.getBigDecimal(attributeInfo.name);
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Date dateVal = typedInstance.getDate(attributeInfo.name);
            //Convert Property value to Long  while persisting
            propertyValue = dateVal.getTime();
        } else if (attributeInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            if (attrValue != null) {
                propertyValue = ((EnumValue)attrValue).value;
            }
        }


        GraphHelper.setProperty(instanceVertex, vertexPropertyName, propertyValue);
    }

    private Edge removeUnusedReference(String edgeId, AttributeInfo attributeInfo, IDataType<?> elementType) {
        //Remove edges for property values which do not exist any more
        Edge removedRelation = null;
        switch (elementType.getTypeCategory()) {
        case STRUCT:
            removedRelation = graphHelper.removeRelation(edgeId, true);
            //Remove the vertex from state so that further processing no longer uses this
            break;
        case CLASS:
            removedRelation = graphHelper.removeRelation(edgeId, attributeInfo.isComposite);
            break;
        }
        return removedRelation;
    }
}
