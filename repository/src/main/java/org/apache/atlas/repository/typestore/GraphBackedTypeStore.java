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

package org.apache.atlas.repository.typestore;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Singleton
public class GraphBackedTypeStore implements ITypeStore {
    public static final String VERTEX_TYPE = "typeSystem";
    private static final String PROPERTY_PREFIX = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";

    private static Logger LOG = LoggerFactory.getLogger(GraphBackedTypeStore.class);

    private final TitanGraph titanGraph;

    @Inject
    public GraphBackedTypeStore(GraphProvider<TitanGraph> graphProvider) {
        titanGraph = graphProvider.get();
    }

    @Override
    public void store(TypeSystem typeSystem) throws AtlasException {
        store(typeSystem, ImmutableList.copyOf(typeSystem.getTypeNames()));
    }

    @Override
    @GraphTransaction
    public void store(TypeSystem typeSystem, ImmutableList<String> typeNames) throws AtlasException {
        for (String typeName : typeNames) {
            IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
            LOG.debug("Processing {}.{} in type store", dataType.getTypeCategory(), dataType.getName());
            switch (dataType.getTypeCategory()) {
            case ENUM:
                storeInGraph((EnumType) dataType);
                break;

            case STRUCT:
                StructType structType = (StructType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(),
                        ImmutableList.copyOf(structType.infoToNameMap.keySet()), ImmutableList.<String>of());
                break;

            case TRAIT:
            case CLASS:
                HierarchicalType type = (HierarchicalType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(), type.immediateAttrs,
                        type.superTypes);
                break;

            default:    //Ignore primitive/collection types as they are covered under references
                break;
            }
        }
    }

    private void addProperty(Vertex vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.setProperty(propertyName, value);
    }

    private void storeInGraph(EnumType dataType) {
        Vertex vertex = createVertex(dataType.getTypeCategory(), dataType.getName());
        List<String> values = new ArrayList<>(dataType.values().size());
        for (EnumValue enumValue : dataType.values()) {
            String key = getPropertyKey(dataType.getName(), enumValue.value);
            addProperty(vertex, key, enumValue.ordinal);
            values.add(enumValue.value);
        }
        addProperty(vertex, getPropertyKey(dataType.getName()), values);
    }

    private String getPropertyKey(String name) {
        return PROPERTY_PREFIX + name;
    }

    private String getPropertyKey(String parent, String child) {
        return PROPERTY_PREFIX + parent + "." + child;
    }

    private String getEdgeLabel(String parent, String child) {
        return PROPERTY_PREFIX + "edge." + parent + "." + child;
    }

    private void storeInGraph(TypeSystem typeSystem, DataTypes.TypeCategory category, String typeName,
            ImmutableList<AttributeInfo> attributes, ImmutableList<String> superTypes) throws AtlasException {
        Vertex vertex = createVertex(category, typeName);
        List<String> attrNames = new ArrayList<>();
        if (attributes != null) {
            for (AttributeInfo attribute : attributes) {
                String propertyKey = getPropertyKey(typeName, attribute.name);
                try {
                    addProperty(vertex, propertyKey, attribute.toJson());
                } catch (JSONException e) {
                    throw new StorageException(typeName, e);
                }
                attrNames.add(attribute.name);
                addReferencesForAttribute(typeSystem, vertex, attribute);
            }
        }
        addProperty(vertex, getPropertyKey(typeName), attrNames);

        //Add edges for hierarchy
        if (superTypes != null) {
            for (String superTypeName : superTypes) {
                HierarchicalType superType = typeSystem.getDataType(HierarchicalType.class, superTypeName);
                Vertex superVertex = createVertex(superType.getTypeCategory(), superTypeName);
                addEdge(vertex, superVertex, SUPERTYPE_EDGE_LABEL);
            }
        }
    }

    //Add edges for complex attributes
    private void addReferencesForAttribute(TypeSystem typeSystem, Vertex vertex, AttributeInfo attribute)
            throws AtlasException {
        ImmutableList<String> coreTypes = typeSystem.getCoreTypes();
        List<IDataType> attrDataTypes = new ArrayList<>();
        IDataType attrDataType = attribute.dataType();
        String vertexTypeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY);

        switch (attrDataType.getTypeCategory()) {
        case ARRAY:
            String attrType = TypeUtils.parseAsArrayType(attrDataType.getName());
            IDataType elementType = typeSystem.getDataType(IDataType.class, attrType);
            attrDataTypes.add(elementType);
            break;

        case MAP:
            String[] attrTypes = TypeUtils.parseAsMapType(attrDataType.getName());
            IDataType keyType = typeSystem.getDataType(IDataType.class, attrTypes[0]);
            IDataType valueType = typeSystem.getDataType(IDataType.class, attrTypes[1]);
            attrDataTypes.add(keyType);
            attrDataTypes.add(valueType);
            break;

        case ENUM:
        case STRUCT:
        case CLASS:
            attrDataTypes.add(attrDataType);
            break;

        case PRIMITIVE: //no vertex for primitive type, hence no edge required
            break;

        default:
            throw new IllegalArgumentException(
                    "Attribute cannot reference instances of type : " + attrDataType.getTypeCategory());
        }

        for (IDataType attrType : attrDataTypes) {
            if (!coreTypes.contains(attrType.getName())) {
                Vertex attrVertex = createVertex(attrType.getTypeCategory(), attrType.getName());
                String label = getEdgeLabel(vertexTypeName, attribute.name);
                addEdge(vertex, attrVertex, label);
            }
        }
    }

    private void addEdge(Vertex fromVertex, Vertex toVertex, String label) {
        LOG.debug("Adding edge from {} to {} with label {}", toString(fromVertex), toString(toVertex), label);
        titanGraph.addEdge(null, fromVertex, toVertex, label);
    }

    @Override
    @GraphTransaction
    public TypesDef restore() throws AtlasException {
        //Get all vertices for type system
        Iterator vertices =
                titanGraph.query().has(Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE).vertices().iterator();

        ImmutableList.Builder<EnumTypeDefinition> enums = ImmutableList.builder();
        ImmutableList.Builder<StructTypeDefinition> structs = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<ClassType>> classTypes = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.builder();

        while (vertices.hasNext()) {
            Vertex vertex = (Vertex) vertices.next();
            DataTypes.TypeCategory typeCategory = vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY);
            String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY);
            LOG.info("Restoring type {}.{}", typeCategory, typeName);
            switch (typeCategory) {
            case ENUM:
                enums.add(getEnumType(vertex));
                break;

            case STRUCT:
                AttributeDefinition[] attributes = getAttributes(vertex, typeName);
                structs.add(new StructTypeDefinition(typeName, attributes));
                break;

            case CLASS:
                ImmutableList<String> superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                classTypes.add(new HierarchicalTypeDefinition(ClassType.class, typeName, superTypes, attributes));
                break;

            case TRAIT:
                superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                traits.add(new HierarchicalTypeDefinition(TraitType.class, typeName, superTypes, attributes));
                break;

            default:
                throw new IllegalArgumentException("Unhandled type category " + typeCategory);
            }
        }
        return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
    }

    private EnumTypeDefinition getEnumType(Vertex vertex) {
        String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY);
        List<EnumValue> enumValues = new ArrayList<>();
        List<String> values = vertex.getProperty(getPropertyKey(typeName));
        for (String value : values) {
            String valueProperty = getPropertyKey(typeName, value);
            enumValues.add(new EnumValue(value, vertex.<Integer>getProperty(valueProperty)));
        }
        return new EnumTypeDefinition(typeName, enumValues.toArray(new EnumValue[enumValues.size()]));
    }

    private ImmutableList<String> getSuperTypes(Vertex vertex) {
        List<String> superTypes = new ArrayList<>();
        Iterator<Edge> edges = vertex.getEdges(Direction.OUT, SUPERTYPE_EDGE_LABEL).iterator();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            superTypes.add((String) edge.getVertex(Direction.IN).getProperty(Constants.TYPENAME_PROPERTY_KEY));
        }
        return ImmutableList.copyOf(superTypes);
    }

    private AttributeDefinition[] getAttributes(Vertex vertex, String typeName) throws AtlasException {
        List<AttributeDefinition> attributes = new ArrayList<>();
        List<String> attrNames = vertex.getProperty(getPropertyKey(typeName));
        if (attrNames != null) {
            for (String attrName : attrNames) {
                try {
                    String propertyKey = getPropertyKey(typeName, attrName);
                    attributes.add(AttributeInfo.fromJson((String) vertex.getProperty(propertyKey)));
                } catch (JSONException e) {
                    throw new AtlasException(e);
                }
            }
        }
        return attributes.toArray(new AttributeDefinition[attributes.size()]);
    }

    private String toString(Vertex vertex) {
        return PROPERTY_PREFIX + vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY);
    }

    /**
     * Find vertex for the given type category and name, else create new vertex
     * @param category
     * @param typeName
     * @return vertex
     */
    private Vertex findVertex(DataTypes.TypeCategory category, String typeName) {
        LOG.debug("Finding vertex for {}.{}", category, typeName);

        Iterator results = titanGraph.query().has(Constants.TYPENAME_PROPERTY_KEY, typeName).vertices().iterator();
        Vertex vertex = null;
        if (results != null && results.hasNext()) {
            //There should be just one vertex with the given typeName
            vertex = (Vertex) results.next();
        }
        return vertex;
    }

    private Vertex createVertex(DataTypes.TypeCategory category, String typeName) {
        Vertex vertex = findVertex(category, typeName);
        if (vertex == null) {
            LOG.debug("Adding vertex {}{}", PROPERTY_PREFIX, typeName);
            vertex = titanGraph.addVertex(null);
            addProperty(vertex, Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE);  //Mark as type vertex
            addProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, category);
            addProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, typeName);
        }
        return vertex;
    }
}
