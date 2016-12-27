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

package org.apache.atlas.repository.graphdb.titan0;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Sanity test of basic graph operations using the Titan 0.5.4 graphdb
 * abstraction layer implementation.
 */
public class Titan0DatabaseTest {

    private AtlasGraph<?, ?> atlasGraph;

    private <V, E> AtlasGraph<V, E> getGraph() {
        if (atlasGraph == null) {
            Titan0GraphDatabase db = new Titan0GraphDatabase();
            atlasGraph = db.getGraph();
            AtlasGraphManagement mgmt = atlasGraph.getManagementSystem();
            // create the index (which defines these properties as being mult
            // many)
            for (String propertyName : new String[]{"__superTypeNames", "__traitNames"}) {
                AtlasPropertyKey propertyKey = mgmt.getPropertyKey(propertyName);
                if (propertyKey == null) {
                    propertyKey = mgmt.makePropertyKey(propertyName, String.class, AtlasCardinality.SET);
                    mgmt.createExactMatchIndex(propertyName, false, Collections.singletonList(propertyKey));
                }
            }
            mgmt.commit();
        }
        return (AtlasGraph<V, E>) atlasGraph;
    }

    @AfterClass
    public void cleanup() {
        atlasGraph.clear();
        atlasGraph = null;

    }

    @Test
    public <V, E> void testPropertyDataTypes() {

        // primitives
        AtlasGraph<V, E> graph = getGraph();

        testProperty(graph, "booleanProperty", Boolean.TRUE);
        testProperty(graph, "booleanProperty", Boolean.FALSE);

        testProperty(graph, "byteProperty", Byte.MAX_VALUE);
        testProperty(graph, "byteProperty", Byte.MIN_VALUE);

        testProperty(graph, "shortProperty", Short.MAX_VALUE);
        testProperty(graph, "shortProperty", Short.MIN_VALUE);

        testProperty(graph, "intProperty", Integer.MAX_VALUE);
        testProperty(graph, "intProperty", Integer.MIN_VALUE);

        testProperty(graph, "longProperty", Long.MIN_VALUE);
        testProperty(graph, "longProperty", Long.MAX_VALUE);

        testProperty(graph, "doubleProperty", Double.MAX_VALUE);
        testProperty(graph, "doubleProperty", Double.MIN_VALUE);

        testProperty(graph, "floatProperty", Float.MAX_VALUE);
        testProperty(graph, "floatProperty", Float.MIN_VALUE);

        // enumerations - TypeCategory
        testProperty(graph, "typeCategoryProperty", TypeCategory.CLASS);

        // biginteger
        testProperty(graph, "bigIntegerProperty",
                new BigInteger(String.valueOf(Long.MAX_VALUE)).multiply(BigInteger.TEN));

        // bigdecimal
        BigDecimal bigDecimal = new BigDecimal(Double.MAX_VALUE);
        testProperty(graph, "bigDecimalProperty", bigDecimal.multiply(bigDecimal));
    }

    private <V, E> void testProperty(AtlasGraph<V, E> graph, String name, Object value) {

        AtlasVertex<V, E> vertex = graph.addVertex();
        vertex.setProperty(name, value);
        assertEquals(value, vertex.getProperty(name, value.getClass()));
        AtlasVertex<V, E> loaded = graph.getVertex(vertex.getId().toString());
        assertEquals(value, loaded.getProperty(name, value.getClass()));
    }

    @Test
    public <V, E> void testMultiplicityOnePropertySupport() {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> vertex = graph.addVertex();
        vertex.setProperty("name", "Jeff");
        vertex.setProperty("location", "Littleton");
        assertEquals("Jeff", vertex.getProperty("name", String.class));
        assertEquals("Littleton", vertex.getProperty("location", String.class));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());

        assertEquals("Jeff", vertexCopy.getProperty("name", String.class));
        assertEquals("Littleton", vertexCopy.getProperty("location", String.class));

        assertTrue(vertexCopy.getPropertyKeys().contains("name"));
        assertTrue(vertexCopy.getPropertyKeys().contains("location"));

        assertTrue(vertexCopy.getPropertyValues("name", String.class).contains("Jeff"));
        assertTrue(vertexCopy.getPropertyValues("location", String.class).contains("Littleton"));
        assertTrue(vertexCopy.getPropertyValues("test", String.class).isEmpty());
        assertNull(vertexCopy.getProperty("test", String.class));

        vertex.removeProperty("name");
        assertFalse(vertex.getPropertyKeys().contains("name"));
        assertNull(vertex.getProperty("name", String.class));
        assertTrue(vertex.getPropertyValues("name", String.class).isEmpty());

        vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("name"));
        assertNull(vertexCopy.getProperty("name", String.class));
        assertTrue(vertexCopy.getPropertyValues("name", String.class).isEmpty());

    }

    @Test
    public <V, E> void testRemoveEdge() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = graph.addVertex();
        AtlasVertex<V, E> v2 = graph.addVertex();

        AtlasEdge<V, E> edge = graph.addEdge(v1, v2, "knows");

        // make sure the edge exists
        AtlasEdge<V, E> edgeCopy = graph.getEdge(edge.getId().toString());
        assertNotNull(edgeCopy);
        assertEquals(edgeCopy, edge);

        graph.removeEdge(edge);

        edgeCopy = graph.getEdge(edge.getId().toString());
        // should return null now, since edge was deleted
        assertNull(edgeCopy);

    }

    @Test
    public <V, E> void testRemoveVertex() {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> v1 = graph.addVertex();

        assertNotNull(graph.getVertex(v1.getId().toString()));

        graph.removeVertex(v1);

        assertNull(graph.getVertex(v1.getId().toString()));
    }

    @Test
    public <V, E> void testGetEdges() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> v1 = graph.addVertex();
        AtlasVertex<V, E> v2 = graph.addVertex();
        AtlasVertex<V, E> v3 = graph.addVertex();

        AtlasEdge<V, E> knows = graph.addEdge(v2, v1, "knows");
        AtlasEdge<V, E> eats = graph.addEdge(v3, v1, "eats");
        AtlasEdge<V, E> drives = graph.addEdge(v3, v2, "drives");
        AtlasEdge<V, E> sleeps = graph.addEdge(v2, v3, "sleeps");

        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.IN), knows, eats);
        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.OUT));
        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.BOTH), knows, eats);

        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.IN, "knows"), knows);
        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.OUT, "knows"));
        assertEdgesMatch(v1.getEdges(AtlasEdgeDirection.BOTH, "knows"), knows);

        assertEdgesMatch(v2.getEdges(AtlasEdgeDirection.IN), drives);
        assertEdgesMatch(v2.getEdges(AtlasEdgeDirection.OUT), knows, sleeps);
        assertEdgesMatch(v2.getEdges(AtlasEdgeDirection.BOTH), knows, sleeps, drives);

        assertEdgesMatch(v2.getEdges(AtlasEdgeDirection.BOTH, "delivers"));
    }

    private <V, E> void assertEdgesMatch(Iterable<AtlasEdge<V, E>> edgesIt, AtlasEdge<V, E>... expected) {
        List<AtlasEdge<V, E>> edges = toList(edgesIt);
        assertEquals(expected.length, edges.size());
        for (AtlasEdge<V, E> edge : expected) {
            assertTrue(edges.contains(edge));
        }
    }

    @Test
    public <V, E> void testMultiplictyManyPropertySupport() {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> vertex = graph.addVertex();
        String vertexId = vertex.getId().toString();
        vertex.setProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        vertex.setProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        assertEquals(vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class).size(), 2);
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait3");
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait4");

        assertTrue(vertex.getPropertyKeys().contains(Constants.TRAIT_NAMES_PROPERTY_KEY));
        validateMultManyPropertiesInVertex(vertex);
        // fetch a copy of the vertex, make sure result
        // is the same

        validateMultManyPropertiesInVertex(graph.getVertex(vertexId));

    }

    private <V, E> void validateMultManyPropertiesInVertex(AtlasVertex<V, E> vertex) {

        assertTrue(vertex.getPropertyKeys().contains(Constants.TRAIT_NAMES_PROPERTY_KEY));
        Collection<String> traitNames = vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertTrue(traitNames.contains("trait1"));
        assertTrue(traitNames.contains("trait2"));
        assertTrue(traitNames.contains("trait3"));
        assertTrue(traitNames.contains("trait4"));

        try {
            vertex.getProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
            fail("Expected exception not thrown");
        } catch (IllegalStateException expected) {
            // multiple property values exist
        }
    }

    @Test
    public <V, E> void testListProperties() throws AtlasException {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> vertex = graph.addVertex();
        vertex.setListProperty("colors", Arrays.asList("red", "blue", "green"));
        List<String> colors = vertex.getListProperty("colors");
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());
        colors = vertexCopy.getListProperty("colors");
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
        assertTrue(colors.contains("green"));

    }

    @Test
    public <V, E> void testRemoveProperty() {

        AtlasGraph<V, E> graph = getGraph();
        AtlasVertex<V, E> vertex = graph.addVertex();
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        vertex.setProperty("name", "Jeff");

        // remove existing property - multiplicity one
        vertex.removeProperty("jeff");

        assertFalse(vertex.getPropertyKeys().contains("jeff"));

        // remove existing property - multiplicity many
        vertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);
        assertFalse(vertex.getPropertyKeys().contains(Constants.TRAIT_NAMES_PROPERTY_KEY));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("jeff"));
        assertFalse(vertexCopy.getPropertyKeys().contains(Constants.TRAIT_NAMES_PROPERTY_KEY));

        // remove non-existing property
        vertex.removeProperty(Constants.TRAIT_NAMES_PROPERTY_KEY);
        vertex.removeProperty("jeff");

    }

    @Test
    public <V, E> void getGetGraphQueryForVertices() {

        AtlasGraph<V, E> graph = getGraph();

        AtlasVertex<V, E> v1 = graph.addVertex();
        AtlasVertex<V, E> v2 = graph.addVertex();
        AtlasVertex<V, E> v3 = graph.addVertex();

        v1.setProperty("name", "Jeff");
        v1.setProperty("weight", 1);

        v2.setProperty("name", "Fred");
        v2.setProperty("weight", 2);

        v3.setProperty("name", "Chris");
        v3.setProperty("weight", 3);

        AtlasEdge<V, E> knows = graph.addEdge(v2, v1, "knows");
        knows.setProperty("weight", 1);
        AtlasEdge<V, E> eats = graph.addEdge(v3, v1, "eats");
        eats.setProperty("weight", 2);
        AtlasEdge<V, E> drives = graph.addEdge(v3, v2, "drives");
        drives.setProperty("weight", 3);

        AtlasEdge<V, E> sleeps = graph.addEdge(v2, v3, "sleeps");
        sleeps.setProperty("weight", 4);

        testExecuteGraphQuery("name", null, "Jeff", v1);
        testExecuteGraphQuery("weight", ComparisionOperator.EQUAL, 2, v2);
        testExecuteGraphQuery("weight", ComparisionOperator.GREATER_THAN_EQUAL, 2, v2, v3);
        testExecuteGraphQuery("weight", ComparisionOperator.LESS_THAN_EQUAL, 2, v2, v1);

    }

    private <V, E> void testExecuteGraphQuery(String property, ComparisionOperator op, Object value,
            AtlasVertex<V, E>... expected) {
        AtlasGraph<V, E> graph = getGraph();
        AtlasGraphQuery<V, E> query = graph.query();
        if (op != null) {
            query.has(property, op, value);
        } else {
            query.has(property, value);
        }
        Iterable<? extends AtlasVertex<V, E>> result = query.vertices();
        List<AtlasVertex<V, E>> list = toList(result);
        assertEquals(expected.length, list.size());
        for (AtlasVertex<V, E> vertex : expected) {
            assertTrue(list.contains(vertex));
        }
    }

    @Test
    public <V, E> void testAddMultManyPropertyValueTwice() {

        AtlasGraph<V, E> graph = getGraph();
        String vertexId;

        AtlasVertex<V, E> vertex = graph.addVertex();
        vertexId = vertex.getId().toString();
        vertex.setProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        vertex.setProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait1");
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");
        vertex.addProperty(Constants.TRAIT_NAMES_PROPERTY_KEY, "trait2");

        validateDuplicatePropertyVertex(vertex);

        // fetch a copy of the vertex, make sure result is the same

        validateDuplicatePropertyVertex(graph.getVertex(vertexId));
    }

    private <V, E> void validateDuplicatePropertyVertex(AtlasVertex<V, E> vertex) {
        assertEquals(2, vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class).size());
        assertTrue(vertex.getPropertyKeys().contains(Constants.TRAIT_NAMES_PROPERTY_KEY));
        Collection<String> traitNames = vertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
        assertTrue(traitNames.contains("trait1"));
        assertTrue(traitNames.contains("trait2"));
    }

    private static <T> List<T> toList(Iterable<? extends T> iterable) {
        List<T> result = new ArrayList<>();
        for (T item : iterable) {
            result.add(item);
        }
        return result;
    }

}
