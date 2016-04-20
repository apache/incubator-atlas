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
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtils;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createStructTypeDef;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedTypeStoreTest {
    @Inject
    private GraphProvider<TitanGraph> graphProvider;

    @Inject
    private ITypeStore typeStore;

    private TypeSystem ts;

    @BeforeClass
    public void setUp() throws Exception {
        ts = TypeSystem.getInstance();
        ts.reset();
        TestUtils.defineDeptEmployeeTypes(ts);
    }

    @AfterClass
    public void tearDown() throws Exception {
        ts.reset();
        graphProvider.get().shutdown();
        try {
            TitanCleanup.clear(graphProvider.get());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStore() throws AtlasException {
        ImmutableList<String> typeNames = ts.getTypeNames();
        typeStore.store(ts, typeNames);
        dumpGraph();
    }

    private void dumpGraph() {
        TitanGraph graph = graphProvider.get();
        for (Vertex v : graph.getVertices()) {
            System.out.println("****v = " + GraphHelper.vertexString(v));
            for (Edge e : v.getEdges(Direction.OUT)) {
                System.out.println("****e = " + GraphHelper.edgeString(e));
            }
        }
    }

    @Test(dependsOnMethods = "testStore")
    public void testRestore() throws Exception {
        String description = "_description";
        TypesDef types = typeStore.restore();

        //validate enum
        List<EnumTypeDefinition> enumTypes = types.enumTypesAsJavaList();
        Assert.assertEquals(1, enumTypes.size());
        EnumTypeDefinition orgLevel = enumTypes.get(0);
        Assert.assertEquals(orgLevel.name, "OrgLevel");
        Assert.assertEquals(orgLevel.description, "OrgLevel"+description);
        Assert.assertEquals(orgLevel.enumValues.length, 2);
        EnumValue enumValue = orgLevel.enumValues[0];
        Assert.assertEquals(enumValue.value, "L1");
        Assert.assertEquals(enumValue.ordinal, 1);

        //validate class
        List<StructTypeDefinition> structTypes = types.structTypesAsJavaList();
        Assert.assertEquals(1, structTypes.size());

        boolean clsTypeFound = false;
        List<HierarchicalTypeDefinition<ClassType>> classTypes = types.classTypesAsJavaList();
        for (HierarchicalTypeDefinition<ClassType> classType : classTypes) {
            if (classType.typeName.equals("Manager")) {
                ClassType expectedType = ts.getDataType(ClassType.class, classType.typeName);
                Assert.assertEquals(expectedType.immediateAttrs.size(), classType.attributeDefinitions.length);
                Assert.assertEquals(expectedType.superTypes.size(), classType.superTypes.size());
                Assert.assertEquals(classType.typeDescription, classType.typeName+description);
                clsTypeFound = true;
            }
        }
        Assert.assertTrue(clsTypeFound, "Manager type not restored");

        //validate trait
        List<HierarchicalTypeDefinition<TraitType>> traitTypes = types.traitTypesAsJavaList();
        Assert.assertEquals(1, traitTypes.size());
        HierarchicalTypeDefinition<TraitType> trait = traitTypes.get(0);
        Assert.assertEquals("SecurityClearance", trait.typeName);
        Assert.assertEquals(trait.typeName+description, trait.typeDescription);
        Assert.assertEquals(1, trait.attributeDefinitions.length);
        AttributeDefinition attribute = trait.attributeDefinitions[0];
        Assert.assertEquals("level", attribute.name);
        Assert.assertEquals(DataTypes.INT_TYPE.getName(), attribute.dataTypeName);

        //validate the new types
        ts.reset();
        ts.defineTypes(types);
    }

    @Test(dependsOnMethods = "testStore")
    public void testTypeUpdate() throws Exception {
        //Add enum value
        String _description = "_description_updated";
        EnumTypeDefinition orgLevelEnum = new EnumTypeDefinition("OrgLevel", "OrgLevel"+_description, new EnumValue("L1", 1),
                new EnumValue("L2", 2), new EnumValue("L3", 3));

        //Add attribute
        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("state", DataTypes.STRING_TYPE));

        //Add supertype
        HierarchicalTypeDefinition<ClassType> superTypeDef = createClassTypeDef("Division", ImmutableSet.<String>of(),
                createOptionalAttrDef("dname", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department", "Department"+_description,
            ImmutableSet.of(superTypeDef.typeName), createRequiredAttrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                        true, "department"));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(deptTypeDef, superTypeDef));

        Map<String, IDataType> typesAdded = ts.updateTypes(typesDef);
        typeStore.store(ts, ImmutableList.copyOf(typesAdded.keySet()));

        // ATLAS-474: verify that type update did not write duplicate edges to the type store.
        if (typeStore instanceof GraphBackedTypeStore) {
            GraphBackedTypeStore gbTypeStore = (GraphBackedTypeStore) typeStore;
            Vertex typeVertex = gbTypeStore.findVertex(TypeCategory.CLASS, "Department");
            int edgeCount = countOutgoingEdges(typeVertex, GraphBackedTypeStore.SUPERTYPE_EDGE_LABEL);
            Assert.assertEquals(edgeCount, 1);
            edgeCount = countOutgoingEdges(typeVertex, gbTypeStore.getEdgeLabel("Department", "employees"));
            Assert.assertEquals(edgeCount, 1, "Should only be 1 edge for employees attribute on Department type vertex");
        }
        
        //Validate the updated types
        TypesDef types = typeStore.restore();
        ts.reset();
        ts.defineTypes(types);

        //Assert new enum value
        EnumType orgLevel = ts.getDataType(EnumType.class, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.name, orgLevelEnum.name);
        Assert.assertEquals(orgLevel.description, orgLevelEnum.description);
        Assert.assertEquals(orgLevel.values().size(), orgLevelEnum.enumValues.length);
        Assert.assertEquals(orgLevel.fromValue("L3").ordinal, 3);

        //Assert new attribute
        StructType addressType = ts.getDataType(StructType.class, addressDetails.typeName);
        Assert.assertEquals(addressType.numFields, 3);
        Assert.assertEquals(addressType.fieldMapping.fields.get("state").dataType(), DataTypes.STRING_TYPE);

        //Assert new super type
        ClassType deptType = ts.getDataType(ClassType.class, deptTypeDef.typeName);
        Assert.assertTrue(deptType.superTypes.contains(superTypeDef.typeName));
        Assert.assertNotNull(ts.getDataType(ClassType.class, superTypeDef.typeName));
    }

    @Test(dependsOnMethods = "testTypeUpdate")
    public void testAddSecondSuperType() throws Exception {
        // Add a second supertype to Department class
        HierarchicalTypeDefinition<ClassType> superTypeDef2 = createClassTypeDef("SuperClass2", ImmutableSet.<String>of(),
                createOptionalAttrDef("name", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef("Department",
            ImmutableSet.of("Division", superTypeDef2.typeName), createRequiredAttrDef("name", DataTypes.STRING_TYPE),
            new AttributeDefinition("employees", String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                    true, "department"));
        TypesDef typesDef = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
            ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
            ImmutableList.of(deptTypeDef, superTypeDef2));
        ts.updateTypes(typesDef);
        typeStore.store(ts, ImmutableList.of(superTypeDef2.typeName, deptTypeDef.typeName));
        
        // ATLAS-474: verify that type update did not write duplicate edges to the type store.
        if (typeStore instanceof GraphBackedTypeStore) {
            GraphBackedTypeStore gbTypeStore = (GraphBackedTypeStore) typeStore;
            Vertex typeVertex = gbTypeStore.findVertex(TypeCategory.CLASS, "Department");
            // There should now be 2 super type outgoing edges on the Department type vertex.
            int edgeCount = countOutgoingEdges(typeVertex, GraphBackedTypeStore.SUPERTYPE_EDGE_LABEL);
            Assert.assertEquals(edgeCount, 2);
            // There should still be 1 outgoing edge for the employees attribute.
            edgeCount = countOutgoingEdges(typeVertex, gbTypeStore.getEdgeLabel("Department", "employees"));
            Assert.assertEquals(edgeCount, 1);
        }
        
        // Verify Department now has 2 super types.
        TypesDef types = typeStore.restore();
        for (HierarchicalTypeDefinition<ClassType> classTypeDef : types.classTypesAsJavaList()) {
            if (classTypeDef.typeName.equals("Department")) {
                Assert.assertEquals(classTypeDef.superTypes.size(), 2);
                Assert.assertTrue(classTypeDef.superTypes.containsAll(
                    Arrays.asList("Division", superTypeDef2.typeName)));
                break;
            }
        }
        ts.reset();
        Map<String, IDataType> typesMap = ts.defineTypes(types);
        IDataType dataType = typesMap.get(deptTypeDef.typeName);
        Assert.assertTrue(dataType instanceof ClassType);
        ClassType deptType = (ClassType) dataType;
        Assert.assertEquals(deptType.superTypes.size(), 2);
        Assert.assertTrue(deptType.superTypes.containsAll(
            Arrays.asList("Division", superTypeDef2.typeName)));
   }
    
    private int countOutgoingEdges(Vertex typeVertex, String edgeLabel) {

        Iterator<Edge> outGoingEdgesByLabel = GraphHelper.getOutGoingEdgesByLabel(typeVertex, edgeLabel);
        int edgeCount = 0;
        for (Iterator<Edge> iterator = outGoingEdgesByLabel; iterator.hasNext();) {
            iterator.next();
            edgeCount++;
        }
        return edgeCount;
    }
}
