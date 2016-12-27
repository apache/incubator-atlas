/*
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

package org.apache.atlas.query

import org.apache.atlas.TestUtils
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy
import org.apache.atlas.query.Expressions._class
import org.apache.atlas.query.Expressions.id
import org.apache.atlas.query.Expressions.int
import org.apache.atlas.repository.graph.AtlasGraphProvider
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository
import org.apache.atlas.repository.graphdb.AtlasGraph
import org.apache.atlas.typesystem.types.TypeSystem
import org.testng.annotations.AfterClass
import org.testng.annotations.BeforeClass
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test

class LineageQueryTest extends BaseGremlinTest {

    var g: AtlasGraph[_,_] = null
    var gp:GraphPersistenceStrategies = null;

    @BeforeMethod
    def resetRequestContext() {
        TestUtils.resetRequestContext()
    }


    @BeforeClass
    def beforeAll() {
        TypeSystem.getInstance().reset()
        var repo = new GraphBackedMetadataRepository(null);
        TestUtils.setupGraphProvider(repo);
        //force graph to be initialized first
        AtlasGraphProvider.getGraphInstance();
      
        //create types and indices up front.  Without this, some of the property keys (particularly __traitNames and __superTypes)
        //get ended up created implicitly with some graph backends with the wrong multiplicity.  This also makes the queries  
        //we execute perform better :-)
       QueryTestsUtils.setupTypesAndIndices()
    
       gp = new DefaultGraphPersistenceStrategy(repo);
       g = QueryTestsUtils.setupTestGraph(repo)
    }

    @AfterClass
    def afterAll() {
        AtlasGraphProvider.cleanup()
    }

    val PREFIX_SPACES_REGEX = ("\\n\\s*").r

  @Test def testInputTables {
        val r = QueryProcessor.evaluate(_class("LoadProcess").field("inputTables"), g, gp)
        val x = r.toJson
        validateJson(r,"""{
                         |  "query":"LoadProcess inputTables",
                         |  "dataType":{
                         |    "superTypes":[
                         |
                         |    ],
                         |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                         |    "typeName":"Table",
                         |    "attributeDefinitions":[
                         |      {
                         |        "name":"name",
                         |        "dataTypeName":"string",
                         |        "multiplicity":{
                         |          "lower":0,
                         |          "upper":1,
                         |          "isUnique":false
                         |        },
                         |        "isComposite":false,
                         |        "isUnique":false,
                         |        "isIndexable":false,
                         |        "reverseAttributeName":null
                         |      },
                         |      {
                         |        "name":"db",
                         |        "dataTypeName":"DB",
                         |        "multiplicity":{
                         |          "lower":1,
                         |          "upper":1,
                         |          "isUnique":false
                         |        },
                         |        "isComposite":false,
                         |        "isUnique":false,
                         |        "isIndexable":false,
                         |        "reverseAttributeName":null
                         |      },
                         |      {
                         |        "name":"sd",
                         |        "dataTypeName":"StorageDescriptor",
                         |        "multiplicity":{
                         |          "lower":1,
                         |          "upper":1,
                         |          "isUnique":false
                         |        },
                         |        "isComposite":false,
                         |        "isUnique":false,
                         |        "isIndexable":false,
                         |        "reverseAttributeName":null
                         |      },
                         |      {
                         |        "name":"created",
                         |        "dataTypeName":"date",
                         |        "multiplicity":{
                         |          "lower":0,
                         |          "upper":1,
                         |          "isUnique":false
                         |        },
                         |        "isComposite":false,
                         |        "isUnique":false,
                         |        "isIndexable":false,
                         |        "reverseAttributeName":null
                         |      }
                         |    ]
                         |  },
                         |  "rows":[
                         |    {
                         |      "$typeName$":"Table",
                         |      "$id$":{
                         |        "$typeName$":"Table",
                         |        "version":0
                         |      },
                         |      "created":"2014-12-11T02:35:58.440Z",
                         |      "sd":{
                         |        "$typeName$":"StorageDescriptor",
                         |        "version":0
                         |      },
                         |      "db":{
                         |        "$typeName$":"DB",
                         |        "version":0
                         |      },
                         |      "name":"sales_fact"
                         |    },
                         |    {
                         |      "$typeName$":"Table",
                         |      "$id$":{
                         |        "$typeName$":"Table",
                         |        "version":0
                         |      },
                         |      "created":"2014-12-11T02:35:58.440Z",
                         |      "sd":{
                         |        "$typeName$":"StorageDescriptor",
                         |        "version":0
                         |      },
                         |      "db":{
                         |        "$typeName$":"DB",
                         |        "version":0
                         |      },
                         |      "name":"time_dim",
                         |      "$traits$":{
                         |        "Dimension":{
                         |          "$typeName$":"Dimension"
                         |        }
                         |      }
                         |    },
                         |    {
                         |      "$typeName$":"Table",
                         |      "$id$":{
                         |        "$typeName$":"Table",
                         |        "version":0
                         |      },
                         |      "created":"2014-12-11T02:35:58.440Z",
                         |      "sd":{
                         |        "$typeName$":"StorageDescriptor",
                         |        "version":0
                         |      },
                         |      "db":{
                         |        "$typeName$":"DB",
                         |        "version":0
                         |      },
                         |      "name":"sales_fact_daily_mv"
                         |    }
                         |  ]
                         |}
          """.stripMargin)
    }

  @Test def testLoadProcessOut {
        val r = QueryProcessor.evaluate(_class("Table").field("LoadProcess").field("outputTable"), g, gp)
        validateJson(r, null)
    }

  @Test def testLineageAll {
        val r = QueryProcessor.evaluate(_class("Table").loop(id("LoadProcess").field("outputTable")), g, gp)
        validateJson(r, """{
                          |  "query":"Table as _loop0 loop (LoadProcess outputTable)",
                          |  "dataType":{
                          |    "superTypes":[
                          |
                          |    ],
                          |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                          |    "typeName":"Table",
                          |    "attributeDefinitions":[
                          |      {
                          |        "name":"name",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"db",
                          |        "dataTypeName":"DB",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"sd",
                          |        "dataTypeName":"StorageDescriptor",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"created",
                          |        "dataTypeName":"date",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      }
                          |    ]
                          |  },
                          |  "rows":[
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_daily_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_monthly_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_daily_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_monthly_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_monthly_mv"
                          |    }
                          |  ]
                          |}""".stripMargin)
    }

  @Test def testLineageAllSelect {
        val r = QueryProcessor.evaluate(_class("Table").as("src").loop(id("LoadProcess").field("outputTable")).as("dest").
            select(id("src").field("name").as("srcTable"), id("dest").field("name").as("destTable")), g, gp)
        validateJson(r, """{
  "query":"Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as destTable",
  "dataType":{
    "typeName":"__tempQueryResultStruct2",
    "attributeDefinitions":[
      {
        "name":"srcTable",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":false,
        "reverseAttributeName":null
      },
      {
        "name":"destTable",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":false,
        "reverseAttributeName":null
      }
    ]
  },
  "rows":[
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact",
      "destTable":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact",
      "destTable":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"time_dim",
      "destTable":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"time_dim",
      "destTable":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact_daily_mv",
      "destTable":"sales_fact_monthly_mv"
    }
  ]
}""".stripMargin)
    }

    @Test def testLineageFixedDepth {
        val r = QueryProcessor.evaluate(_class("Table").loop(id("LoadProcess").field("outputTable"), int(1)), g, gp)
        validateJson(r, """{
                          |  "query":"Table as _loop0 loop (LoadProcess outputTable) times 1",
                          |  "dataType":{
                          |    "superTypes":[
                          |
                          |    ],
                          |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                          |    "typeName":"Table",
                          |    "attributeDefinitions":[
                          |      {
                          |        "name":"name",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"db",
                          |        "dataTypeName":"DB",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"sd",
                          |        "dataTypeName":"StorageDescriptor",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"created",
                          |        "dataTypeName":"date",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":false,
                          |        "reverseAttributeName":null
                          |      }
                          |    ]
                          |  },
                          |  "rows":[
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_daily_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_daily_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDescriptor",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_monthly_mv"
                          |    }
                          |  ]
                          |}""".stripMargin)
    }
}