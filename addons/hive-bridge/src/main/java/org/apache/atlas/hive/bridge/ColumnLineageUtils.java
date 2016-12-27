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

package org.apache.atlas.hive.bridge;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnLineageUtils {
    public static final Logger LOG = LoggerFactory.getLogger(ColumnLineageUtils.class);
    public static class HiveColumnLineageInfo {
        public final String depenendencyType;
        public final String expr;
        public final String inputColumn;

        HiveColumnLineageInfo(LineageInfo.Dependency d, String inputCol) {
            depenendencyType = d.getType().name();
            expr = d.getExpr();
            inputColumn = inputCol;
        }

        @Override
        public String toString(){
            return inputColumn;
        }
    }

    public static String getQualifiedName(LineageInfo.DependencyKey key){
        String db = key.getDataContainer().getTable().getDbName();
        String table = key.getDataContainer().getTable().getTableName();
        String col = key.getFieldSchema().getName();
        return db + "." + table + "." + col;
    }

    public static Map<String, List<HiveColumnLineageInfo>> buildLineageMap(LineageInfo lInfo) {
        Map<String, List<HiveColumnLineageInfo>> m = new HashMap<>();

        for (Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> e : lInfo.entrySet()) {
            List<HiveColumnLineageInfo> l = new ArrayList<>();
            String k = getQualifiedName(e.getKey());
            for (LineageInfo.BaseColumnInfo iCol : e.getValue().getBaseCols()) {
                String db = iCol.getTabAlias().getTable().getDbName();
                String table = iCol.getTabAlias().getTable().getTableName();
                String colQualifiedName = iCol.getColumn() == null ? db + "." + table : db + "." + table + "." + iCol.getColumn().getName();
                l.add(new HiveColumnLineageInfo(e.getValue(), colQualifiedName));
            }
            LOG.debug("Setting lineage --> Input: {} ==> Output : {}", l, k);
            m.put(k, l);
        }
        return m;
    }

    static String[] extractComponents(String qualifiedName) {
        String[] comps = qualifiedName.split("\\.");
        int lastIdx = comps.length - 1;
        int atLoc = comps[lastIdx].indexOf('@');
        if (atLoc > 0) {
            comps[lastIdx] = comps[lastIdx].substring(0, atLoc);
        }
        return comps;
    }

    static void populateColumnReferenceableMap(Map<String, Referenceable> m,
                                               Referenceable r) {
        if (r.getTypeName().equals(HiveDataTypes.HIVE_TABLE.getName())) {
            String qName = (String) r.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME);
            String[] qNameComps = extractComponents(qName);
            for (Referenceable col : (List<Referenceable>) r.get(HiveMetaStoreBridge.COLUMNS)) {
                String cName = (String) col.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME);
                String[] colQNameComps = extractComponents(cName);
                String colQName = colQNameComps[0] + "." + colQNameComps[1] + "." + colQNameComps[2];
                m.put(colQName, col);
            }
            String tableQName = qNameComps[0] + "." + qNameComps[1];
            m.put(tableQName, r);
        }
    }


    public static Map<String, Referenceable> buildColumnReferenceableMap(List<Referenceable> inputs,
                                                                         List<Referenceable> outputs) {
        Map<String, Referenceable> m = new HashMap<>();

        for (Referenceable r : inputs) {
            populateColumnReferenceableMap(m, r);
        }

        for (Referenceable r : outputs) {
            populateColumnReferenceableMap(m, r);
        }

        return m;
    }
}
