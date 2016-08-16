/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.bridge;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.hive.HiveITBase;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class HiveMetastoreBridgeIT extends HiveITBase {

    @Test
    public void testCreateTableAndImport() throws Exception {
        String tableName = tableName();

        String pFile = createTestDFSPath("parentPath");
        final String query = String.format("create EXTERNAL table %s(id string, cnt int) location '%s'", tableName, pFile);
        runCommand(query);
        String dbId = assertDatabaseIsRegistered(DEFAULT_DB);
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        //verify lineage is created
        String processId = assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(),
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                getTableProcessQualifiedName(DEFAULT_DB, tableName), null);
        Referenceable processReference = atlasClient.getEntity(processId);
        validateHDFSPaths(processReference, INPUTS, pFile);

        List<Id> outputs = (List<Id>) processReference.get(OUTPUTS);
        assertEquals(outputs.size(), 1);
        assertEquals(outputs.get(0).getId()._getId(), tableId);

        int tableCount = atlasClient.listEntities(HiveDataTypes.HIVE_TABLE.getName()).size();

        //Now import using import tool - should be no-op. This also tests update since table exists
        hiveMetaStoreBridge.importTable(atlasClient.getEntity(dbId), DEFAULT_DB, tableName, true);
        String tableId2 = assertTableIsRegistered(DEFAULT_DB, tableName);
        assertEquals(tableId2, tableId);

        String processId2 = assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(),
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                getTableProcessQualifiedName(DEFAULT_DB, tableName), null);
        assertEquals(processId2, processId);

        //assert that table is de-duped and no new entity is created
        int newTableCount = atlasClient.listEntities(HiveDataTypes.HIVE_TABLE.getName()).size();
        assertEquals(newTableCount, tableCount);
    }

    @Test
    public void testImportCreatedTable() throws Exception {
        String tableName = tableName();
        String pFile = createTestDFSPath("parentPath");
        runCommand(driverWithoutContext, String.format("create EXTERNAL table %s(id string) location '%s'", tableName, pFile));
        String dbId = assertDatabaseIsRegistered(DEFAULT_DB);

        hiveMetaStoreBridge.importTable(atlasClient.getEntity(dbId), DEFAULT_DB, tableName, true);
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String processId = assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(),
                AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                getTableProcessQualifiedName(DEFAULT_DB, tableName), null);
        List<Id> outputs = (List<Id>) atlasClient.getEntity(processId).get(OUTPUTS);
        assertEquals(outputs.size(), 1);
        assertEquals(outputs.get(0).getId()._getId(), tableId);
    }
}
