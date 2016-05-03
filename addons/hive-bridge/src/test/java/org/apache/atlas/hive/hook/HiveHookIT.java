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

package org.apache.atlas.hive.hook;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.fs.model.FSDataTypes;
import org.apache.atlas.hive.bridge.HiveMetaStoreBridge;
import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization$;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.utils.ParamChecker;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.hive.hook.HiveHook.normalize;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class HiveHookIT {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HiveHookIT.class);

    private static final String DGI_URL = "http://localhost:21000/";
    private static final String CLUSTER_NAME = "test";
    public static final String DEFAULT_DB = "default";
    private Driver driver;
    private AtlasClient atlasClient;
    private HiveMetaStoreBridge hiveMetaStoreBridge;
    private SessionState ss;
    
    private static final String INPUTS = AtlasClient.PROCESS_ATTRIBUTE_INPUTS;
    private static final String OUTPUTS = AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS;

    @BeforeClass
    public void setUp() throws Exception {
        //Set-up hive session
        HiveConf conf = new HiveConf();
        //Run in local mode
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.default.name", "file:///'");
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());
        driver = new Driver(conf);
        ss = new SessionState(conf, System.getProperty("user.name"));
        ss = SessionState.start(ss);
        SessionState.setCurrentSessionState(ss);

        Configuration configuration = ApplicationProperties.get();
        atlasClient = new AtlasClient(configuration.getString(HiveMetaStoreBridge.ATLAS_ENDPOINT, DGI_URL));

        hiveMetaStoreBridge = new HiveMetaStoreBridge(conf, atlasClient);
        hiveMetaStoreBridge.registerHiveDataModel();

    }

    private void runCommand(String cmd) throws Exception {
        LOG.debug("Running command '{}'", cmd);
        ss.setCommandType(null);
        CommandProcessorResponse response = driver.run(cmd);
        assertEquals(response.getResponseCode(), 0);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1', 'p2'='v2')");
        String dbId = assertDatabaseIsRegistered(dbName);

        Referenceable definition = atlasClient.getEntity(dbId);
        Map params = (Map) definition.get(HiveDataModelGenerator.PARAMETERS);
        Assert.assertNotNull(params);
        Assert.assertEquals(params.size(), 2);
        Assert.assertEquals(params.get("p1"), "v1");

        //There should be just one entity per dbname
        runCommand("drop database " + dbName);
        assertDBIsNotRegistered(dbName);

        runCommand("create database " + dbName);
        String dbid = assertDatabaseIsRegistered(dbName);

        //assert on qualified name
        Referenceable dbEntity = atlasClient.getEntity(dbid);
        Assert.assertEquals(dbEntity.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), dbName.toLowerCase() + "@" + CLUSTER_NAME);

    }

    private String dbName() {
        return "db" + random();
    }

    private String createDatabase() throws Exception {
        String dbName = dbName();
        runCommand("create database " + dbName);
        return dbName;
    }

    private String tableName() {
        return "table" + random();
    }

    private String columnName() {
        return "col" + random();
    }

    private String createTable() throws Exception {
        return createTable(false);
    }

    private String createTable(boolean isPartitioned) throws Exception {
        String tableName = tableName();
        runCommand("create table " + tableName + "(id int, name string) comment 'table comment' " + (isPartitioned ?
                " partitioned by(dt string)" : ""));
        return tableName;
    }

    private String createTable(boolean isExternal, boolean isPartitioned, boolean isTemporary) throws Exception {
        String tableName = tableName();

        String location = "";
        if (isExternal) {
            location = " location '" +  createTestDFSPath("someTestPath") + "'";
        }
        runCommand("create " + (isExternal ? " EXTERNAL " : "") + (isTemporary ? "TEMPORARY " : "") + "table " + tableName + "(id int, name string) comment 'table comment' " + (isPartitioned ?
            " partitioned by(dt string)" : "") + location);
        return tableName;
    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        String colName = columnName();
        runCommand("create table " + dbName + "." + tableName + "(" + colName + " int, name string)");
        String tableId = assertTableIsRegistered(dbName, tableName);

        //there is only one instance of column registered
        String colId = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName), colName));
        Referenceable colEntity = atlasClient.getEntity(colId);
        Assert.assertEquals(colEntity.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), String.format("%s.%s.%s@%s", dbName.toLowerCase(),
                tableName.toLowerCase(), colName.toLowerCase(), CLUSTER_NAME));
        Assert.assertNotNull(colEntity.get(HiveDataModelGenerator.TABLE));
        Assert.assertEquals(((Id) colEntity.get(HiveDataModelGenerator.TABLE))._getId(), tableId);

        tableName = createTable();
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        Referenceable tableRef = atlasClient.getEntity(tableId);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.TABLE_TYPE_ATTR), TableType.MANAGED_TABLE.name());
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.COMMENT), "table comment");
        String entityName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.NAME), entityName);
        Assert.assertEquals(tableRef.get(HiveDataModelGenerator.NAME), "default." + tableName.toLowerCase() + "@" + CLUSTER_NAME);

        Table t = hiveMetaStoreBridge.hiveClient.getTable(DEFAULT_DB, tableName);
        long createTime = Long.parseLong(t.getMetadata().getProperty(hive_metastoreConstants.DDL_TIME)) * HiveMetaStoreBridge.MILLIS_CONVERT_FACTOR;

        verifyTimestamps(tableRef, HiveDataModelGenerator.CREATE_TIME, createTime);
        verifyTimestamps(tableRef, HiveDataModelGenerator.LAST_ACCESS_TIME, createTime);

        final Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_IS_STORED_AS_SUB_DIRS), false);
        Assert.assertNotNull(sdRef.get(HiveDataModelGenerator.TABLE));
        Assert.assertEquals(((Id) sdRef.get(HiveDataModelGenerator.TABLE))._getId(), tableId);

        //Create table where database doesn't exist, will create database instance as well
        assertDatabaseIsRegistered(DEFAULT_DB);
    }

    private void verifyTimestamps(Referenceable ref, String property, long expectedTime) throws ParseException {
        //Verify timestamps.
        String createTimeStr = (String) ref.get(property);
        Date createDate = TypeSystem.getInstance().getDateFormat().parse(createTimeStr);
        Assert.assertNotNull(createTimeStr);

        if (expectedTime > 0) {
            Assert.assertEquals(expectedTime, createDate.getTime());
        }
    }

    private void verifyTimestamps(Referenceable ref, String property) throws ParseException {
        verifyTimestamps(ref, property, 0);
    }

    @Test
    public void testCreateExternalTable() throws Exception {
        String tableName = tableName();
        String dbName = createDatabase();
        String colName = columnName();

        String pFile = createTestDFSPath("parentPath");
        final String query = String.format("create EXTERNAL table %s.%s( %s, %s) location '%s'", dbName , tableName , colName + " int", "name string",  pFile);
        runCommand(query);
        String tableId = assertTableIsRegistered(dbName, tableName);

        Referenceable processReference = validateProcess(query, 1, 1);

        verifyTimestamps(processReference, "startTime");
        verifyTimestamps(processReference, "endTime");

        validateHDFSPaths(processReference, pFile, INPUTS);
        validateOutputTables(processReference, tableId);
    }

    private void validateOutputTables(Referenceable processReference, String... expectedTableGuids) throws Exception {
       validateTables(processReference, OUTPUTS, expectedTableGuids);
    }

    private void validateInputTables(Referenceable processReference, String... expectedTableGuids) throws Exception {
        validateTables(processReference, INPUTS, expectedTableGuids);
    }

    private void validateTables(Referenceable processReference, String attrName, String... expectedTableGuids) throws Exception {
        List<Id> tableRef = (List<Id>) processReference.get(attrName);
        for(int i = 0; i < expectedTableGuids.length; i++) {
            Assert.assertEquals(tableRef.get(i)._getId(), expectedTableGuids[i]);
        }
    }

    private String assertColumnIsRegistered(String colName) throws Exception {
        return assertColumnIsRegistered(colName, null);
    }

    private String assertColumnIsRegistered(String colName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for column {}", colName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                colName, assertPredicate);
    }

    private String assertSDIsRegistered(String sdQFName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for sd {}", sdQFName.toLowerCase());
        return assertEntityIsRegistered(HiveDataTypes.HIVE_STORAGEDESC.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
            sdQFName.toLowerCase(), assertPredicate);
    }

    private void assertColumnIsNotRegistered(String colName) throws Exception {
        LOG.debug("Searching for column {}", colName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_COLUMN.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                colName);
    }

    @Test
    public void testCTAS() throws Exception {
        String tableName = createTable();
        String ctasTableName = "table" + random();
        String query = "create table " + ctasTableName + " as select * from " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
        assertTableIsRegistered(DEFAULT_DB, ctasTableName);
    }

    @Test
    public void testCreateView() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
        assertTableIsRegistered(DEFAULT_DB, viewName);
    }

    @Test
    public void testAlterViewAsSelect() throws Exception {

        //Create the view from table1
        String table1Name = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + table1Name;
        runCommand(query);

        String table1Id = assertTableIsRegistered(DEFAULT_DB, table1Name);
        assertProcessIsRegistered(query);
        String viewId = assertTableIsRegistered(DEFAULT_DB, viewName);

        //Check lineage which includes table1
        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(viewId));
        Assert.assertTrue(vertices.has(table1Id));

        //Alter the view from table2
        String table2Name = createTable();
        query = "alter view " + viewName + " as select * from " + table2Name;
        runCommand(query);

        //Check if alter view process is reqistered
        assertProcessIsRegistered(query);
        String table2Id = assertTableIsRegistered(DEFAULT_DB, table2Name);
        Assert.assertEquals(assertTableIsRegistered(DEFAULT_DB, viewName), viewId);

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName);
        response = atlasClient.getInputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(viewId));

        //This is through the alter view process
        Assert.assertTrue(vertices.has(table2Id));

        //This is through the Create view process
        Assert.assertTrue(vertices.has(table1Id));

        //Outputs dont exist
        response = atlasClient.getOutputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertEquals(vertices.length(), 0);
    }

    private String createTestDFSPath(String path) throws Exception {
        return "pfile://" + mkdir(path);
    }

    private String createTestDFSFile(String path) throws Exception {
        return "pfile://" + file(path);
    }

    @Test
    public void testLoadLocalPath() throws Exception {
        String tableName = createTable(false);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName;
        runCommand(query);

        assertProcessIsRegistered(query);
    }

    @Test
    public void testLoadLocalPathIntoPartition() throws Exception {
        String tableName = createTable(true);

        String loadFile = file("load");
        String query = "load data local inpath 'file://" + loadFile + "' into table " + tableName +  " partition(dt = '2015-01-01')";
        runCommand(query);

        validateProcess(query, 0, 1);
    }

    @Test
    public void testLoadDFSPath() throws Exception {
        String tableName = createTable(true, true, false);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String loadFile = createTestDFSFile("loadDFSFile");
        String query = "load data inpath '" + loadFile + "' into table " + tableName + " partition(dt = '2015-01-01')";
        runCommand(query);

        Referenceable processReference = validateProcess(query, 1, 1);

        validateHDFSPaths(processReference, loadFile, INPUTS);

        validateOutputTables(processReference, tableId);
    }

    private Referenceable validateProcess(String query, int numInputs, int numOutputs) throws Exception {
        String processId = assertProcessIsRegistered(query);
        Referenceable process = atlasClient.getEntity(processId);
        if (numInputs == 0) {
            Assert.assertNull(process.get(INPUTS));
        } else {
            Assert.assertEquals(((List<Referenceable>) process.get(INPUTS)).size(), numInputs);
        }

        if (numOutputs == 0) {
            Assert.assertNull(process.get(OUTPUTS));
        } else {
            Assert.assertEquals(((List<Id>) process.get(OUTPUTS)).size(), numOutputs);
        }

        return process;
    }

    private Referenceable validateProcess(String query, String[] inputs, String[] outputs) throws Exception {
        String processId = assertProcessIsRegistered(query);
        Referenceable process = atlasClient.getEntity(processId);
        if (inputs == null) {
            Assert.assertNull(process.get(INPUTS));
        } else {
            Assert.assertEquals(((List<Referenceable>) process.get(INPUTS)).size(), inputs.length);
            validateInputTables(process, inputs);
        }

        if (outputs == null) {
            Assert.assertNull(process.get(OUTPUTS));
        } else {
            Assert.assertEquals(((List<Id>) process.get(OUTPUTS)).size(), outputs.length);
            validateOutputTables(process, outputs);
        }

        return process;
    }

    @Test
    public void testInsertIntoTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable();
        String query =
                "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);

        String inputTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);

        validateProcess(query, new String[]{inputTableId}, new String[]{opTableId});
    }

    @Test
    public void testInsertIntoLocalDir() throws Exception {
        String tableName = createTable();
        File randomLocalPath = File.createTempFile("hiverandom", ".tmp");
        String query =
            "insert overwrite LOCAL DIRECTORY '" + randomLocalPath.getAbsolutePath() + "' select id, name from " + tableName;

        runCommand(query);
        validateProcess(query, 1, 0);

        assertTableIsRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testInsertIntoDFSDir() throws Exception {
        String tableName = createTable();
        String pFile = createTestDFSPath("somedfspath");
        String query =
            "insert overwrite DIRECTORY '" + pFile  + "' select id, name from " + tableName;

        runCommand(query);
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, pFile, OUTPUTS);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        validateInputTables(processReference, tableId);
    }

    @Test
    public void testInsertIntoTempTable() throws Exception {
        String tableName = createTable();
        String insertTableName = createTable(false, false, true);

        String query =
            "insert into " + insertTableName + " select id, name from " + tableName;

        runCommand(query);
        validateProcess(query, 1, 1);

        String ipTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);
        validateProcess(query, new String[]{ipTableId}, new String[]{opTableId});
    }

    @Test
    public void testInsertIntoPartition() throws Exception {
        String tableName = createTable(true);
        String insertTableName = createTable(true);
        String query =
            "insert into " + insertTableName + " partition(dt = '2015-01-01') select id, name from " + tableName
                + " where dt = '2015-01-01'";
        runCommand(query);
        validateProcess(query, 1, 1);

        String ipTableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        String opTableId = assertTableIsRegistered(DEFAULT_DB, insertTableName);
        validateProcess(query, new String[]{ipTableId}, new String[]{opTableId});
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }

    private String file(String tag) throws Exception {
        String filename = "./target/" + tag + "-data-" + random();
        File file = new File(filename);
        file.createNewFile();
        return file.getAbsolutePath();
    }

    private String mkdir(String tag) throws Exception {
        String filename = "./target/" + tag + "-data-" + random();
        File file = new File(filename);
        file.mkdirs();
        return file.getAbsolutePath();
    }

    @Test
    public void testExportImportUnPartitionedTable() throws Exception {
        String tableName = createTable(false);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        String filename = "pfile://" + mkdir("export");
        String query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, OUTPUTS);
        validateInputTables(processReference, tableId);

        //Import
        tableName = createTable(false);
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, INPUTS);

        validateOutputTables(processReference, tableId);
    }

    @Test
    public void testExportImportPartitionedTable() throws Exception {
        String tableName = createTable(true);
        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        //Add a partition
        String partFile = "pfile://" + mkdir("partition");
        String query = "alter table " + tableName + " add partition (dt='2015-01-01') location '" + partFile + "'";
        runCommand(query);

        String filename = "pfile://" + mkdir("export");
        query = "export table " + tableName + " to \"" + filename + "\"";
        runCommand(query);
        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, OUTPUTS);

        validateInputTables(processReference, tableId);

        //Import
        tableName = createTable(true);
        tableId = assertTableIsRegistered(DEFAULT_DB, tableName);

        query = "import table " + tableName + " from '" + filename + "'";
        runCommand(query);
        processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, filename, INPUTS);

        validateOutputTables(processReference, tableId);
    }

    @Test
    public void testIgnoreSelect() throws Exception {
        String tableName = createTable();
        String query = "select * from " + tableName;
        runCommand(query);
        assertProcessIsNotRegistered(query);

        //check with uppercase table name
        query = "SELECT * from " + tableName.toUpperCase();
        runCommand(query);
        assertProcessIsNotRegistered(query);
    }

    @Test
    public void testAlterTableRename() throws Exception {
        String tableName = createTable(true);
        final String newDBName = createDatabase();

        assertTableIsRegistered(DEFAULT_DB, tableName);
        String columnGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), HiveDataModelGenerator.NAME));
        String sdGuid = assertSDIsRegistered(HiveMetaStoreBridge.getStorageDescQFName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName)), null);
        assertDatabaseIsRegistered(newDBName);

        //Add trait to column
        String colTraitDetails = createTrait(columnGuid);

        //Add trait to sd
        String sdTraitDetails = createTrait(sdGuid);

        String partColumnGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "dt"));
        //Add trait to part col keys
        String partColTraitDetails = createTrait(partColumnGuid);

        String newTableName = tableName();
        String query = String.format("alter table %s rename to %s", DEFAULT_DB + "." + tableName, newDBName + "." + newTableName);
        runCommand(query);

        String newColGuid = assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, newTableName), HiveDataModelGenerator.NAME));
        Assert.assertEquals(newColGuid, columnGuid);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, tableName), HiveDataModelGenerator.NAME));

        assertTrait(columnGuid, colTraitDetails);
        String newSdGuid = assertSDIsRegistered(HiveMetaStoreBridge.getStorageDescQFName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, newDBName, newTableName)), null);
        Assert.assertEquals(newSdGuid, sdGuid);

        assertTrait(sdGuid, sdTraitDetails);
        assertTrait(partColumnGuid, partColTraitDetails);

        assertTableIsNotRegistered(DEFAULT_DB, tableName);
        assertTableIsRegistered(newDBName, newTableName);
    }

    private List<Referenceable> getColumns(String dbName, String tableName) throws Exception {
        String tableId = assertTableIsRegistered(dbName, tableName);
        Referenceable tableRef = atlasClient.getEntity(tableId);
        return ((List<Referenceable>)tableRef.get(HiveDataModelGenerator.COLUMNS));
    }


    private String createTrait(String guid) throws AtlasServiceException, JSONException {
        //add trait
        String traitName = "PII_Trait" + RandomStringUtils.random(10);
        atlasClient.createTraitType(traitName);

        Struct traitInstance = new Struct(traitName);
        atlasClient.addTrait(guid, traitInstance);
        return traitName;
    }

    private void assertTrait(String guid, String traitName) throws AtlasServiceException, JSONException {
        List<String> traits = atlasClient.listTraits(guid);
        Assert.assertEquals(traits.get(0), traitName);
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        String tableName = createTable();
        String column = columnName();
        String query = "alter table " + tableName + " add columns (" + column + " string)";
        runCommand(query);

        assertColumnIsRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                    column));

        //Verify the number of columns present in the table
        final List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 3);
    }

    @Test
    public void testAlterTableDropColumn() throws Exception {
        String tableName = createTable();
        final String colDropped = "id";
        String query = "alter table " + tableName + " replace columns (name string)";
        runCommand(query);

        assertColumnIsNotRegistered(HiveMetaStoreBridge
            .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                colDropped));

        //Verify the number of columns present in the table
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                List<Referenceable> columns = (List<Referenceable>) tableRef.get(HiveDataModelGenerator.COLUMNS);
                Assert.assertEquals(columns.size(), 1);
                Assert.assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), HiveDataModelGenerator.NAME);

            }
        });
    }

    @Test
    public void testAlterTableChangeColumn() throws Exception {
        //Change name
        String oldColName = HiveDataModelGenerator.NAME;
        String newColName = "name1";
        String tableName = createTable();
        String query = String.format("alter table %s change %s %s string", tableName, oldColName, newColName);
        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName));

        //Verify the number of columns present in the table
        List<Referenceable> columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        //Change column type
        oldColName = "name1";
        newColName = "name2";
        final String newColType = "int";
        query = String.format("alter table %s change column %s %s %s", tableName, oldColName, newColName, newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        String newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                assertEquals(entity.get("type"), "int");
            }
        });

        //Change name and add comment
        oldColName = "name2";
        newColName = "name3";
        final String comment = "added comment";
        query = String.format("alter table %s change column %s %s %s COMMENT '%s' after id", tableName, oldColName,
            newColName, newColType, comment);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));
        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);

        assertColumnIsRegistered(newColQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                assertEquals(entity.get(HiveDataModelGenerator.COMMENT), comment);
            }
        });

        //Change column position
        oldColName = "name3";
        newColName = "name4";
        query = String.format("alter table %s change column %s %s %s first", tableName, oldColName, newColName,
                newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        final String finalNewColName = newColName;
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    List<Referenceable> columns = (List<Referenceable>) entity.get(HiveDataModelGenerator.COLUMNS);
                    assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), finalNewColName);
                    assertEquals(columns.get(1).get(HiveDataModelGenerator.NAME), "id");
                }
            }
        );

        //Change col position again
        oldColName = "name4";
        newColName = "name5";
        query = String.format("alter table %s change column %s %s %s after id", tableName, oldColName, newColName, newColType);
        runCommand(query);

        columns = getColumns(DEFAULT_DB, tableName);
        Assert.assertEquals(columns.size(), 2);

        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), oldColName));

        newColQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), newColName);
        assertColumnIsRegistered(newColQualifiedName);

        //Check col position
        final String finalNewColName2 = newColName;
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    List<Referenceable> columns = (List<Referenceable>) entity.get(HiveDataModelGenerator.COLUMNS);
                    assertEquals(columns.get(1).get(HiveDataModelGenerator.NAME), finalNewColName2);
                    assertEquals(columns.get(0).get(HiveDataModelGenerator.NAME), "id");
                }
            }
        );
    }

    @Test
    public void testTruncateTable() throws Exception {
        String tableName = createTable(false);
        String query = String.format("truncate table %s", tableName);
        runCommand(query);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName);
        validateProcess(query, 0, 1);

        //Check lineage
        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        //Below should be assertTrue - Fix https://issues.apache.org/jira/browse/ATLAS-653
        Assert.assertFalse(vertices.has(tableId));
    }

    @Test
    public void testAlterTablePartitionColumnType() throws Exception {
        String tableName = createTable(true, true, false);
        final String newType = "int";
        String query = String.format("ALTER TABLE %s PARTITION COLUMN (dt %s)", tableName, newType);
        runCommand(query);

        String colQualifiedName = HiveMetaStoreBridge.getColumnQualifiedName(
            HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "dt");
        final String dtColId = assertColumnIsRegistered(colQualifiedName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable column) throws Exception {
                Assert.assertEquals(column.get("type"), newType);
            }
        });

        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable table) throws Exception {
                final List<Referenceable> partitionKeys = (List<Referenceable>) table.get("partitionKeys");
                Assert.assertEquals(partitionKeys.size(), 1);
                Assert.assertEquals(partitionKeys.get(0).getId()._getId(), dtColId);

            }
        });
    }

    @Test
    public void testAlterViewRename() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String newName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        query = "alter view " + viewName + " rename to " + newName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, newName);
        assertTableIsNotRegistered(DEFAULT_DB, viewName);
    }

    @Test
    public void testAlterTableLocation() throws Exception {
        //Its an external table, so the HDFS location should also be registered as an entity
        String tableName = createTable(true, true, false);
        final String testPath = createTestDFSPath("testBaseDir");
        String query = "alter table " + tableName + " set location '" + testPath + "'";
        runCommand(query);

        String tableId = assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Assert.assertEquals(new Path((String)sdRef.get("location")).toString(), new Path(testPath).toString());
            }
        });

        Referenceable processReference = validateProcess(query, 1, 1);
        validateHDFSPaths(processReference, testPath, INPUTS);

        validateOutputTables(processReference, tableId);
    }

    private String validateHDFSPaths(Referenceable processReference, String testPath, String attributeName) throws Exception {
        List<Id> hdfsPathRefs = (List<Id>) processReference.get(attributeName);

        final String testPathNormed = normalize(new Path(testPath).toString());
        String hdfsPathId = assertHDFSPathIsRegistered(testPathNormed);
        Assert.assertEquals(hdfsPathRefs.get(0)._getId(), hdfsPathId);

        Referenceable hdfsPathRef = atlasClient.getEntity(hdfsPathId);
        Assert.assertEquals(hdfsPathRef.get("path"), testPathNormed);
        Assert.assertEquals(hdfsPathRef.get(HiveDataModelGenerator.NAME), testPathNormed);
//        Assert.assertEquals(hdfsPathRef.get("name"), new Path(testPath).getName());
        Assert.assertEquals(hdfsPathRef.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), testPathNormed);

        return hdfsPathRef.getId()._getId();
    }

    private String assertHDFSPathIsRegistered(String path) throws Exception {
        LOG.debug("Searching for hdfs path {}", path);
        return assertEntityIsRegistered(FSDataTypes.HDFS_PATH().toString(), HiveDataModelGenerator.NAME, path, null);
    }

    @Test
    public void testAlterTableFileFormat() throws Exception {
        String tableName = createTable();
        final String testFormat = "orc";
        String query = "alter table " + tableName + " set FILEFORMAT " + testFormat;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT),
                    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT),
                    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
                Assert.assertNotNull(sdRef.get("serdeInfo"));

                Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
                Assert.assertEquals(serdeInfo.get("serializationLib"), "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
                Assert.assertNotNull(serdeInfo.get(HiveDataModelGenerator.PARAMETERS));
                Assert.assertEquals(
                    ((Map<String, String>) serdeInfo.get(HiveDataModelGenerator.PARAMETERS))
                        .get("serialization.format"),
                    "1");
            }
        });


        /**
         * Hive 'alter table stored as' is not supported - See https://issues.apache.org/jira/browse/HIVE-9576
         * query = "alter table " + tableName + " STORED AS " + testFormat.toUpperCase();
         * runCommand(query);

         * tableRef = atlasClient.getEntity(tableId);
         * sdRef = (Referenceable)tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_INPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
         * Assert.assertEquals(sdRef.get(HiveDataModelGenerator.STORAGE_DESC_OUTPUT_FMT), "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
         * Assert.assertEquals(((Map) sdRef.get(HiveDataModelGenerator.PARAMETERS)).get("orc.compress"), "ZLIB");
         */
    }

    @Test
    public void testAlterTableBucketingClusterSort() throws Exception {
        String tableName = createTable();
        ImmutableList<String> cols = ImmutableList.of("id");
        runBucketSortQuery(tableName, 5, cols, cols);

        cols = ImmutableList.of("id", HiveDataModelGenerator.NAME);
        runBucketSortQuery(tableName, 2, cols, cols);
    }

    private void runBucketSortQuery(String tableName, final int numBuckets,  final ImmutableList<String> bucketCols,
                                    final ImmutableList<String> sortCols) throws Exception {
        final String fmtQuery = "alter table %s CLUSTERED BY (%s) SORTED BY (%s) INTO %s BUCKETS";
        String query = String.format(fmtQuery, tableName, stripListBrackets(bucketCols.toString()),
                stripListBrackets(sortCols.toString()), numBuckets);
        runCommand(query);
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) throws Exception {
                verifyBucketSortingProperties(entity, numBuckets, bucketCols, sortCols);
            }
        });
    }

    private String stripListBrackets(String listElements) {
        return StringUtils.strip(StringUtils.strip(listElements, "["), "]");
    }

    private void verifyBucketSortingProperties(Referenceable tableRef, int numBuckets,
                                               ImmutableList<String> bucketColNames,
                                               ImmutableList<String>  sortcolNames) throws Exception {
        Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
        Assert.assertEquals(((scala.math.BigInt) sdRef.get(HiveDataModelGenerator.STORAGE_NUM_BUCKETS)).intValue(),
                numBuckets);
        Assert.assertEquals(sdRef.get("bucketCols"), bucketColNames);

        List<Struct> hiveOrderStructList = (List<Struct>) sdRef.get("sortCols");
        Assert.assertNotNull(hiveOrderStructList);
        Assert.assertEquals(hiveOrderStructList.size(), sortcolNames.size());

        for (int i = 0; i < sortcolNames.size(); i++) {
            Assert.assertEquals(hiveOrderStructList.get(i).get("col"), sortcolNames.get(i));
            Assert.assertEquals(((scala.math.BigInt) hiveOrderStructList.get(i).get("order")).intValue(), 1);
        }
    }

    @Test
    public void testAlterTableSerde() throws Exception {
        //SERDE PROPERTIES
        String tableName = createTable();
        Map<String, String> expectedProps = new HashMap<String, String>() {{
            put("key1", "value1");
        }};

        runSerdePropsQuery(tableName, expectedProps);

        expectedProps.put("key2", "value2");

        //Add another property
        runSerdePropsQuery(tableName, expectedProps);
    }

    @Test
    public void testDropTable() throws Exception {
        //Test Deletion of tables and its corrresponding columns
        String tableName = createTable(true, true, false);

        assertTableIsRegistered(DEFAULT_DB, tableName);
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), "id"));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName), HiveDataModelGenerator.NAME));

        final String query = String.format("drop table %s ", tableName);
        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                    "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, tableName),
                    HiveDataModelGenerator.NAME));
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
    }

    @Test
    public void testDropDatabaseWithCascade() throws Exception {
        //Test Deletion of database and its corresponding tables
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1')");

        final int numTables = 10;
        String[] tableNames = new String[numTables];
        for(int i = 0; i < numTables; i++) {
            tableNames[i] = createTable(true, true, false);
        }

        final String query = String.format("drop database %s cascade", dbName);
        runCommand(query);

        //Verify columns are not registered for one of the tables
        assertColumnIsNotRegistered(HiveMetaStoreBridge.getColumnQualifiedName(
                HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableNames[0]), "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableNames[0]),
                    HiveDataModelGenerator.NAME));

        for(int i = 0; i < numTables; i++) {
            assertTableIsNotRegistered(dbName, tableNames[i]);
        }
        assertDBIsNotRegistered(dbName);
    }

    @Test
    public void testDropDatabaseWithoutCascade() throws Exception {
        //Test Deletion of database and its corresponding tables
        String dbName = "db" + random();
        runCommand("create database " + dbName + " WITH DBPROPERTIES ('p1'='v1')");

        final int numTables = 10;
        String[] tableNames = new String[numTables];
        for(int i = 0; i < numTables; i++) {
            tableNames[i] = createTable(true, true, false);
            String query = String.format("drop table %s", tableNames[i]);
            runCommand(query);
            assertTableIsNotRegistered(dbName, tableNames[i]);
        }

        final String query = String.format("drop database %s", dbName);
        runCommand(query);

        assertDBIsNotRegistered(dbName);
    }

    @Test
    public void testDropNonExistingDB() throws Exception {
        //Test Deletion of a non existing DB
        final String dbName = "nonexistingdb";
        assertDBIsNotRegistered(dbName);
        final String query = String.format("drop database if exists %s cascade", dbName);
        runCommand(query);

        //Should have no effect
        assertDBIsNotRegistered(dbName);
        assertProcessIsNotRegistered(query);
    }

    @Test
    public void testDropNonExistingTable() throws Exception {
        //Test Deletion of a non existing table
        final String tableName = "nonexistingtable";
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
        final String query = String.format("drop table if exists %s", tableName);
        runCommand(query);

        //Should have no effect
        assertTableIsNotRegistered(DEFAULT_DB, tableName);
        assertProcessIsNotRegistered(query);
    }

    @Test
    public void testDropView() throws Exception {
        //Test Deletion of tables and its corrresponding columns
        String tableName = createTable(true, true, false);
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        assertTableIsRegistered(DEFAULT_DB, viewName);
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName), "id"));
        assertColumnIsRegistered(HiveMetaStoreBridge.getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName), HiveDataModelGenerator.NAME));

        query = String.format("drop view %s ", viewName);

        runCommand(query);
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName),
                    "id"));
        assertColumnIsNotRegistered(HiveMetaStoreBridge
                .getColumnQualifiedName(HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, viewName),
                    HiveDataModelGenerator.NAME));
        assertTableIsNotRegistered(DEFAULT_DB, viewName);
    }

    private void runSerdePropsQuery(String tableName, Map<String, String> expectedProps) throws Exception {

        final String serdeLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

        final String serializedProps = getSerializedProps(expectedProps);
        String query = String.format("alter table %s set SERDE '%s' WITH SERDEPROPERTIES (%s)", tableName, serdeLib, serializedProps);
        runCommand(query);

        verifyTableSdProperties(tableName, serdeLib, expectedProps);
    }

    private String getSerializedProps(Map<String, String> expectedProps) {
        StringBuilder sb = new StringBuilder();
        for(String expectedPropKey : expectedProps.keySet()) {
            if(sb.length() > 0) {
                sb.append(",");
            }
            sb.append("'").append(expectedPropKey).append("'");
            sb.append("=");
            sb.append("'").append(expectedProps.get(expectedPropKey)).append("'");
        }
        return sb.toString();
    }

    @Test
    public void testAlterDBOwner() throws Exception {
        String dbName = createDatabase();
        assertDatabaseIsRegistered(dbName);

        final String owner = "testOwner";
        final String fmtQuery = "alter database %s set OWNER %s %s";
        String query = String.format(fmtQuery, dbName, "USER", owner);

        runCommand(query);

        assertDatabaseIsRegistered(dbName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable entity) {
                assertEquals(entity.get(HiveDataModelGenerator.OWNER), owner);
            }
        });
    }

    @Test
    public void testAlterDBProperties() throws Exception {
        String dbName = createDatabase();
        final String fmtQuery = "alter database %s %s DBPROPERTIES (%s)";
        testAlterProperties(Entity.Type.DATABASE, dbName, fmtQuery);
    }

    @Test
    public void testAlterTableProperties() throws Exception {
        String tableName = createTable();
        final String fmtQuery = "alter table %s %s TBLPROPERTIES (%s)";
        testAlterProperties(Entity.Type.TABLE, tableName, fmtQuery);
    }

    private void testAlterProperties(Entity.Type entityType, String entityName, String fmtQuery) throws Exception {
        final String SET_OP = "set";
        final String UNSET_OP = "unset";

        final Map<String, String> expectedProps = new HashMap<String, String>() {{
            put("testPropKey1", "testPropValue1");
            put("comment", "test comment");
        }};

        String query = String.format(fmtQuery, entityName, SET_OP, getSerializedProps(expectedProps));
        runCommand(query);
        verifyEntityProperties(entityType, entityName, expectedProps, false);

        expectedProps.put("testPropKey2", "testPropValue2");
        //Add another property
        query = String.format(fmtQuery, entityName, SET_OP, getSerializedProps(expectedProps));
        runCommand(query);
        verifyEntityProperties(entityType, entityName, expectedProps, false);

        if (entityType != Entity.Type.DATABASE) {
            //Database unset properties doesnt work strangely - alter database %s unset DBPROPERTIES doesnt work
            //Unset all the props
            StringBuilder sb = new StringBuilder("'");
            query = String.format(fmtQuery, entityName, UNSET_OP, Joiner.on("','").skipNulls().appendTo(sb, expectedProps.keySet()).append('\''));
            runCommand(query);

            verifyEntityProperties(entityType, entityName, expectedProps, true);
        }
    }

    @Test
    public void testAlterViewProperties() throws Exception {
        String tableName = createTable();
        String viewName = tableName();
        String query = "create view " + viewName + " as select * from " + tableName;
        runCommand(query);

        final String fmtQuery = "alter view %s %s TBLPROPERTIES (%s)";
        testAlterProperties(Entity.Type.TABLE, viewName, fmtQuery);
    }

    private void verifyEntityProperties(Entity.Type type, String entityName, final Map<String, String> expectedProps,
                                        final boolean checkIfNotExists) throws Exception {
        switch(type) {
        case TABLE:
            assertTableIsRegistered(DEFAULT_DB, entityName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    verifyProperties(entity, expectedProps, checkIfNotExists);
                }
            });
            break;
        case DATABASE:
            assertDatabaseIsRegistered(entityName, new AssertPredicate() {
                @Override
                public void assertOnEntity(Referenceable entity) throws Exception {
                    verifyProperties(entity, expectedProps, checkIfNotExists);
                }
            });
            break;
        }
    }

    private void verifyTableSdProperties(String tableName, final String serdeLib, final Map<String, String> expectedProps) throws Exception {
        assertTableIsRegistered(DEFAULT_DB, tableName, new AssertPredicate() {
            @Override
            public void assertOnEntity(Referenceable tableRef) throws Exception {
                Referenceable sdRef = (Referenceable) tableRef.get(HiveDataModelGenerator.STORAGE_DESC);
                Struct serdeInfo = (Struct) sdRef.get("serdeInfo");
                Assert.assertEquals(serdeInfo.get("serializationLib"), serdeLib);
                verifyProperties(serdeInfo, expectedProps, false);
            }
        });
    }

    private void verifyProperties(Struct referenceable, Map<String, String> expectedProps, boolean checkIfNotExists) {
        Map<String, String> parameters = (Map<String, String>) referenceable.get(HiveDataModelGenerator.PARAMETERS);

        if (checkIfNotExists == false) {
            //Check if properties exist
            Assert.assertNotNull(parameters);
            for (String propKey : expectedProps.keySet()) {
                Assert.assertEquals(parameters.get(propKey), expectedProps.get(propKey));
            }
        } else {
            //Check if properties dont exist
            if (expectedProps != null && parameters != null) {
                for (String propKey : expectedProps.keySet()) {
                    Assert.assertFalse(parameters.containsKey(propKey));
                }
            }
        }
    }

    private String assertProcessIsRegistered(String queryStr) throws Exception {
        LOG.debug("Searching for process with query {}", queryStr);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.NAME, normalize(queryStr), null);
    }

    private void assertProcessIsNotRegistered(String queryStr) throws Exception {
        LOG.debug("Searching for process with query {}", queryStr);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_PROCESS.getName(), AtlasClient.NAME, normalize(queryStr));
    }

    private void assertTableIsNotRegistered(String dbName, String tableName) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.NAME, tableQualifiedName);
    }

    private void assertDBIsNotRegistered(String dbName) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(CLUSTER_NAME, dbName);
        assertEntityIsNotRegistered(HiveDataTypes.HIVE_DB.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, dbQualifiedName);
    }

    private String assertTableIsRegistered(String dbName, String tableName) throws Exception {
        return assertTableIsRegistered(dbName, tableName, null);
    }

    private String assertTableIsRegistered(String dbName, String tableName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for table {}.{}", dbName, tableName);
        String tableQualifiedName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, dbName, tableName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_TABLE.getName(), AtlasClient.NAME, tableQualifiedName,
                assertPredicate);
    }

    private String assertDatabaseIsRegistered(String dbName) throws Exception {
        return assertDatabaseIsRegistered(dbName, null);
    }

    private String assertDatabaseIsRegistered(String dbName, AssertPredicate assertPredicate) throws Exception {
        LOG.debug("Searching for database {}", dbName);
        String dbQualifiedName = HiveMetaStoreBridge.getDBQualifiedName(CLUSTER_NAME, dbName);
        return assertEntityIsRegistered(HiveDataTypes.HIVE_DB.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                dbQualifiedName, assertPredicate);
    }

    private String assertEntityIsRegistered(final String typeName, final String property, final String value,
                                            final AssertPredicate assertPredicate) throws Exception {
        waitFor(80000, new Predicate() {
            @Override
            public void evaluate() throws Exception {
                Referenceable entity = atlasClient.getEntity(typeName, property, value);
                assertNotNull(entity);
                if(assertPredicate != null) {
                    assertPredicate.assertOnEntity(entity);
                }
            }
        });
        Referenceable entity = atlasClient.getEntity(typeName, property, value);
        return entity.getId()._getId();
    }

    private void assertEntityIsNotRegistered(final String typeName, final String property, final String value) throws Exception {
        waitFor(80000, new Predicate() {
            @Override
            public void evaluate() throws Exception {
                try {
                    atlasClient.getEntity(typeName, property, value);
                } catch (AtlasServiceException e) {
                    if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                        return;
                    }
                }
                fail(String.format("Entity was not supposed to exist for typeName = %s, attributeName = %s, "
                    + "attributeValue = %s", typeName, property, value));
            }
        });
    }

    @Test
    public void testLineage() throws Exception {
        String table1 = createTable(false);

        String db2 = createDatabase();
        String table2 = tableName();

        String query = String.format("create table %s.%s as select * from %s", db2, table2, table1);
        runCommand(query);
        String table1Id = assertTableIsRegistered(DEFAULT_DB, table1);
        String table2Id = assertTableIsRegistered(db2, table2);

        String datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, db2, table2);
        JSONObject response = atlasClient.getInputGraph(datasetName);
        JSONObject vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(table1Id));
        Assert.assertTrue(vertices.has(table2Id));

        datasetName = HiveMetaStoreBridge.getTableQualifiedName(CLUSTER_NAME, DEFAULT_DB, table1);
        response = atlasClient.getOutputGraph(datasetName);
        vertices = response.getJSONObject("values").getJSONObject("vertices");
        Assert.assertTrue(vertices.has(table1Id));
        Assert.assertTrue(vertices.has(table2Id));
    }

    //For ATLAS-448
    @Test
    public void testNoopOperation() throws Exception {
        runCommand("show compactions");
        runCommand("show transactions");
    }

    public interface AssertPredicate {
        void assertOnEntity(Referenceable entity) throws Exception;
    }

    public interface Predicate {
        /**
         * Perform a predicate evaluation.
         *
         * @return the boolean result of the evaluation.
         * @throws Exception thrown if the predicate evaluation could not evaluate.
         */
        void evaluate() throws Exception;
    }

    /**
     * Wait for a condition, expressed via a {@link Predicate} to become true.
     *
     * @param timeout maximum time in milliseconds to wait for the predicate to become true.
     * @param predicate predicate waiting on.
     */
    protected void waitFor(int timeout, Predicate predicate) throws Exception {
        ParamChecker.notNull(predicate, "predicate");
        long mustEnd = System.currentTimeMillis() + timeout;

        while (true) {
            try {
                predicate.evaluate();
                return;
            } catch(Error | Exception e) {
                if (System.currentTimeMillis() >= mustEnd) {
                    fail("Assertions failed. Failing after waiting for timeout " + timeout + " msecs", e);
                }
                LOG.debug("Waiting up to " + (mustEnd - System.currentTimeMillis()) + " msec as assertion failed", e);
                Thread.sleep(400);
            }
        }
    }
}
