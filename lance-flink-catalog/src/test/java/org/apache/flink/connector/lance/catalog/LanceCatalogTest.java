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

package org.apache.flink.connector.lance.catalog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Production-ready unit tests for LanceCatalog.
 */
public class LanceCatalogTest {
    private String tempMetadataPath;
    private LanceCatalog catalog;
    
    @Before
    public void setUp() throws Exception {
        tempMetadataPath = Files.createTempDirectory("lance-catalog-test").toString();
        catalog = new LanceCatalog("test_catalog", tempMetadataPath);
        catalog.open();
    }
    
    @After
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        // Clean up temp directory
        deleteDirectory(new File(tempMetadataPath));
    }
    
    @Test
    public void testCatalogOpen() throws Exception {
        assertNotNull(catalog);
        assertEquals("test_catalog", catalog.getName());
    }
    
    @Test
    public void testCreateDatabase() throws Exception {
        catalog.createDatabase("test_db");
        List<String> databases = catalog.listDatabases();
        assertTrue(databases.contains("test_db"));
    }
    
    @Test
    public void testDatabaseExists() throws Exception {
        catalog.createDatabase("test_db");
        assertTrue(catalog.databaseExists("test_db"));
        assertFalse(catalog.databaseExists("non_existent"));
    }
    
    @Test
    public void testDropDatabase() throws Exception {
        catalog.createDatabase("test_db");
        assertTrue(catalog.databaseExists("test_db"));
        catalog.dropDatabase("test_db");
        assertFalse(catalog.databaseExists("test_db"));
    }
    
    @Test
    public void testListDatabases() throws Exception {
        catalog.createDatabase("db1");
        catalog.createDatabase("db2");
        catalog.createDatabase("db3");
        
        List<String> databases = catalog.listDatabases();
        assertEquals(3, databases.size());
        assertTrue(databases.contains("db1"));
        assertTrue(databases.contains("db2"));
        assertTrue(databases.contains("db3"));
    }
    
    @Test
    public void testCreateTable() throws Exception {
        catalog.createDatabase("test_db");
        
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        schema.addColumn("id", "BIGINT");
        schema.addColumn("name", "STRING");
        
        catalog.createTable("test_db", "test_table", schema);
        assertTrue(catalog.tableExists("test_db", "test_table"));
    }
    
    @Test
    public void testDropTable() throws Exception {
        catalog.createDatabase("test_db");
        
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        schema.addColumn("id", "BIGINT");
        
        catalog.createTable("test_db", "test_table", schema);
        assertTrue(catalog.tableExists("test_db", "test_table"));
        
        catalog.dropTable("test_db", "test_table");
        assertFalse(catalog.tableExists("test_db", "test_table"));
    }
    
    @Test
    public void testListTables() throws Exception {
        catalog.createDatabase("test_db");
        
        for (int i = 1; i <= 3; i++) {
            LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
                "table" + i, "/tmp/lance/table" + i
            );
            schema.addColumn("id", "BIGINT");
            catalog.createTable("test_db", "table" + i, schema);
        }
        
        List<String> tables = catalog.listTables("test_db");
        assertEquals(3, tables.size());
        assertTrue(tables.contains("table1"));
        assertTrue(tables.contains("table2"));
        assertTrue(tables.contains("table3"));
    }
    
    @Test
    public void testGetTableSchema() throws Exception {
        catalog.createDatabase("test_db");
        
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        schema.addColumn("id", "BIGINT");
        schema.addColumn("name", "STRING");
        schema.addColumn("age", "INT");
        
        catalog.createTable("test_db", "test_table", schema);
        
        LanceCatalog.LanceTableSchema retrieved = catalog.getTableSchema("test_db", "test_table");
        assertNotNull(retrieved);
        assertEquals("test_table", retrieved.getTableName());
        assertEquals("/tmp/lance/test_table", retrieved.getTableUri());
        assertEquals(3, retrieved.getColumns().size());
        assertEquals("BIGINT", retrieved.getColumns().get("id"));
        assertEquals("STRING", retrieved.getColumns().get("name"));
    }
    
    @Test
    public void testTableSchemaJsonSerialization() throws Exception {
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        schema.addColumn("id", "BIGINT");
        schema.addColumn("value", "DOUBLE");
        schema.setCompression("snappy");
        
        String json = schema.toJson();
        assertNotNull(json);
        assertTrue(json.contains("test_table"));
        assertTrue(json.contains("snappy"));
        
        LanceCatalog.LanceTableSchema deserialized = 
            LanceCatalog.LanceTableSchema.fromJson(json);
        assertEquals("test_table", deserialized.getTableName());
        assertEquals("snappy", deserialized.getCompression());
    }
    
    @Test
    public void testCatalogPersistence() throws Exception {
        // Create catalog and database
        catalog.createDatabase("persistent_db");
        
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "persistent_table", "/tmp/lance/persistent_table"
        );
        schema.addColumn("id", "BIGINT");
        catalog.createTable("persistent_db", "persistent_table", schema);
        
        // Close and reopen
        catalog.close();
        
        LanceCatalog catalog2 = new LanceCatalog("test_catalog", tempMetadataPath);
        catalog2.open();
        
        // Verify data persisted
        List<String> databases = catalog2.listDatabases();
        assertTrue(databases.contains("persistent_db"));
        
        List<String> tables = catalog2.listTables("persistent_db");
        assertTrue(tables.contains("persistent_table"));
        
        LanceCatalog.LanceTableSchema retrievedSchema = 
            catalog2.getTableSchema("persistent_db", "persistent_table");
        assertEquals("persistent_table", retrievedSchema.getTableName());
        assertEquals("BIGINT", retrievedSchema.getColumns().get("id"));
        
        catalog2.close();
    }
    
    @Test
    public void testMultipleDatabasesAndTables() throws Exception {
        // Create multiple databases with tables
        for (int i = 1; i <= 3; i++) {
            catalog.createDatabase("db" + i);
            
            for (int j = 1; j <= 2; j++) {
                LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
                    "table" + j, "/tmp/lance/db" + i + "/table" + j
                );
                schema.addColumn("id", "BIGINT");
                catalog.createTable("db" + i, "table" + j, schema);
            }
        }
        
        // Verify structure
        List<String> allDatabases = catalog.listDatabases();
        assertEquals(3, allDatabases.size());
        
        for (int i = 1; i <= 3; i++) {
            List<String> tables = catalog.listTables("db" + i);
            assertEquals(2, tables.size());
        }
    }
    
    @Test
    public void testTableSchemaWithMultipleColumns() throws Exception {
        catalog.createDatabase("test_db");
        
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "multi_column_table", "/tmp/lance/multi_column_table"
        );
        schema.addColumn("id", "BIGINT");
        schema.addColumn("name", "STRING");
        schema.addColumn("age", "INT");
        schema.addColumn("salary", "DOUBLE");
        schema.addColumn("hire_date", "TIMESTAMP");
        
        catalog.createTable("test_db", "multi_column_table", schema);
        
        LanceCatalog.LanceTableSchema retrieved = 
            catalog.getTableSchema("test_db", "multi_column_table");
        assertEquals(5, retrieved.getColumns().size());
        assertEquals("TIMESTAMP", retrieved.getColumns().get("hire_date"));
    }
    
    @Test(expected = Exception.class)
    public void testDropNonExistentDatabase() throws Exception {
        catalog.dropDatabase("non_existent_db");
    }
    
    @Test(expected = Exception.class)
    public void testCreateTableInNonExistentDatabase() throws Exception {
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        catalog.createTable("non_existent_db", "test_table", schema);
    }
    
    // ============ Helper Methods ============
    
    private void deleteDirectory(File dir) throws Exception {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }
}
