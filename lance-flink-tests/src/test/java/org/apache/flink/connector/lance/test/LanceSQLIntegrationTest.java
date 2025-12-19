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

package org.apache.flink.connector.lance.test;

import org.apache.flink.connector.lance.dialect.LanceSQLDialect;
import org.apache.flink.connector.lance.catalog.LanceCatalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for Lance SQL support.
 * Covers SQL statement parsing, DDL operations, and catalog management.
 */
public class LanceSQLIntegrationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private LanceSQLDialect dialect;
    private LanceCatalog catalog;

    @Before
    public void setup() throws IOException {
        dialect = new LanceSQLDialect();
        catalog = new LanceCatalog("test_catalog", tempFolder.getRoot().getAbsolutePath());
        catalog.open();
    }

    // ============ SQL Parsing Tests ============

    @Test
    public void testParseLanceSelectSQL() throws Exception {
        // Standard SELECT should fail in Lance dialect (not supported)
        try {
            dialect.parseStatement("SELECT * FROM my_table");
            fail("Should not support SELECT");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unsupported"));
        }
    }

    @Test
    public void testParseMultipleOptimizeStatements() throws Exception {
        String[] sqls = {
                "OPTIMIZE TABLE table1 BY (id)",
                "OPTIMIZE TABLE table2 BY (id, name, age)",
                "OPTIMIZE TABLE my_dataset BY (user_id, timestamp)"
        };

        for (String sql : sqls) {
            LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
            assertNotNull(stmt);
            assertTrue(stmt instanceof LanceSQLDialect.OptimizeStatement);
        }
    }

    @Test
    public void testParseVacuumWithVariousRetention() throws Exception {
        int[] retentionDays = {0, 1, 7, 30, 90};

        for (int days : retentionDays) {
            String sql = String.format("VACUUM TABLE my_table RETENTION %d DAYS", days);
            LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
            assertTrue(stmt instanceof LanceSQLDialect.VacuumStatement);
            
            LanceSQLDialect.VacuumStatement vacuum = (LanceSQLDialect.VacuumStatement) stmt;
            assertEquals(days, vacuum.retentionDays);
        }
    }

    @Test
    public void testParseCompactWithDifferentSizes() throws Exception {
        String[] sqls = {
                "COMPACT TABLE t1 TARGET_SIZE 1000 MIN_ROWS 100",
                "COMPACT TABLE t2 TARGET_SIZE 104857600 MIN_ROWS 1000",
                "COMPACT TABLE t3 TARGET_SIZE 1073741824 MIN_ROWS 10000"
        };

        for (String sql : sqls) {
            LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
            assertTrue(stmt instanceof LanceSQLDialect.CompactStatement);
        }
    }

    @Test
    public void testParseRestoreMultipleVersions() throws Exception {
        long[] versions = {1L, 5L, 10L, 100L};

        for (long version : versions) {
            String sql = String.format("RESTORE TABLE my_table TO VERSION %d", version);
            LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
            assertTrue(stmt instanceof LanceSQLDialect.RestoreStatement);
            
            LanceSQLDialect.RestoreStatement restore = (LanceSQLDialect.RestoreStatement) stmt;
            assertEquals(version, restore.versionId);
        }
    }

    // ============ DDL Operation Tests ============

    @Test
    public void testOptimizeStatementOutput() throws Exception {
        String sql = "OPTIMIZE TABLE my_table BY (id)";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("OPTIMIZE"));
        assertTrue(result.contains("my_table"));
    }

    @Test
    public void testVacuumStatementOutput() throws Exception {
        String sql = "VACUUM TABLE my_table RETENTION 7 DAYS";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("VACUUM"));
        assertTrue(result.contains("7"));
    }

    @Test
    public void testCompactStatementOutput() throws Exception {
        String sql = "COMPACT TABLE my_table TARGET_SIZE 1000 MIN_ROWS 100";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("COMPACT"));
    }

    @Test
    public void testShowStatsOutput() throws Exception {
        String sql = "SHOW STATS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("Statistics"));
    }

    @Test
    public void testShowVersionsOutput() throws Exception {
        String sql = "SHOW VERSIONS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("Version history"));
    }

    @Test
    public void testShowFragmentsOutput() throws Exception {
        String sql = "SHOW FRAGMENTS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        
        assertTrue(result.contains("Fragments"));
    }

    // ============ Catalog Tests ============

    @Test
    public void testCatalogDatabaseOperations() throws Exception {
        // Create database
        catalog.createDatabase("test_db", new CatalogDatabase(), false);
        assertTrue(catalog.databaseExists("test_db"));

        // List databases
        List<String> databases = catalog.listDatabases();
        assertTrue(databases.contains("test_db"));
        assertTrue(databases.contains("default"));

        // Drop database
        catalog.dropDatabase("test_db", false, false);
        assertFalse(catalog.databaseExists("test_db"));
    }

    @Test
    public void testCatalogTableOperations() throws Exception {
        // Create multiple databases
        catalog.createDatabase("db1", new CatalogDatabase(), false);
        catalog.createDatabase("db2", new CatalogDatabase(), false);

        // List tables
        List<String> tablesDb1 = catalog.listTables("db1");
        assertNotNull(tablesDb1);
        assertTrue(tablesDb1.isEmpty());

        // Check table existence
        ObjectPath table1Path = new ObjectPath("db1", "table1");
        assertFalse(catalog.tableExists(table1Path));
    }

    @Test
    public void testCatalogMultipleDatabases() throws Exception {
        // Create multiple databases
        String[] databaseNames = {"db1", "db2", "db3", "analytics", "warehouse"};
        
        for (String dbName : databaseNames) {
            catalog.createDatabase(dbName, new CatalogDatabase(), false);
        }

        // Verify all databases exist
        List<String> databases = catalog.listDatabases();
        assertEquals(databaseNames.length + 1, databases.size()); // +1 for default

        // Verify each database
        for (String dbName : databaseNames) {
            assertTrue(catalog.databaseExists(dbName));
        }
    }

    // ============ End-to-End Tests ============

    @Test
    public void testSQLWorkflowSimulation() throws Exception {
        // Step 1: Create database
        catalog.createDatabase("analytics", new CatalogDatabase(), false);
        assertTrue(catalog.databaseExists("analytics"));

        // Step 2: Parse and execute DDL operations
        String optimizeSQL = "OPTIMIZE TABLE events BY (user_id, timestamp)";
        LanceSQLDialect.LanceSQLStatement optimizeStmt = dialect.parseStatement(optimizeSQL);
        String optimizeResult = optimizeStmt.execute();
        assertNotNull(optimizeResult);

        // Step 3: Vacuum operation
        String vacuumSQL = "VACUUM TABLE events RETENTION 30 DAYS";
        LanceSQLDialect.LanceSQLStatement vacuumStmt = dialect.parseStatement(vacuumSQL);
        String vacuumResult = vacuumStmt.execute();
        assertNotNull(vacuumResult);

        // Step 4: Show stats
        String statsSQL = "SHOW STATS FOR events";
        LanceSQLDialect.LanceSQLStatement statsStmt = dialect.parseStatement(statsSQL);
        String statsResult = statsStmt.execute();
        assertTrue(statsResult.contains("Statistics"));

        // Step 5: Verify database still exists
        assertTrue(catalog.databaseExists("analytics"));
    }

    @Test
    public void testStatementChaining() throws Exception {
        String[] statements = {
                "OPTIMIZE TABLE table1 BY (id)",
                "VACUUM TABLE table1 RETENTION 7 DAYS",
                "COMPACT TABLE table1 TARGET_SIZE 100000 MIN_ROWS 500",
                "SHOW STATS FOR table1",
                "SHOW VERSIONS FOR table1"
        };

        for (String sql : statements) {
            LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
            String result = stmt.execute();
            assertNotNull(result);
            assertFalse(result.isEmpty());
        }
    }

    @Test
    public void testErrorHandlingForInvalidSQL() throws Exception {
        String[] invalidSqls = {
                "",
                null,
                "INVALID SQL STATEMENT",
                "SELECT * FROM table",
                "INSERT INTO table VALUES (...)"
        };

        for (String sql : invalidSqls) {
            try {
                if (sql != null && !sql.isEmpty()) {
                    dialect.parseStatement(sql);
                    if (!sql.isEmpty()) {
                        fail("Should have thrown exception for: " + sql);
                    }
                }
            } catch (Exception e) {
                // Expected
                assertTrue(true);
            }
        }
    }
}
