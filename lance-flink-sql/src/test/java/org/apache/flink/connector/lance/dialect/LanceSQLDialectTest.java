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

package org.apache.flink.connector.lance.dialect;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for LanceSQLDialect.
 * Verifies SQL statement parsing and execution.
 */
public class LanceSQLDialectTest {

    private final LanceSQLDialect dialect = new LanceSQLDialect();

    @Test
    public void testParseOptimizeStatement() throws Exception {
        String sql = "OPTIMIZE TABLE my_table BY (id, name)";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.OptimizeStatement);
    }

    @Test
    public void testParseVacuumStatement() throws Exception {
        String sql = "VACUUM TABLE my_table RETENTION 7 DAYS";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.VacuumStatement);
        
        LanceSQLDialect.VacuumStatement vacuum = (LanceSQLDialect.VacuumStatement) stmt;
        assertEquals(7, vacuum.retentionDays);
    }

    @Test
    public void testParseVacuumStatementDryRun() throws Exception {
        String sql = "VACUUM TABLE my_table RETENTION 0 DAYS DRY RUN";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.VacuumStatement);
        
        LanceSQLDialect.VacuumStatement vacuum = (LanceSQLDialect.VacuumStatement) stmt;
        assertTrue(vacuum.dryRun);
    }

    @Test
    public void testParseCompactStatement() throws Exception {
        String sql = "COMPACT TABLE my_table TARGET_SIZE 104857600 MIN_ROWS 1000";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.CompactStatement);
        
        LanceSQLDialect.CompactStatement compact = (LanceSQLDialect.CompactStatement) stmt;
        assertEquals(104857600, compact.targetSize);
        assertEquals(1000, compact.minRows);
    }

    @Test
    public void testParseShowStatsStatement() throws Exception {
        String sql = "SHOW STATS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.ShowStatsStatement);
    }

    @Test
    public void testParseShowVersionsStatement() throws Exception {
        String sql = "SHOW VERSIONS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.ShowVersionsStatement);
    }

    @Test
    public void testParseRestoreStatement() throws Exception {
        String sql = "RESTORE TABLE my_table TO VERSION 5";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.RestoreStatement);
        
        LanceSQLDialect.RestoreStatement restore = (LanceSQLDialect.RestoreStatement) stmt;
        assertEquals(5, restore.versionId);
    }

    @Test
    public void testParseDropVersionStatement() throws Exception {
        String sql = "DROP VERSION 3 FROM my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.DropVersionStatement);
        
        LanceSQLDialect.DropVersionStatement drop = (LanceSQLDialect.DropVersionStatement) stmt;
        assertEquals(3, drop.versionId);
    }

    @Test
    public void testParseShowFragmentsStatement() throws Exception {
        String sql = "SHOW FRAGMENTS FOR my_table";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.ShowFragmentsStatement);
    }

    @Test
    public void testParseShowFragmentDetailsStatement() throws Exception {
        String sql = "SHOW FRAGMENTS FOR my_table DETAILS 2";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        assertNotNull(stmt);
        assertTrue(stmt instanceof LanceSQLDialect.ShowFragmentsStatement);
        
        LanceSQLDialect.ShowFragmentsStatement fragments = (LanceSQLDialect.ShowFragmentsStatement) stmt;
        assertTrue(fragments.showDetails);
        assertEquals(2, fragments.fragmentId);
    }

    @Test(expected = Exception.class)
    public void testParseEmptyStatement() throws Exception {
        dialect.parseStatement("");
    }

    @Test(expected = Exception.class)
    public void testParseNullStatement() throws Exception {
        dialect.parseStatement(null);
    }

    @Test(expected = Exception.class)
    public void testParseUnsupportedStatement() throws Exception {
        dialect.parseStatement("SELECT * FROM my_table");
    }

    @Test
    public void testOptimizeStatementExecution() throws Exception {
        String sql = "OPTIMIZE TABLE my_table BY (id)";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        assertNotNull(result);
        assertTrue(result.contains("OPTIMIZE"));
    }

    @Test
    public void testVacuumStatementExecution() throws Exception {
        String sql = "VACUUM TABLE my_table RETENTION 7 DAYS";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        assertNotNull(result);
        assertTrue(result.contains("VACUUM"));
    }

    @Test
    public void testCompactStatementExecution() throws Exception {
        String sql = "COMPACT TABLE my_table TARGET_SIZE 100 MIN_ROWS 50";
        LanceSQLDialect.LanceSQLStatement stmt = dialect.parseStatement(sql);
        String result = stmt.execute();
        assertNotNull(result);
        assertTrue(result.contains("COMPACT"));
    }
}
