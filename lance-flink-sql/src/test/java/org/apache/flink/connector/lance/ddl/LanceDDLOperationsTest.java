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

package org.apache.flink.connector.lance.ddl;

import org.junit.Test;
import java.util.Arrays;
import static org.junit.Assert.*;

/**
 * Unit tests for LanceDDLOperations.
 * Tests DDL operation definitions and result handling.
 */
public class LanceDDLOperationsTest {

    private static final String TEST_URI = "s3://test-bucket/test-dataset";

    @Test
    public void testOptimizeResultBuilder() {
        LanceDDLOperations.OptimizeResult result = new LanceDDLOperations.OptimizeResult()
            .setDuration(5000)
            .setDatasetUri(TEST_URI);
        
        assertEquals(5000, result.getDuration());
        assertNotNull(result);
    }

    @Test
    public void testVacuumResultBuilder() {
        LanceDDLOperations.VacuumResult result = new LanceDDLOperations.VacuumResult()
            .setDuration(120000)
            .setDatasetUri(TEST_URI)
            .setDryRun(false);
        
        assertEquals(120000, result.getDuration());
        assertFalse(result.isDryRun());
    }

    @Test
    public void testCompactResultBuilder() {
        LanceDDLOperations.CompactResult result = new LanceDDLOperations.CompactResult()
            .setDuration(60000)
            .setDatasetUri(TEST_URI);
        
        assertEquals(60000, result.getDuration());
    }

    @Test
    public void testRestoreResultBuilder() {
        LanceDDLOperations.RestoreResult result = new LanceDDLOperations.RestoreResult()
            .setVersionId(42)
            .setCurrentVersion(42)
            .setRowCount(1000000)
            .setDuration(3000)
            .setDatasetUri(TEST_URI);
        
        assertEquals(42, result.getVersionId());
        assertEquals(1000000, result.getRowCount());
    }

    @Test
    public void testDropVersionResultBuilder() {
        LanceDDLOperations.DropVersionResult result = new LanceDDLOperations.DropVersionResult()
            .setVersionId(5)
            .setDuration(1000)
            .setDatasetUri(TEST_URI);
        
        assertEquals(5, result.getVersionId());
    }

    @Test
    public void testStatisticsBuilder() {
        LanceDDLOperations.Statistics stats = new LanceDDLOperations.Statistics()
            .setDatasetUri(TEST_URI)
            .setRowCount(5000000)
            .setVersion(10)
            .setFragmentCount(25);
        
        assertEquals(5000000, stats.getRowCount());
        assertEquals(10, stats.getVersion());
        assertEquals(25, stats.getFragmentCount());
    }

    @Test
    public void testVersionHistoryBuilder() {
        LanceDDLOperations.VersionHistory history = new LanceDDLOperations.VersionHistory()
            .setDatasetUri(TEST_URI)
            .setTotalVersions(100);
        
        assertEquals(100, history.getTotalVersions());
    }

    @Test
    public void testFragmentListBuilder() {
        LanceDDLOperations.FragmentList list = new LanceDDLOperations.FragmentList()
            .setDatasetUri(TEST_URI)
            .setFragmentCount(50);
        
        assertEquals(50, list.getFragmentCount());
    }

    @Test
    public void testFragmentInfoBuilder() {
        LanceDDLOperations.FragmentInfo info = new LanceDDLOperations.FragmentInfo()
            .setFragmentId(3)
            .setDatasetUri(TEST_URI)
            .setRowCount(800000);
        
        assertEquals(3, info.getFragmentId());
        assertEquals(800000, info.getRowCount());
    }

    @Test(expected = NullPointerException.class)
    public void testNullUri() {
        new LanceDDLOperations(null);
    }
}
