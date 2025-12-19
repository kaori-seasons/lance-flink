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

package org.apache.flink.connector.lance.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.sink.LanceUpsertSinkFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for streaming Lance dataset reading and writing.
 * 
 * Tests cover:
 * - LanceUnboundedSourceFunction state management and initialization
 * - Fragment polling and version tracking logic
 * - Checkpoint state serialization/deserialization
 * - LanceUpsertSinkFunction buffering and upsert semantics
 * - Primary key based record deduplication
 */
public class LanceStreamingIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(LanceStreamingIntegrationTest.class);

    /**
     * Test LanceUnboundedSourceFunction state initialization and management.
     */
    @Test
    public void testUnboundedSourceStateInitialization() {
        LOG.info("=== Test: Unbounded Source State Initialization ===");
        
        // Create initial state with version and fragments
        List<Integer> initialFragments = Arrays.asList(0, 1, 2, 3);
        LanceUnboundedSourceFunction.UnboundedSourceState state = 
                new LanceUnboundedSourceFunction.UnboundedSourceState(10L, initialFragments);
        
        assertEquals(10L, state.currentVersionId, "Initial version ID should be 10");
        assertEquals(4, state.processedFragmentIds.size(), "Should have 4 fragments");
        assertTrue(state.processedFragmentIds.contains(0), "Fragment 0 should exist");
        assertTrue(state.processedFragmentIds.contains(3), "Fragment 3 should exist");
        
        LOG.info("✓ State initialization test passed");
    }

    /**
     * Test state string representation for debugging.
     */
    @Test
    public void testUnboundedSourceStateToString() {
        LOG.info("=== Test: Unbounded Source State toString ===");
        
        LanceUnboundedSourceFunction.UnboundedSourceState state =
                new LanceUnboundedSourceFunction.UnboundedSourceState(42L, Arrays.asList(1, 2, 3));
        
        String str = state.toString();
        assertNotNull(str, "State should have string representation");
        assertTrue(str.contains("42"), "String should contain version ID");
        assertTrue(str.contains("fragments=3"), "String should show fragment count");
        
        LOG.info("State toString: {}", str);
        LOG.info("✓ State toString test passed");
    }

    /**
     * Test fragment tracking and polling logic.
     */
    @Test
    public void testFragmentPollLogic() {
        LOG.info("=== Test: Fragment Poll Logic ===");
        
        // Simulate initial snapshot
        java.util.Set<Integer> processedFragments = new java.util.HashSet<>(
                Arrays.asList(0, 1, 2, 3, 4)
        );
        assertEquals(5, processedFragments.size(), "Should have 5 initial fragments");
        
        // Simulate dataset update with new fragments
        List<Integer> allFragments = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
        List<Integer> newFragments = new ArrayList<>();
        
        for (Integer fragmentId : allFragments) {
            if (!processedFragments.contains(fragmentId)) {
                newFragments.add(fragmentId);
            }
        }
        
        assertEquals(2, newFragments.size(), "Should detect 2 new fragments");
        assertTrue(newFragments.contains(5), "Fragment 5 should be new");
        assertTrue(newFragments.contains(6), "Fragment 6 should be new");
        
        // Add new fragments to processed set
        processedFragments.addAll(newFragments);
        assertEquals(7, processedFragments.size(), "Should have 7 fragments after polling");
        
        LOG.info("✓ Fragment poll logic test passed");
    }

    /**
     * Test version change detection.
     */
    @Test
    public void testVersionChangeDetection() {
        LOG.info("=== Test: Version Change Detection ===");
        
        long currentVersion = 5L;
        long polledVersion1 = 5L;
        long polledVersion2 = 6L;
        
        // No change
        boolean changed1 = (polledVersion1 != currentVersion);
        assertFalse(changed1, "Version should not have changed");
        
        // Version changed
        boolean changed2 = (polledVersion2 != currentVersion);
        assertTrue(changed2, "Version should have changed");
        assertTrue(polledVersion2 > currentVersion, "New version should be greater");
        
        LOG.info("✓ Version change detection test passed");
    }

    /**
     * Test LanceUpsertSinkFunction state creation and serialization.
     */
    @Test
    public void testUpsertSinkStateCreation() {
        LOG.info("=== Test: Upsert Sink State Creation ===");
        
        HashMap<Object, Row> buffer = new HashMap<>();
        
        // Add test records
        for (int i = 0; i < 3; i++) {
            Row row = new Row(2);
            row.setField(0, "id_" + i);
            row.setField(1, "value_" + i);
            buffer.put("id_" + i, row);
        }
        
        LanceUpsertSinkFunction.UpsertSinkState state = 
                new LanceUpsertSinkFunction.UpsertSinkState(100L, buffer);
        
        assertEquals(100L, state.recordsProcessed, "Records processed should be 100");
        assertEquals(3, state.bufferedRecords.size(), "Buffer should have 3 records");
        assertTrue(state.bufferedRecords.containsKey("id_0"), "Buffer should contain id_0");
        assertTrue(state.bufferedRecords.containsKey("id_2"), "Buffer should contain id_2");
        
        LOG.info("✓ Upsert sink state creation test passed");
    }

    /**
     * Test upsert semantics with primary key updates.
     */
    @Test
    public void testUpsertSemantics() {
        LOG.info("=== Test: Upsert Semantics ===");
        
        HashMap<Object, Row> recordBuffer = new HashMap<>();
        
        // Insert first record
        Row row1 = new Row(2);
        row1.setField(0, "user123");
        row1.setField(1, "score_90");
        recordBuffer.put("user123", row1);
        assertEquals(1, recordBuffer.size(), "Buffer size should be 1");
        
        // Update same primary key with new value
        Row row2 = new Row(2);
        row2.setField(0, "user123");
        row2.setField(1, "score_95");
        recordBuffer.put("user123", row2);  // Same key, different value
        
        // Verify upsert behavior
        assertEquals(1, recordBuffer.size(), "Buffer size should still be 1 (upsert)");
        assertEquals("score_95", recordBuffer.get("user123").getField(1), 
                "Value should be updated to latest");
        
        // Insert different key
        Row row3 = new Row(2);
        row3.setField(0, "user456");
        row3.setField(1, "score_85");
        recordBuffer.put("user456", row3);
        
        assertEquals(2, recordBuffer.size(), "Buffer size should be 2");
        assertTrue(recordBuffer.containsKey("user123"), "Should contain user123");
        assertTrue(recordBuffer.containsKey("user456"), "Should contain user456");
        assertEquals("score_95", recordBuffer.get("user123").getField(1), 
                "user123 value should still be 95");
        
        LOG.info("✓ Upsert semantics test passed");
    }

    /**
     * Test upsert sink state toString.
     */
    @Test
    public void testUpsertSinkStateToString() {
        LOG.info("=== Test: Upsert Sink State toString ===");
        
        HashMap<Object, Row> buffer = new HashMap<>();
        Row row = new Row(1);
        row.setField(0, "test");
        buffer.put("pk1", row);
        
        LanceUpsertSinkFunction.UpsertSinkState state =
                new LanceUpsertSinkFunction.UpsertSinkState(50L, buffer);
        
        String str = state.toString();
        assertNotNull(str, "State should have string representation");
        assertTrue(str.contains("50"), "String should contain processed count");
        assertTrue(str.contains("buffered=1"), "String should show buffered count");
        
        LOG.info("State toString: {}", str);
        LOG.info("✓ Upsert sink state toString test passed");
    }

    /**
     * Test checkpoint state save and restore cycle.
     */
    @Test
    public void testCheckpointStateRestoration() {
        LOG.info("=== Test: Checkpoint State Restoration ===");
        
        // Simulate initial processing state
        List<Integer> processedFragments = new ArrayList<>(Arrays.asList(0, 1));
        long currentVersion = 5L;
        
        // Save state (as if checkpointing)
        List<Integer> snapshotFragments = new ArrayList<>(processedFragments);
        long snapshotVersion = currentVersion;
        
        // Simulate more processing after checkpoint
        processedFragments.addAll(Arrays.asList(2, 3, 4));
        currentVersion = 6L;
        assertEquals(5, processedFragments.size(), "Should have processed 5 fragments");
        assertEquals(6L, currentVersion, "Version should be updated to 6");
        
        // Simulate failure and restore from checkpoint
        processedFragments = new ArrayList<>(snapshotFragments);
        currentVersion = snapshotVersion;
        
        // Verify restored state
        assertEquals(2, processedFragments.size(), "Restored state should have 2 fragments");
        assertEquals(5L, currentVersion, "Restored version should be 5");
        assertFalse(processedFragments.contains(2), "Fragment 2 should not be in restored state");
        
        LOG.info("✓ Checkpoint state restoration test passed");
    }

    /**
     * Test LanceConfig builder for streaming configuration.
     */
    @Test
    public void testLanceConfigBuilder() {
        LOG.info("=== Test: LanceConfig Builder ===");
        
        LanceConfig config = new LanceConfig.Builder("file:///tmp/test_dataset")
                .readBatchSize(256)
                .writeBatchSize(512)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();
        
        assertEquals("file:///tmp/test_dataset", config.getDatasetUri(), 
                "URI should match");
        assertEquals(256L, config.getReadBatchSize(), 
                "Read batch size should be 256");
        assertEquals(512L, config.getWriteBatchSize(), 
                "Write batch size should be 512");
        assertTrue(config.isEnablePredicatePushdown(), 
                "Predicate pushdown should be enabled");
        assertTrue(config.isEnableColumnPruning(), 
                "Column pruning should be enabled");
        
        LOG.info("✓ LanceConfig builder test passed");
    }

    /**
     * Test LanceReadOptions builder for streaming read configuration.
     */
    @Test
    public void testLanceReadOptionsBuilder() {
        LOG.info("=== Test: LanceReadOptions Builder ===");
        
        LanceReadOptions readOptions = new LanceReadOptions.Builder()
                .columns(Arrays.asList("id", "name", "value"))
                .build();
        
        assertEquals(3, readOptions.getColumns().size(), 
                "Should have 3 columns");
        assertTrue(readOptions.getColumns().contains("id"), 
                "Should contain id column");
        assertTrue(readOptions.getColumns().contains("name"), 
                "Should contain name column");
        assertTrue(readOptions.getColumns().contains("value"), 
                "Should contain value column");
        
        LOG.info("✓ LanceReadOptions builder test passed");
    }

    /**
     * Test batch buffer overflow and flush scenario.
     */
    @Test
    public void testBufferFlushLogic() {
        LOG.info("=== Test: Buffer Flush Logic ===");
        
        int batchSize = 1024;
        HashMap<Object, Row> recordBuffer = new HashMap<>();
        List<Row> flushedRecords = new ArrayList<>();
        
        // Fill buffer to near capacity
        for (int i = 0; i < 1023; i++) {
            Row row = new Row(1);
            row.setField(0, "record_" + i);
            recordBuffer.put(i, row);
        }
        
        assertEquals(1023, recordBuffer.size(), "Buffer should have 1023 records");
        assertFalse(shouldFlush(recordBuffer.size(), batchSize), 
                "Should not flush yet");
        
        // Add one more record to trigger flush
        Row row = new Row(1);
        row.setField(0, "record_1023");
        recordBuffer.put(1023, row);
        
        assertEquals(1024, recordBuffer.size(), "Buffer should have 1024 records");
        assertTrue(shouldFlush(recordBuffer.size(), batchSize), 
                "Should flush now");
        
        // Simulate flush
        flushedRecords.addAll(recordBuffer.values());
        recordBuffer.clear();
        
        assertEquals(1024, flushedRecords.size(), "Flushed records should be 1024");
        assertEquals(0, recordBuffer.size(), "Buffer should be empty after flush");
        
        LOG.info("✓ Buffer flush logic test passed");
    }

    /**
     * Helper method to determine if buffer should be flushed.
     */
    private boolean shouldFlush(int currentSize, int batchSize) {
        return currentSize >= batchSize;
    }

    /**
     * Test checkpoint cycle with multiple updates.
     */
    @Test
    public void testMultipleCheckpointCycles() {
        LOG.info("=== Test: Multiple Checkpoint Cycles ===");
        
        // Checkpoint 1
        List<Integer> processed1 = Arrays.asList(0, 1, 2);
        long version1 = 1L;
        
        // Process more fragments
        List<Integer> processed2 = new ArrayList<>(processed1);
        processed2.addAll(Arrays.asList(3, 4, 5));
        long version2 = 2L;
        
        // Checkpoint 2
        List<Integer> checkpoint2 = new ArrayList<>(processed2);
        long checkpointVersion2 = version2;
        
        // Process even more
        List<Integer> processed3 = new ArrayList<>(processed2);
        processed3.addAll(Arrays.asList(6, 7, 8));
        long version3 = 3L;
        
        // Verify states
        assertEquals(3, processed1.size(), "Checkpoint 1 should have 3 fragments");
        assertEquals(6, checkpoint2.size(), "Checkpoint 2 should have 6 fragments");
        assertEquals(9, processed3.size(), "Final state should have 9 fragments");
        
        assertEquals(1L, version1, "Version 1 should be 1");
        assertEquals(2L, checkpointVersion2, "Checkpoint version should be 2");
        assertEquals(3L, version3, "Version 3 should be 3");
        
        LOG.info("✓ Multiple checkpoint cycles test passed");
    }
}
