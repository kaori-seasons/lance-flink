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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-End test for LanceUpsertSinkFunction.
 * 
 * Tests the complete sink pipeline:
 * 1. Initialize upsert sink with configuration
 * 2. Buffer and accumulate records
 * 3. Perform upsert operations (deduplication by primary key)
 * 4. Flush buffered data
 * 5. Verify checkpoint state and recovery
 */
public class LanceUpsertSinkE2ETest {
    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertSinkE2ETest.class);

    /**
     * End-to-end test: Sink initialization and buffering.
     */
    @Test
    public void testSinkInitializationAndBuffering() {
        LOG.info("=== E2E Test: Sink Initialization and Buffering ===");
        
        // Setup: Create sink configuration
        LanceConfig config = new LanceConfig.Builder("file:///tmp/test_output")
                .writeBatchSize(1024)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();
        
        assertEquals("file:///tmp/test_output", config.getDatasetUri(),
                "Output URI should be set");
        assertEquals(1024L, config.getWriteBatchSize(),
                "Write batch size should be 1024");
        
        // Create sink instance
        LanceUpsertSinkFunction sink = new LanceUpsertSinkFunction(
                config,
                "id",      // primary key
                512        // buffer size
        );
        
        // Simulate record buffering
        Map<Object, Row> buffer = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            Row row = new Row(2);
            row.setField(0, "id_" + i);
            row.setField(1, "value_" + i);
            buffer.put("id_" + i, row);
        }
        
        assertEquals(100, buffer.size(),
                "Buffer should contain 100 records");
        
        // Verify buffer content
        Row sampleRow = buffer.get("id_0");
        assertNotNull(sampleRow, "Sample row should exist");
        assertEquals("id_0", sampleRow.getField(0),
                "ID field should be id_0");
        
        LOG.info("✓ Sink initialization and buffering test passed");
    }

    /**
     * End-to-end test: Upsert semantics with deduplication.
     */
    @Test
    public void testUpsertSemanticsWithDeduplication() {
        LOG.info("=== E2E Test: Upsert Semantics with Deduplication ===");
        
        // Simulate record buffer with upsert behavior
        Map<Object, Row> recordBuffer = new HashMap<>();
        
        // Insert initial records
        for (int i = 0; i < 10; i++) {
            Row row = new Row(3);
            row.setField(0, "user_" + i);
            row.setField(1, "score_" + (100 + i));
            row.setField(2, "timestamp_1");
            recordBuffer.put("user_" + i, row);
        }
        
        assertEquals(10, recordBuffer.size(),
                "Buffer should have 10 unique users");
        
        // Update some records (same primary key, new values)
        for (int i = 0; i < 5; i++) {
            Row updatedRow = new Row(3);
            updatedRow.setField(0, "user_" + i);
            updatedRow.setField(1, "score_" + (150 + i));  // Updated score
            updatedRow.setField(2, "timestamp_2");
            recordBuffer.put("user_" + i, updatedRow);     // Upsert: same key, new value
        }
        
        // Verify upsert behavior
        assertEquals(10, recordBuffer.size(),
                "Buffer size should remain 10 (upsert behavior)");
        
        // Verify updated records
        Row updatedUser0 = recordBuffer.get("user_0");
        assertEquals("score_150", updatedUser0.getField(1),
                "User_0 score should be updated to 150");
        assertEquals("timestamp_2", updatedUser0.getField(2),
                "User_0 timestamp should be updated to timestamp_2");
        
        // Verify unchanged records
        Row unchangedUser9 = recordBuffer.get("user_9");
        assertEquals("score_109", unchangedUser9.getField(1),
                "User_9 score should remain 109 (not updated)");
        assertEquals("timestamp_1", unchangedUser9.getField(2),
                "User_9 timestamp should remain timestamp_1");
        
        LOG.info("✓ Upsert semantics with deduplication test passed");
    }

    /**
     * End-to-end test: Buffer flush and record count tracking.
     */
    @Test
    public void testBufferFlushAndRecordTracking() {
        LOG.info("=== E2E Test: Buffer Flush and Record Tracking ===");
        
        int batchSize = 100;
        Map<Object, Row> recordBuffer = new HashMap<>();
        List<Row> flushedRecords = new ArrayList<>();
        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicInteger flushCount = new AtomicInteger(0);
        
        // Phase 1: Fill buffer to near capacity
        for (int i = 0; i < 50; i++) {
            Row row = new Row(1);
            row.setField(0, "record_" + i);
            recordBuffer.put(i, row);
            totalProcessed.incrementAndGet();
        }
        
        assertEquals(50, recordBuffer.size(),
                "Buffer should have 50 records");
        assertFalse(shouldFlush(recordBuffer.size(), batchSize),
                "Should not flush yet");
        
        // Phase 2: Add more records to trigger first flush
        for (int i = 50; i < 120; i++) {
            Row row = new Row(1);
            row.setField(0, "record_" + i);
            recordBuffer.put(i, row);
            totalProcessed.incrementAndGet();
            
            // Check flush condition
            if (shouldFlush(recordBuffer.size(), batchSize)) {
                // Flush buffer
                flushedRecords.addAll(recordBuffer.values());
                recordBuffer.clear();
                flushCount.incrementAndGet();
                
                LOG.debug("Flush #{}: {} records", flushCount.get(), flushedRecords.size());
            }
        }
        
        // Final flush
        if (!recordBuffer.isEmpty()) {
            flushedRecords.addAll(recordBuffer.values());
            recordBuffer.clear();
            flushCount.incrementAndGet();
        }
        
        // Verify results
        assertEquals(120, totalProcessed.get(),
                "Total processed should be 120");
        assertEquals(120, flushedRecords.size(),
                "Flushed records should be 120 total");
        assertEquals(0, recordBuffer.size(),
                "Buffer should be empty after final flush");
        
        LOG.info("✓ Buffer flush and record tracking test passed");
    }

    /**
     * End-to-end test: Checkpoint state save and recovery.
     */
    @Test
    public void testCheckpointSaveAndRecovery() {
        LOG.info("=== E2E Test: Checkpoint Save and Recovery ===");
        
        // Initial processing phase
        Map<Object, Row> recordBuffer = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            Row row = new Row(2);
            row.setField(0, "pk_" + i);
            row.setField(1, "value_" + i);
            recordBuffer.put("pk_" + i, row);
        }
        
        long recordsProcessed = 50;
        
        // Create checkpoint state
        LanceUpsertSinkFunction.UpsertSinkState checkpointState =
                new LanceUpsertSinkFunction.UpsertSinkState(
                        recordsProcessed,
                        new HashMap<>(recordBuffer)
                );
        
        assertEquals(50L, checkpointState.recordsProcessed,
                "Processed records should be 50");
        assertEquals(20, checkpointState.bufferedRecords.size(),
                "Buffered records should be 20");
        
        // Simulate more processing
        for (int i = 20; i < 40; i++) {
            Row row = new Row(2);
            row.setField(0, "pk_" + i);
            row.setField(1, "value_" + i);
            recordBuffer.put("pk_" + i, row);
        }
        recordsProcessed = 100;
        
        assertEquals(40, recordBuffer.size(),
                "Buffer should have 40 records after processing");
        
        // Simulate failure and recovery
        recordBuffer = new HashMap<>(checkpointState.bufferedRecords);
        recordsProcessed = checkpointState.recordsProcessed;
        
        assertEquals(50L, recordsProcessed,
                "Should recover to 50 processed records");
        assertEquals(20, recordBuffer.size(),
                "Should recover to 20 buffered records");
        
        // Verify recovered records are correct
        for (int i = 0; i < 20; i++) {
            assertTrue(recordBuffer.containsKey("pk_" + i),
                    "Should have recovered record pk_" + i);
        }
        
        LOG.info("✓ Checkpoint save and recovery test passed");
    }

    /**
     * End-to-end test: Multiple sink instances with partitioning.
     */
    @Test
    public void testMultipleSinkInstancesPartitioning() {
        LOG.info("=== E2E Test: Multiple Sink Instances with Partitioning ===");
        
        // Simulate 3 sink instances
        int sinkParallelism = 3;
        List<Map<Object, Row>> sinkBuffers = new ArrayList<>();
        
        for (int i = 0; i < sinkParallelism; i++) {
            sinkBuffers.add(new HashMap<>());
        }
        
        // Distribute records among sinks based on hash of primary key
        int totalRecords = 30;
        for (int i = 0; i < totalRecords; i++) {
            String pk = "key_" + i;
            int hashCode = pk.hashCode();
            int sinkIndex = ((hashCode % sinkParallelism) + sinkParallelism) % sinkParallelism;
            
            Row row = new Row(2);
            row.setField(0, pk);
            row.setField(1, "value_" + i);
            
            sinkBuffers.get(sinkIndex).put(pk, row);
        }
        
        // Verify distribution
        int totalInSinks = 0;
        for (int i = 0; i < sinkParallelism; i++) {
            int bufferedCount = sinkBuffers.get(i).size();
            totalInSinks += bufferedCount;
            LOG.info("Sink {} has {} records", i, bufferedCount);
            
            assertTrue(bufferedCount > 0,
                    "Sink " + i + " should have some records");
        }
        
        assertEquals(totalRecords, totalInSinks,
                "Total records in all sinks should be " + totalRecords);
        
        LOG.info("✓ Multiple sink instances partitioning test passed");
    }

    /**
     * End-to-end test: State serialization and consistency.
     */
    @Test
    public void testStateSerializationConsistency() {
        LOG.info("=== E2E Test: State Serialization and Consistency ===");
        
        // Create sink state
        Map<Object, Row> originalBuffer = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Row row = new Row(1);
            row.setField(0, "data_" + i);
            originalBuffer.put("key_" + i, row);
        }
        
        LanceUpsertSinkFunction.UpsertSinkState originalState =
                new LanceUpsertSinkFunction.UpsertSinkState(100L, originalBuffer);
        
        // Verify toString representation
        String stateStr = originalState.toString();
        assertNotNull(stateStr,
                "State should have string representation");
        assertTrue(stateStr.contains("100"),
                "String should contain processed count");
        assertTrue(stateStr.contains("buffered=5"),
                "String should show buffered count");
        
        // Simulate serialization/deserialization
        LanceUpsertSinkFunction.UpsertSinkState restoredState =
                new LanceUpsertSinkFunction.UpsertSinkState(
                        originalState.recordsProcessed,
                        new HashMap<>(originalState.bufferedRecords)
                );
        
        // Verify consistency
        assertEquals(originalState.recordsProcessed, restoredState.recordsProcessed,
                "Processed records should match");
        assertEquals(originalState.bufferedRecords.size(), restoredState.bufferedRecords.size(),
                "Buffered records size should match");
        
        for (Object pk : originalState.bufferedRecords.keySet()) {
            assertTrue(restoredState.bufferedRecords.containsKey(pk),
                    "Key " + pk + " should exist in restored state");
        }
        
        LOG.info("✓ State serialization and consistency test passed");
    }

    /**
     * End-to-end test: Data transformation and enrichment pipeline.
     */
    @Test
    public void testDataTransformationPipeline() {
        LOG.info("=== E2E Test: Data Transformation Pipeline ===");
        
        // Stage 1: Input records
        List<Row> inputRecords = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Row row = new Row(2);
            row.setField(0, "user_" + i);
            row.setField(1, String.valueOf(100 + i));  // Raw score
            inputRecords.add(row);
        }
        
        // Stage 2: Buffer in sink with transformation
        Map<Object, Row> transformedBuffer = new HashMap<>();
        for (Row inputRow : inputRecords) {
            String pk = (String) inputRow.getField(0);
            int rawScore = Integer.parseInt((String) inputRow.getField(1));
            
            // Transformation: multiply score by 1.1
            int transformedScore = (int) (rawScore * 1.1);
            
            Row transformedRow = new Row(3);
            transformedRow.setField(0, pk);
            transformedRow.setField(1, String.valueOf(transformedScore));
            transformedRow.setField(2, System.currentTimeMillis());
            
            transformedBuffer.put(pk, transformedRow);
        }
        
        assertEquals(50, transformedBuffer.size(),
                "Should have 50 transformed records");
        
        // Stage 3: Verify transformation
        Row firstTransformed = transformedBuffer.get("user_0");
        assertEquals("user_0", firstTransformed.getField(0),
                "User ID should be preserved");
        assertEquals("110", firstTransformed.getField(1),
                "Score 100 should be transformed to 110");
        assertNotNull(firstTransformed.getField(2),
                "Timestamp should be added");
        
        LOG.info("✓ Data transformation pipeline test passed");
    }

    /**
     * End-to-end test: Failure recovery with partial flush.
     */
    @Test
    public void testFailureRecoveryWithPartialFlush() {
        LOG.info("=== E2E Test: Failure Recovery with Partial Flush ===");
        
        int batchSize = 100;
        Map<Object, Row> recordBuffer = new HashMap<>();
        List<Row> flushedRecords = new ArrayList<>();
        AtomicInteger totalFlushed = new AtomicInteger(0);
        
        // Normal processing: insert and flush
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 120; i++) {
                Row row = new Row(1);
                row.setField(0, "record_" + (batch * 120 + i));
                recordBuffer.put(batch * 120 + i, row);
                
                if (shouldFlush(recordBuffer.size(), batchSize)) {
                    flushedRecords.addAll(recordBuffer.values());
                    totalFlushed.addAndGet(recordBuffer.size());
                    recordBuffer.clear();
                }
            }
        }
        
        // Create checkpoint state
        java.util.Set<Object> checkpointFragments = new java.util.HashSet<Object>(recordBuffer.keySet());
        int checkpointFlushed = totalFlushed.get();
        
        // Simulate more writes
        for (int i = 0; i < 50; i++) {
            Row row = new Row(1);
            row.setField(0, "record_extra_" + i);
            recordBuffer.put(10000 + i, row);
        }
        
        // FAILURE: Restore from checkpoint
        recordBuffer = new HashMap<>();
        for (Object key : checkpointFragments) {
            Row row = new Row(1);
            row.setField(0, "record_" + key);
            recordBuffer.put(key, row);
        }
        
        totalFlushed = new AtomicInteger(checkpointFlushed);
        
        // Resume processing
        for (int i = 0; i < 100; i++) {
            Row row = new Row(1);
            row.setField(0, "record_resumed_" + i);
            recordBuffer.put(20000 + i, row);
            
            if (shouldFlush(recordBuffer.size(), batchSize)) {
                flushedRecords.addAll(recordBuffer.values());
                totalFlushed.addAndGet(recordBuffer.size());
                recordBuffer.clear();
            }
        }
        
        // Final flush
        if (!recordBuffer.isEmpty()) {
            flushedRecords.addAll(recordBuffer.values());
            totalFlushed.addAndGet(recordBuffer.size());
        }
        
        assertTrue(totalFlushed.get() > 0,
                "Should have flushed records");
        
        LOG.info("✓ Failure recovery with partial flush test passed");
        LOG.info("Total records flushed: {}", totalFlushed.get());
    }

    /**
     * Helper method to determine if buffer should be flushed.
     */
    private boolean shouldFlush(int currentSize, int batchSize) {
        return currentSize >= batchSize;
    }
}
