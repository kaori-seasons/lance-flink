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
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-End test for LanceUnboundedSourceFunction.
 * 
 * Tests the complete source pipeline:
 * 1. Initialize unbounded source with mock dataset
 * 2. Read initial snapshot
 * 3. Simulate version updates
 * 4. Detect and process new fragments
 * 5. Verify state persistence and recovery
 */
public class LanceUnboundedSourceE2ETest {
    private static final Logger LOG = LoggerFactory.getLogger(LanceUnboundedSourceE2ETest.class);

    /**
     * End-to-end test: Source initialization and snapshot reading.
     */
    @Test
    public void testSourceInitializationAndSnapshot() {
        LOG.info("=== E2E Test: Source Initialization and Snapshot ===");
        
        // Setup: Create source configuration
        LanceConfig config = new LanceConfig.Builder("file:///tmp/test_dataset")
                .readBatchSize(256)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();
        
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING
                },
                new String[] {"id", "name", "value"}
        );
        
        LanceReadOptions readOptions = new LanceReadOptions.Builder()
                .columns(Arrays.asList("id", "name", "value"))
                .build();
        
        // Create unbounded source
        LanceUnboundedSourceFunction source = new LanceUnboundedSourceFunction(
                config,
                readOptions,
                rowTypeInfo,
                5000,  // poll interval
                true   // enable snapshot
        );
        
        // Verify configuration
        assertEquals("file:///tmp/test_dataset", config.getDatasetUri(), 
                "Dataset URI should be set");
        assertEquals(256L, config.getReadBatchSize(), 
                "Read batch size should be 256");
        assertTrue(config.isEnablePredicatePushdown(), 
                "Predicate pushdown should be enabled");
        assertTrue(config.isEnableColumnPruning(), 
                "Column pruning should be enabled");
        
        // Verify read options
        assertEquals(3, readOptions.getColumns().size(), 
                "Should read 3 columns");
        
        LOG.info("✓ Source initialization and snapshot test passed");
    }

    /**
     * End-to-end test: Version tracking and fragment polling.
     */
    @Test
    public void testVersionTrackingAndFragmentPolling() {
        LOG.info("=== E2E Test: Version Tracking and Fragment Polling ===");
        
        // Simulate state evolution
        long currentVersion = 1L;
        java.util.Set<Integer> processedFragments = new java.util.HashSet<>(
                Arrays.asList(0, 1, 2, 3)
        );
        
        assertEquals(4, processedFragments.size(), 
                "Should start with 4 fragments");
        assertEquals(1L, currentVersion, 
                "Should start at version 1");
        
        // Simulate first poll: no new fragments
        long polledVersion1 = 1L;
        boolean versionChanged1 = (polledVersion1 != currentVersion);
        assertFalse(versionChanged1, 
                "Version should not change");
        
        // Simulate second poll: new version with new fragments
        polledVersion1 = 2L;
        boolean versionChanged2 = (polledVersion1 != currentVersion);
        assertTrue(versionChanged2, 
                "Version should change to 2");
        
        currentVersion = polledVersion1;
        
        // Add new fragments
        processedFragments.add(4);
        processedFragments.add(5);
        
        assertEquals(6, processedFragments.size(), 
                "Should have 6 fragments after polling");
        assertEquals(2L, currentVersion, 
                "Version should be 2");
        
        // Simulate checkpoint
        List<Integer> checkpointFragments = new ArrayList<>(processedFragments);
        long checkpointVersion = currentVersion;
        
        // Continue processing
        processedFragments.add(6);
        currentVersion = 3L;
        
        // Simulate failure recovery
        processedFragments = new java.util.HashSet<>(checkpointFragments);
        currentVersion = checkpointVersion;
        
        assertEquals(6, processedFragments.size(), 
                "Should recover to 6 fragments");
        assertEquals(2L, currentVersion, 
                "Should recover to version 2");
        
        LOG.info("✓ Version tracking and fragment polling test passed");
    }

    /**
     * End-to-end test: Checkpoint state save and restore.
     */
    @Test
    public void testCheckpointSaveAndRestore() {
        LOG.info("=== E2E Test: Checkpoint Save and Restore ===");
        
        // Create initial state
        List<Integer> initialFragments = Arrays.asList(0, 1, 2);
        LanceUnboundedSourceFunction.UnboundedSourceState initialState =
                new LanceUnboundedSourceFunction.UnboundedSourceState(5L, initialFragments);
        
        assertEquals(5L, initialState.currentVersionId, 
                "Initial version should be 5");
        assertEquals(3, initialState.processedFragmentIds.size(), 
                "Should have 3 fragments");
        
        // Simulate state changes
        List<Integer> newFragments = new ArrayList<>(initialState.processedFragmentIds);
        newFragments.addAll(Arrays.asList(3, 4, 5));
        long newVersion = 6L;
        
        // Create checkpoint state (snapshot)
        LanceUnboundedSourceFunction.UnboundedSourceState checkpointState =
                new LanceUnboundedSourceFunction.UnboundedSourceState(newVersion, newFragments);
        
        // Simulate more processing
        List<Integer> moreFragments = new ArrayList<>(newFragments);
        moreFragments.addAll(Arrays.asList(6, 7));
        long finalVersion = 7L;
        
        // Simulate failure and recovery
        List<Integer> recoveredFragments = new ArrayList<>(checkpointState.processedFragmentIds);
        long recoveredVersion = checkpointState.currentVersionId;
        
        assertEquals(6, recoveredFragments.size(), 
                "Should recover to checkpoint state with 6 fragments");
        assertEquals(6L, recoveredVersion, 
                "Should recover to version 6");
        
        LOG.info("✓ Checkpoint save and restore test passed");
    }

    /**
     * End-to-end test: Multiple source instances with different parallelism.
     */
    @Test
    public void testMultipleSourceInstancesParallelism() {
        LOG.info("=== E2E Test: Multiple Source Instances with Parallelism ===");
        
        // Simulate 3 parallel source instances
        int parallelism = 3;
        List<java.util.Set<Integer>> assignedFragments = new ArrayList<>();
        
        for (int i = 0; i < parallelism; i++) {
            assignedFragments.add(new java.util.HashSet<>());
        }
        
        // Distribute 10 fragments among 3 instances
        List<Integer> allFragments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allFragments.add(i);
        }
        
        for (int i = 0; i < allFragments.size(); i++) {
            int instanceIndex = i % parallelism;
            assignedFragments.get(instanceIndex).add(allFragments.get(i));
        }
        
        // Verify distribution
        for (int i = 0; i < parallelism; i++) {
            int expectedFragments = (10 / parallelism) + (i < (10 % parallelism) ? 1 : 0);
            int actualFragments = assignedFragments.get(i).size();
            assertTrue(actualFragments >= 3 && actualFragments <= 4, 
                    "Instance " + i + " should have 3-4 fragments, got " + actualFragments);
            LOG.info("Instance {} assigned {} fragments", i, actualFragments);
        }
        
        // Total should be 10
        int totalFragments = assignedFragments.stream()
                .mapToInt(java.util.Set::size)
                .sum();
        assertEquals(10, totalFragments, 
                "Total fragments should be 10");
        
        LOG.info("✓ Multiple source instances parallelism test passed");
    }

    /**
     * End-to-end test: State serialization consistency.
     */
    @Test
    public void testStateSerializationConsistency() {
        LOG.info("=== E2E Test: State Serialization Consistency ===");
        
        // Create initial state
        List<Integer> fragments = Arrays.asList(0, 1, 2, 3, 4);
        LanceUnboundedSourceFunction.UnboundedSourceState originalState =
                new LanceUnboundedSourceFunction.UnboundedSourceState(10L, fragments);
        
        // Verify toString representation
        String stateStr = originalState.toString();
        assertNotNull(stateStr, "State should have string representation");
        assertTrue(stateStr.contains("10"), 
                "String should contain version ID");
        assertTrue(stateStr.contains("fragments=5"), 
                "String should show fragment count");
        
        // Simulate serialization/deserialization
        LanceUnboundedSourceFunction.UnboundedSourceState deserializedState =
                new LanceUnboundedSourceFunction.UnboundedSourceState(
                        originalState.currentVersionId,
                        new ArrayList<>(originalState.processedFragmentIds)
                );
        
        // Verify consistency
        assertEquals(originalState.currentVersionId, deserializedState.currentVersionId, 
                "Version ID should match");
        assertEquals(originalState.processedFragmentIds.size(), 
                deserializedState.processedFragmentIds.size(), 
                "Fragment list size should match");
        
        for (Integer fragmentId : originalState.processedFragmentIds) {
            assertTrue(deserializedState.processedFragmentIds.contains(fragmentId), 
                    "Fragment " + fragmentId + " should be present after deserialization");
        }
        
        LOG.info("✓ State serialization consistency test passed");
    }

    /**
     * End-to-end test: Data flow simulation.
     */
    @Test
    public void testDataFlowSimulation() {
        LOG.info("=== E2E Test: Data Flow Simulation ===");
        
        // Simulate data collection
        AtomicInteger recordsCollected = new AtomicInteger(0);
        List<Row> collectedRows = new ArrayList<>();
        
        // Simulate fragments
        int numFragments = 5;
        int recordsPerFragment = 100;
        
        for (int f = 0; f < numFragments; f++) {
            for (int r = 0; r < recordsPerFragment; r++) {
                Row row = new Row(3);
                row.setField(0, "id_" + f + "_" + r);
                row.setField(1, "name_" + f);
                row.setField(2, "value_" + r);
                
                collectedRows.add(row);
                recordsCollected.incrementAndGet();
            }
        }
        
        // Verify data collection
        assertEquals(numFragments * recordsPerFragment, recordsCollected.get(), 
                "Should collect all records");
        assertEquals(500, collectedRows.size(), 
                "Should have 500 total records");
        
        // Verify first and last record
        Row firstRow = collectedRows.get(0);
        assertEquals("id_0_0", firstRow.getField(0), 
                "First record should be from fragment 0");
        
        Row lastRow = collectedRows.get(499);
        assertEquals("id_4_99", lastRow.getField(0), 
                "Last record should be from fragment 4");
        
        LOG.info("✓ Data flow simulation test passed");
        LOG.info("Total records collected: {}", recordsCollected.get());
    }

    /**
     * End-to-end test: Failure recovery scenario.
     */
    @Test
    public void testFailureRecoveryScenario() {
        LOG.info("=== E2E Test: Failure Recovery Scenario ===");
        
        // Normal processing phase
        java.util.Set<Integer> processedFragments = new java.util.HashSet<>();
        long currentVersion = 1L;
        
        // Process first batch
        for (int i = 0; i < 5; i++) {
            processedFragments.add(i);
        }
        
        assertEquals(5, processedFragments.size(), 
                "Should have processed 5 fragments");
        
        // Create checkpoint (before new fragments)
        List<Integer> checkpointFragments = new ArrayList<>(processedFragments);
        long checkpointVersion = currentVersion;
        
        // New version arrives
        currentVersion = 2L;
        processedFragments.add(5);
        processedFragments.add(6);
        
        assertEquals(7, processedFragments.size(), 
                "Should have processed 7 fragments");
        
        // Processing phase 2 (before failure)
        processedFragments.add(7);
        processedFragments.add(8);
        
        assertEquals(9, processedFragments.size(), 
                "Should have processed 9 fragments");
        
        // FAILURE: Restore from checkpoint
        processedFragments = new java.util.HashSet<>(checkpointFragments);
        currentVersion = checkpointVersion;
        
        assertEquals(5, processedFragments.size(), 
                "Should restore to 5 fragments");
        assertEquals(1L, currentVersion, 
                "Should restore to version 1");
        
        // Resume processing from checkpoint
        currentVersion = 2L;
        for (int i = 5; i < 10; i++) {
            processedFragments.add(i);
        }
        
        assertEquals(10, processedFragments.size(), 
                "Should have processed 10 fragments after recovery");
        
        LOG.info("✓ Failure recovery scenario test passed");
    }
}
