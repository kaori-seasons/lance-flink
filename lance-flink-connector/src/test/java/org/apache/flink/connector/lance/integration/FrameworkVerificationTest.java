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

package org.apache.flink.connector.lance.integration;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.integration.utils.TestDataGenerator;
import org.apache.flink.connector.lance.integration.utils.TestResultValidator;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Sample integration test to verify the testing framework.
 * This test demonstrates basic usage of the MiniClusterIntegrationTestBase.
 */
public class FrameworkVerificationTest extends MiniClusterIntegrationTestBase {

    @Test
    public void testDataGeneratorBasic() {
        // Generate 100 rows with 4 columns
        List<Row> rows = TestDataGenerator.generateRows(100, 4);
        
        // Verify count and structure
        assertEquals("Should generate 100 rows", 100, rows.size());
        for (Row row : rows) {
            assertEquals("Each row should have 4 fields", 4, row.getArity());
            assertNotNull("Row should not be null", row);
        }
    }

    @Test
    public void testDataGeneratorWithPrimaryKey() {
        // Generate rows with primary key
        List<Row> rows = TestDataGenerator.generateRowsWithPrimaryKey(50, "test_key");
        
        assertEquals("Should generate 50 rows", 50, rows.size());
        
        // Verify primary key uniqueness
        TestResultValidator.assertUniqueByPrimaryKey(rows, 0);
    }

    @Test
    public void testDataGeneratorUpsert() {
        // Generate rows for upsert testing (100 initial + 30 updates)
        List<Row> rows = TestDataGenerator.generateRowsForUpsert(100, 30);
        
        // Should have 130 total rows (100 initial + 30 updates)
        assertEquals("Should have 130 rows total", 130, rows.size());
    }

    @Test
    public void testDataGeneratorWithValueRange() {
        // Generate rows with values in range [0, 1000]
        List<Row> rows = TestDataGenerator.generateRowsWithValueRange(50, 0, 1000);
        
        assertEquals("Should generate 50 rows", 50, rows.size());
        
        // Verify all values are in range
        TestResultValidator.assertFieldInRange(rows, 2, 0, 1000);
    }

    @Test
    public void testResultValidatorRowCount() {
        List<Row> rows = TestDataGenerator.generateRows(100);
        
        // Should pass
        TestResultValidator.assertRowCount(rows, 100);
        TestResultValidator.assertMinRowCount(rows, 50);
        TestResultValidator.assertMaxRowCount(rows, 150);
        
        // Should fail
        assertThrows(AssertionError.class, () -> {
            TestResultValidator.assertRowCount(rows, 99);
        });
    }

    @Test
    public void testResultValidatorArity() {
        List<Row> rows = TestDataGenerator.generateRows(50, 4);
        
        // Verify arity
        TestResultValidator.assertArity(rows, 4);
        TestResultValidator.assertConsistentArity(rows);
        
        // Should fail with wrong arity
        assertThrows(AssertionError.class, () -> {
            TestResultValidator.assertArity(rows, 3);
        });
    }

    @Test
    public void testResultValidatorUniqueByPrimaryKey() {
        List<Row> rows = TestDataGenerator.generateRowsWithPrimaryKey(50);
        
        // Should pass for primary key (id field, index 0)
        TestResultValidator.assertUniqueByPrimaryKey(rows, 0);
        
        // Should fail with duplicate keys
        assertThrows(AssertionError.class, () -> {
            List<Row> duplicates = TestDataGenerator.generateRowsWithPrimaryKey(10);
            duplicates.addAll(duplicates);  // Add duplicates
            TestResultValidator.assertUniqueByPrimaryKey(duplicates, 0);
        });
    }

    @Test
    public void testResultValidatorOrdering() {
        List<Row> rows = TestDataGenerator.generateRows(50, 4);
        
        // The generated rows should be in ascending order by id (field 0)
        TestResultValidator.assertFieldAscending(rows, 0);
        
        // Should fail for descending check
        assertThrows(AssertionError.class, () -> {
            TestResultValidator.assertFieldDescending(rows, 0);
        });
    }

    @Test
    public void testResultValidatorNoNullRows() {
        List<Row> rows = TestDataGenerator.generateRows(50);
        
        // Should pass - no null rows
        TestResultValidator.assertNoNullRows(rows);
        
        // Verify specific field non-null
        TestResultValidator.assertNoNullValues(rows, 0);
        TestResultValidator.assertNoNullValues(rows, 1);
    }

    @Test
    public void testMetricsCollector() {
        var metrics = createMetricsCollector();
        
        // Test timer
        metrics.startTimer();
        sleep(100);  // Sleep 100ms
        long elapsed = metrics.stopTimer();
        
        assertTrue("Elapsed time should be >= 100ms", elapsed >= 100);
        
        // Test throughput calculation
        double throughput = metrics.calculateThroughput(1000);
        assertTrue("Throughput should be reasonable", throughput < 20000);  // Less than 20K rows/sec at 100ms
        
        // Test metric recording
        metrics.recordMetric("test_metric", 123.45);
        assertEquals("Metric value should match", 123.45, metrics.getMetric("test_metric"), 0.01);
    }

    @Test
    public void testConfigBuilders() {
        // Test configuration builders
        var config = createDefaultLanceConfig();
        assertNotNull("Config should not be null", config);
        
        var readOptions = createReadOptionsWithColumns(java.util.Arrays.asList("id", "name"));
        assertNotNull("Read options should not be null", readOptions);
        
        var writeOptions = createWriteOptionsWithMode(
                org.apache.flink.connector.lance.common.LanceWriteOptions.WriteMode.APPEND);
        assertNotNull("Write options should not be null", writeOptions);
    }

    @Test
    public void testStreamEnvironment() throws Exception {
        var env = createTestStreamEnvironment();
        assertNotNull("Stream environment should not be null", env);
        assertEquals("Parallelism should be 2", 2, env.getParallelism());
    }

    // Helper method
    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
