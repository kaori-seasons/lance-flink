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

package org.apache.flink.connector.lance.integration.utils;

import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating test data.
 *
 * <p>Provides methods to generate test datasets with various configurations:
 * <ul>
 *   <li>Basic rows with default schema</li>
 *   <li>Rows with primary key</li>
 *   <li>Rows with timestamp</li>
 *   <li>Rows with custom column count</li>
 * </ul>
 */
public class TestDataGenerator {

    private static final Random RANDOM = new Random(42);  // Fixed seed for reproducibility

    private TestDataGenerator() {
        // Utility class, no instantiation
    }

    // ============================================================================
    // Basic Row Generation
    // ============================================================================

    /**
     * Generate basic test rows with specified row count and column count.
     *
     * <p>Schema: (id LONG, name STRING, value DOUBLE, timestamp LONG, ...)
     *
     * @param rowCount Number of rows to generate
     * @param columnCount Number of columns (will add extra STRING columns if > 4)
     * @return List of Row objects
     */
    public static List<Row> generateRows(int rowCount, int columnCount) {
        List<Row> rows = new ArrayList<>(rowCount);
        for (long i = 0; i < rowCount; i++) {
            Object[] fields = new Object[columnCount];
            fields[0] = i;                                           // id (LONG)
            fields[1] = "name_" + i;                                // name (STRING)
            fields[2] = RANDOM.nextDouble() * 1000;                // value (DOUBLE)
            fields[3] = System.currentTimeMillis() + i;            // timestamp (LONG)
            
            // Fill remaining columns with string data
            for (int j = 4; j < columnCount; j++) {
                fields[j] = "col_" + j + "_row_" + i;
            }
            
            rows.add(Row.of(fields));
        }
        return rows;
    }

    /**
     * Generate rows with 4 default columns.
     * Schema: (id LONG, name STRING, value DOUBLE, timestamp LONG)
     */
    public static List<Row> generateRows(int rowCount) {
        return generateRows(rowCount, 4);
    }

    // ============================================================================
    // Rows with Primary Key
    // ============================================================================

    /**
     * Generate rows with explicit primary key values.
     *
     * <p>Schema: (id LONG, name STRING, value DOUBLE, timestamp LONG)
     *
     * @param rowCount Number of rows to generate
     * @param keyPrefix Prefix for name field (used as part of primary key)
     * @return List of Row objects
     */
    public static List<Row> generateRowsWithPrimaryKey(int rowCount, String keyPrefix) {
        List<Row> rows = new ArrayList<>(rowCount);
        for (long i = 0; i < rowCount; i++) {
            rows.add(Row.of(
                    i,                                              // id (PK)
                    keyPrefix + "_" + i,                           // name
                    RANDOM.nextDouble() * 1000,                   // value
                    System.currentTimeMillis() + i                // timestamp
            ));
        }
        return rows;
    }

    /**
     * Generate rows with incrementing primary key (1-based).
     */
    public static List<Row> generateRowsWithPrimaryKey(int rowCount) {
        return generateRowsWithPrimaryKey(rowCount, "key");
    }

    // ============================================================================
    // Rows with Timestamp
    // ============================================================================

    /**
     * Generate rows with realistic timestamps spanning multiple days.
     *
     * <p>Schema: (id LONG, name STRING, value DOUBLE, timestamp LONG)
     *
     * @param rowCount Number of rows to generate
     * @param startTimeMillis Start timestamp in milliseconds
     * @param intervalMillis Interval between consecutive rows in milliseconds
     * @return List of Row objects
     */
    public static List<Row> generateRowsWithTimestamp(int rowCount, long startTimeMillis, long intervalMillis) {
        List<Row> rows = new ArrayList<>(rowCount);
        long currentTime = startTimeMillis;
        
        for (long i = 0; i < rowCount; i++) {
            rows.add(Row.of(
                    i,                              // id
                    "event_" + i,                  // name
                    RANDOM.nextDouble() * 1000,   // value
                    currentTime                    // timestamp
            ));
            currentTime += intervalMillis;
        }
        return rows;
    }

    /**
     * Generate rows with timestamps at 1-second intervals starting from current time.
     */
    public static List<Row> generateRowsWithTimestamp(int rowCount) {
        return generateRowsWithTimestamp(rowCount, System.currentTimeMillis(), 1000);
    }

    // ============================================================================
    // Rows for Update Scenarios
    // ============================================================================

    /**
     * Generate rows for update/upsert testing.
     * Returns both initial insert and update records.
     *
     * @param initialRowCount Number of initial rows
     * @param updateRowCount Number of rows to update
     * @return List of all rows (initial + updates)
     */
    public static List<Row> generateRowsForUpsert(int initialRowCount, int updateRowCount) {
        List<Row> rows = new ArrayList<>(initialRowCount + updateRowCount);
        
        // Generate initial rows
        for (long i = 0; i < initialRowCount; i++) {
            rows.add(Row.of(
                    i,                                              // id (PK)
                    "name_" + i,                                   // name
                    RANDOM.nextDouble() * 1000,                   // value
                    System.currentTimeMillis()                    // timestamp
            ));
        }
        
        // Generate update rows (update first N rows)
        for (long i = 0; i < updateRowCount; i++) {
            rows.add(Row.of(
                    i,                                              // id (same PK)
                    "updated_name_" + i,                          // updated name
                    RANDOM.nextDouble() * 2000,                   // new value
                    System.currentTimeMillis() + 1000            // new timestamp
            ));
        }
        
        return rows;
    }

    // ============================================================================
    // Rows with Specific Data Patterns
    // ============================================================================

    /**
     * Generate rows with specific value ranges for filtering tests.
     *
     * @param rowCount Number of rows to generate
     * @param minValue Minimum value for the value field
     * @param maxValue Maximum value for the value field
     * @return List of Row objects
     */
    public static List<Row> generateRowsWithValueRange(int rowCount, double minValue, double maxValue) {
        List<Row> rows = new ArrayList<>(rowCount);
        for (long i = 0; i < rowCount; i++) {
            double value = minValue + (maxValue - minValue) * (i % rowCount) / (double) rowCount;
            rows.add(Row.of(
                    i,                                  // id
                    "name_" + i,                       // name
                    value,                             // value in range
                    System.currentTimeMillis() + i    // timestamp
            ));
        }
        return rows;
    }

    /**
     * Generate rows with specific patterns for predicate pushdown testing.
     *
     * @param rowCount Number of rows to generate
     * @param filterValue Value to use for filtering tests
     * @param percentMatch Percentage of rows that should match the filter (0-100)
     * @return List of Row objects
     */
    public static List<Row> generateRowsWithPattern(int rowCount, String filterValue, int percentMatch) {
        List<Row> rows = new ArrayList<>(rowCount);
        int matchCount = (int) (rowCount * percentMatch / 100.0);
        
        for (long i = 0; i < rowCount; i++) {
            String nameValue = (i < matchCount) ? filterValue : "other_" + i;
            rows.add(Row.of(
                    i,                                  // id
                    nameValue,                         // name (matches for first percentMatch%)
                    RANDOM.nextDouble() * 1000,       // value
                    System.currentTimeMillis() + i    // timestamp
            ));
        }
        return rows;
    }

    // ============================================================================
    // Bulk Generation
    // ============================================================================

    /**
     * Generate large dataset for performance testing.
     *
     * @param rowCount Number of rows (can be very large: 100K, 1M, etc.)
     * @return List of Row objects
     */
    public static List<Row> generateLargeDataset(int rowCount) {
        return generateRows(rowCount, 4);
    }

    /**
     * Generate dataset in batches (useful for memory-constrained scenarios).
     *
     * @param totalRows Total number of rows to generate
     * @param batchSize Size of each batch
     * @return Generator function that yields batches
     */
    public static List<List<Row>> generateRowsInBatches(int totalRows, int batchSize) {
        List<List<Row>> batches = new ArrayList<>();
        int remainingRows = totalRows;
        
        while (remainingRows > 0) {
            int currentBatchSize = Math.min(batchSize, remainingRows);
            List<Row> batch = generateRows(currentBatchSize);
            batches.add(batch);
            remainingRows -= currentBatchSize;
        }
        
        return batches;
    }

    // ============================================================================
    // Utility Methods
    // ============================================================================

    /**
     * Reset the random seed for reproducible test data generation.
     */
    public static void resetRandomSeed(long seed) {
        synchronized (RANDOM) {
            RANDOM.setSeed(seed);
        }
    }

    /**
     * Generate a random string for testing.
     */
    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
