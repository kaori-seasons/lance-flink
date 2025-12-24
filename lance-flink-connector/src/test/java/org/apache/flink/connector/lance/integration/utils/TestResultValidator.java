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
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for validating test results.
 *
 * <p>Provides assertion methods for common test validation scenarios:
 * <ul>
 *   <li>Row count validation</li>
 *   <li>Column matching</li>
 *   <li>Data completeness (no nulls)</li>
 *   <li>Primary key uniqueness</li>
 *   <li>Data ordering</li>
 *   <li>Value range validation</li>
 * </ul>
 */
public class TestResultValidator {

    private TestResultValidator() {
        // Utility class, no instantiation
    }

    // ============================================================================
    // Row Count Validation
    // ============================================================================

    /**
     * Assert that the result has the expected number of rows.
     *
     * @param results The results to validate
     * @param expectedCount The expected row count
     * @throws AssertionError if counts do not match
     */
    public static void assertRowCount(List<Row> results, int expectedCount) {
        assertNotNull("Results should not be null", results);
        assertEquals("Row count mismatch", expectedCount, results.size());
    }

    /**
     * Assert that the result has at least the specified number of rows.
     */
    public static void assertMinRowCount(List<Row> results, int minCount) {
        assertNotNull("Results should not be null", results);
        assertTrue("Row count should be at least " + minCount + ", but was " + results.size(),
                results.size() >= minCount);
    }

    /**
     * Assert that the result has at most the specified number of rows.
     */
    public static void assertMaxRowCount(List<Row> results, int maxCount) {
        assertNotNull("Results should not be null", results);
        assertTrue("Row count should be at most " + maxCount + ", but was " + results.size(),
                results.size() <= maxCount);
    }

    // ============================================================================
    // Column Validation
    // ============================================================================

    /**
     * Assert that all rows have the expected arity (number of columns).
     *
     * @param results The results to validate
     * @param expectedArity The expected number of columns
     * @throws AssertionError if any row has different arity
     */
    public static void assertArity(List<Row> results, int expectedArity) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            Row row = results.get(i);
            assertEquals("Row " + i + " has incorrect arity", expectedArity, row.getArity());
        }
    }

    /**
     * Assert that columns in results match the expected columns.
     * Note: This validates arity, not actual column names (names not available in Row).
     *
     * @param results The results to validate
     * @param expectedColumns Expected column names (validates length)
     * @throws AssertionError if column count does not match
     */
    public static void assertColumnsMatch(List<Row> results, List<String> expectedColumns) {
        assertNotNull("Results should not be null", results);
        if (!results.isEmpty()) {
            assertArity(results, expectedColumns.size());
        }
    }

    // ============================================================================
    // Data Completeness
    // ============================================================================

    /**
     * Assert that no rows in the results are null.
     *
     * @param results The results to validate
     * @throws AssertionError if any row is null
     */
    public static void assertNoNullRows(List<Row> results) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            assertNotNull("Row " + i + " is null", results.get(i));
        }
    }

    /**
     * Assert that a specific field is not null in all rows.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @throws AssertionError if any row has null value in the field
     */
    public static void assertNoNullValues(List<Row> results, int fieldIndex) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            Row row = results.get(i);
            assertNotNull("Row " + i + ", field " + fieldIndex + " is null", row.getField(fieldIndex));
        }
    }

    /**
     * Assert that all fields in all rows are not null (strict validation).
     *
     * @param results The results to validate
     * @throws AssertionError if any field is null
     */
    public static void assertAllFieldsNotNull(List<Row> results) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            Row row = results.get(i);
            for (int j = 0; j < row.getArity(); j++) {
                assertNotNull("Row " + i + ", field " + j + " is null", row.getField(j));
            }
        }
    }

    // ============================================================================
    // Uniqueness Validation
    // ============================================================================

    /**
     * Assert that primary key values are unique across all rows.
     *
     * @param results The results to validate
     * @param primaryKeyFieldIndex The index of the primary key field
     * @throws AssertionError if duplicate primary key values are found
     */
    public static void assertUniqueByPrimaryKey(List<Row> results, int primaryKeyFieldIndex) {
        assertNotNull("Results should not be null", results);
        Set<Object> seenKeys = new HashSet<>();
        
        for (int i = 0; i < results.size(); i++) {
            Row row = results.get(i);
            Object keyValue = row.getField(primaryKeyFieldIndex);
            assertTrue("Duplicate primary key at row " + i + ": " + keyValue,
                    seenKeys.add(keyValue));
        }
    }

    /**
     * Assert that all rows are unique (considering all fields).
     *
     * @param results The results to validate
     * @throws AssertionError if duplicate rows are found
     */
    public static void assertRowsUnique(List<Row> results) {
        assertNotNull("Results should not be null", results);
        Set<Row> seenRows = new HashSet<>();
        
        for (int i = 0; i < results.size(); i++) {
            Row row = results.get(i);
            assertTrue("Duplicate row at index " + i + ": " + row,
                    seenRows.add(row));
        }
    }

    // ============================================================================
    // Data Ordering Validation
    // ============================================================================

    /**
     * Assert that a field is in ascending order across all rows.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @throws AssertionError if field is not in ascending order
     */
    public static void assertFieldAscending(List<Row> results, int fieldIndex) {
        assertNotNull("Results should not be null", results);
        if (results.size() <= 1) {
            return;  // No ordering to check
        }
        
        for (int i = 1; i < results.size(); i++) {
            Comparable<?> prev = (Comparable<?>) results.get(i - 1).getField(fieldIndex);
            Comparable<?> curr = (Comparable<?>) results.get(i).getField(fieldIndex);
            assertTrue("Row " + (i - 1) + " to " + i + " is not in ascending order",
                    compare(prev, curr) <= 0);
        }
    }

    /**
     * Assert that a field is in descending order across all rows.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @throws AssertionError if field is not in descending order
     */
    public static void assertFieldDescending(List<Row> results, int fieldIndex) {
        assertNotNull("Results should not be null", results);
        if (results.size() <= 1) {
            return;  // No ordering to check
        }
        
        for (int i = 1; i < results.size(); i++) {
            Comparable<?> prev = (Comparable<?>) results.get(i - 1).getField(fieldIndex);
            Comparable<?> curr = (Comparable<?>) results.get(i).getField(fieldIndex);
            assertTrue("Row " + (i - 1) + " to " + i + " is not in descending order",
                    compare(prev, curr) >= 0);
        }
    }

    // ============================================================================
    // Value Range Validation
    // ============================================================================

    /**
     * Assert that a field's values are within the specified range.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @param minValue The minimum allowed value
     * @param maxValue The maximum allowed value
     * @throws AssertionError if any value is outside the range
     */
    public static void assertFieldInRange(List<Row> results, int fieldIndex, double minValue, double maxValue) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            double value = ((Number) results.get(i).getField(fieldIndex)).doubleValue();
            assertTrue("Row " + i + " field " + fieldIndex + " value " + value + 
                    " is outside range [" + minValue + ", " + maxValue + "]",
                    value >= minValue && value <= maxValue);
        }
    }

    /**
     * Assert that a string field starts with the expected prefix.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @param prefix The expected prefix
     * @throws AssertionError if any value does not start with prefix
     */
    public static void assertFieldStartsWith(List<Row> results, int fieldIndex, String prefix) {
        assertNotNull("Results should not be null", results);
        for (int i = 0; i < results.size(); i++) {
            String value = (String) results.get(i).getField(fieldIndex);
            assertTrue("Row " + i + " field " + fieldIndex + " value '" + value + 
                    "' does not start with '" + prefix + "'",
                    value.startsWith(prefix));
        }
    }

    // ============================================================================
    // Data Consistency Validation
    // ============================================================================

    /**
     * Assert that all rows have the same number of fields.
     *
     * @param results The results to validate
     * @throws AssertionError if rows have different arities
     */
    public static void assertConsistentArity(List<Row> results) {
        assertNotNull("Results should not be null", results);
        if (results.isEmpty()) {
            return;  // Nothing to check
        }
        
        int expectedArity = results.get(0).getArity();
        for (int i = 1; i < results.size(); i++) {
            assertEquals("Row " + i + " has inconsistent arity",
                    expectedArity, results.get(i).getArity());
        }
    }

    /**
     * Assert that a field has consistent data types across all rows.
     *
     * @param results The results to validate
     * @param fieldIndex The index of the field to check
     * @throws AssertionError if field types are inconsistent
     */
    public static void assertConsistentFieldType(List<Row> results, int fieldIndex) {
        assertNotNull("Results should not be null", results);
        if (results.isEmpty()) {
            return;  // Nothing to check
        }
        
        Class<?> expectedType = results.get(0).getField(fieldIndex).getClass();
        for (int i = 1; i < results.size(); i++) {
            Object fieldValue = results.get(i).getField(fieldIndex);
            assertEquals("Row " + i + " field " + fieldIndex + " has inconsistent type",
                    expectedType, fieldValue.getClass());
        }
    }

    // ============================================================================
    // Helper Methods
    // ============================================================================

    /**
     * Compare two comparable objects.
     * Handles null values gracefully.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Comparable prev, Comparable curr) {
        if (prev == null && curr == null) return 0;
        if (prev == null) return -1;
        if (curr == null) return 1;
        return prev.compareTo(curr);
    }

    /**
     * Convert row list to string for debugging.
     */
    public static String rowsToString(List<Row> results) {
        if (results == null || results.isEmpty()) {
            return "[]";
        }
        
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < Math.min(10, results.size()); i++) {
            sb.append("  [").append(i).append("]: ").append(results.get(i)).append("\n");
        }
        if (results.size() > 10) {
            sb.append("  ... (").append(results.size() - 10).append(" more rows)\n");
        }
        sb.append("]");
        return sb.toString();
    }
}
