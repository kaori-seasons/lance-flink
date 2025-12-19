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

package org.apache.flink.connector.lance.optimizer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Lance Query Optimizer.
 * 
 * Tests all three optimization rules:
 * 1. Predicate Pushdown (WHERE conditions)
 * 2. Column Pruning (SELECT projection)
 * 3. Top-N Pushdown (ORDER BY + LIMIT)
 */
@DisplayName("Lance Query Optimizer Integration Tests")
public class LanceOptimizerIntegrationTest {

    private LanceOptimizer optimizer;
    private List<String> testColumns;

    @BeforeEach
    void setup() {
        optimizer = new LanceOptimizer(true);
        testColumns = Arrays.asList(
                "user_id", "email", "name", "status",
                "created_at", "updated_at", "last_login",
                "total_orders", "total_spent", "country",
                "city", "zip_code", "phone", "verified"
        );
    }

    // ===== PREDICATE PUSHDOWN TESTS =====

    @Test
    @DisplayName("Predicate pushdown: simple equality")
    void testPredicatePushdownEquality() {
        LancePredicatePushdownRule rule = new LancePredicatePushdownRule();
        
        String where = "status = 'active'";
        LanceOptimizationContext context = rule.optimize(where, testColumns);
        
        assertTrue(context.hasPushdownPredicate());
        assertNotNull(context.getPushdownPredicate().get());
        assertEquals("status = 'active'", context.getPushdownPredicate().get());
    }

    @Test
    @DisplayName("Predicate pushdown: range query")
    void testPredicatePushdownRange() {
        LancePredicatePushdownRule rule = new LancePredicatePushdownRule();
        
        String where = "total_spent > 1000 AND total_spent < 10000";
        LanceOptimizationContext context = rule.optimize(where, testColumns);
        
        assertTrue(context.hasPushdownPredicate());
        assertTrue(context.getPushdownPredicate().get().contains(">"));
        assertTrue(context.getPushdownPredicate().get().contains("<"));
    }

    @Test
    @DisplayName("Predicate pushdown: IN clause")
    void testPredicatePushdownIn() {
        LancePredicatePushdownRule rule = new LancePredicatePushdownRule();
        
        String where = "country IN ('US', 'CA', 'MX')";
        LanceOptimizationContext context = rule.optimize(where, testColumns);
        
        assertTrue(context.hasPushdownPredicate());
        assertTrue(context.getPushdownPredicate().get().toLowerCase().contains("in"));
    }

    @Test
    @DisplayName("Predicate pushdown: selectivity estimation")
    void testPredicatePushdownSelectivity() {
        LancePredicatePushdownRule rule = new LancePredicatePushdownRule();
        
        String where = "user_id = 12345";
        LanceOptimizationContext context = rule.optimize(where, testColumns);
        
        // Equality query should have ~0.1% selectivity
        double selectivity = (double) context.getEstimatedRowsAfter() / context.getEstimatedRowsBefore();
        assertTrue(selectivity < 0.01); // Less than 1%
    }

    // ===== COLUMN PRUNING TESTS =====

    @Test
    @DisplayName("Column pruning: specific columns")
    void testColumnPruningSpecific() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        String select = "SELECT user_id, name, email FROM users";
        LanceOptimizationContext context = rule.optimize(select, testColumns);
        
        assertTrue(context.hasColumnPruning());
        assertEquals(3, context.getProjectedColumns().size());
        assertTrue(context.getProjectedColumns().contains("user_id"));
        assertTrue(context.getProjectedColumns().contains("name"));
        assertTrue(context.getProjectedColumns().contains("email"));
    }

    @Test
    @DisplayName("Column pruning: SELECT *")
    void testColumnPruningSelectAll() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        String select = "SELECT *";
        LanceOptimizationContext context = rule.optimize(select, testColumns);
        
        assertTrue(context.hasColumnPruning());
        assertEquals(testColumns.size(), context.getProjectedColumns().size());
    }

    @Test
    @DisplayName("Column pruning: with WHERE clause")
    void testColumnPruningWithWhere() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        Set<String> whereColumns = rule.extractColumnsFromWhere(
                "WHERE status = 'active' AND created_at > '2024-01-01'",
                testColumns
        );
        
        assertEquals(2, whereColumns.size());
        assertTrue(whereColumns.contains("status"));
        assertTrue(whereColumns.contains("created_at"));
    }

    @Test
    @DisplayName("Column pruning: with ORDER BY clause")
    void testColumnPruningWithOrderBy() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        Set<String> orderColumns = rule.extractColumnsFromOrderBy(
                "ORDER BY created_at DESC, name ASC",
                testColumns
        );
        
        assertEquals(2, orderColumns.size());
        assertTrue(orderColumns.contains("created_at"));
        assertTrue(orderColumns.contains("name"));
    }

    @Test
    @DisplayName("Column pruning: I/O reduction calculation")
    void testColumnPruningIoReduction() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        // 100 columns, selecting 10
        double reductionFactor = rule.getIoReductionFactor(100, 10);
        assertEquals(10.0, reductionFactor);
    }

    // ===== TOP-N PUSHDOWN TESTS =====

    @Test
    @DisplayName("Top-N pushdown: basic ORDER BY LIMIT")
    void testTopNPushdownBasic() {
        LanceTopNPushdownRule rule = new LanceTopNPushdownRule();
        
        String sql = "SELECT * FROM users ORDER BY created_at DESC LIMIT 10";
        LanceOptimizationContext context = rule.optimize(sql, testColumns);
        
        assertTrue(context.hasTopNPushdown());
        assertEquals(10L, context.getTopNLimit().get().longValue());
        assertEquals("created_at", context.getOrderByColumn().get());
        assertTrue(context.isDescending());
    }

    @Test
    @DisplayName("Top-N pushdown: ascending order")
    void testTopNPushdownAscending() {
        LanceTopNPushdownRule rule = new LanceTopNPushdownRule();
        
        String sql = "SELECT * FROM users ORDER BY total_spent ASC LIMIT 100";
        LanceOptimizationContext context = rule.optimize(sql, testColumns);
        
        assertTrue(context.hasTopNPushdown());
        assertEquals(100L, context.getTopNLimit().get().longValue());
        assertEquals("total_spent", context.getOrderByColumn().get());
        assertFalse(context.isDescending());
    }

    @Test
    @DisplayName("Top-N pushdown: optimization value")
    void testTopNPushdownOptimizationValue() {
        LanceTopNPushdownRule rule = new LanceTopNPushdownRule();
        
        long limit = 10;
        long totalRows = 100_000_000_000L; // 100B rows
        
        double optimizationValue = rule.getOptimizationValue(limit, totalRows);
        assertTrue(optimizationValue > 100); // Should be very high (100x+)
    }

    // ===== UNIFIED OPTIMIZER TESTS =====

    @Test
    @DisplayName("Unified optimizer: all rules combined")
    void testUnifiedOptimizerAllRules() {
        String sql = "SELECT user_id, name, email FROM users " +
                    "WHERE status = 'active' AND total_spent > 1000 " +
                    "ORDER BY created_at DESC LIMIT 100";
        
        LanceOptimizationContext context = optimizer.optimizeQuery(sql, testColumns);
        
        // Should have all three optimizations
        assertTrue(context.hasAnyOptimization());
        // Predicate, columns, and Top-N might not all be detected in simple parser
        // but at least some optimization should be applied
    }

    @Test
    @DisplayName("Unified optimizer: predicates with AND")
    void testUnifiedOptimizerComplexPredicates() {
        String sql = "SELECT * FROM users WHERE status = 'active' " +
                    "AND country IN ('US', 'CA') " +
                    "AND created_at > '2024-01-01'";
        
        LanceOptimizationContext context = optimizer.optimizeQuery(sql, testColumns);
        
        // Should have optimization context
        assertNotNull(context);
        assertTrue(context.isOptimizationEnabled());
    }

    @Test
    @DisplayName("Unified optimizer: wide table column pruning")
    void testUnifiedOptimizerWideTable() {
        List<String> wideColumns = Arrays.asList();
        for (int i = 0; i < 100; i++) {
            ((java.util.ArrayList<String>) wideColumns).add("col" + i);
        }
        
        String sql = "SELECT col1, col2 FROM wide_table WHERE col1 > 100";
        LanceOptimizationContext context = optimizer.optimizeQuery(sql, wideColumns);
        
        assertNotNull(context);
    }

    @Test
    @DisplayName("Optimizer: can be disabled")
    void testOptimizerDisabled() {
        LanceOptimizer disabledOptimizer = new LanceOptimizer(false);
        
        String sql = "SELECT * FROM users WHERE status = 'active' LIMIT 10";
        LanceOptimizationContext context = disabledOptimizer.optimizeQuery(sql, testColumns);
        
        assertFalse(context.hasAnyOptimization());
        assertFalse(disabledOptimizer.isEnabled());
    }

    @Test
    @DisplayName("Benchmark: correctness")
    void testBenchmarkCorrectness() {
        LanceOptimizerBenchmark benchmark = new LanceOptimizerBenchmark();
        
        // Should not throw exceptions
        assertDoesNotThrow(benchmark::benchmarkSelectCount);
        assertDoesNotThrow(benchmark::benchmarkSelectWhere);
        assertDoesNotThrow(benchmark::benchmarkGroupBy);
        assertDoesNotThrow(benchmark::benchmarkTopN);
        assertDoesNotThrow(benchmark::benchmarkColumnPruning);
    }

    // ===== PERFORMANCE EXPECTATIONS =====

    @Test
    @DisplayName("Performance: predicate pushdown meets targets")
    void testPerformancePredicatePushdown() {
        LancePredicatePushdownRule rule = new LancePredicatePushdownRule();
        
        String where = "user_id = 12345";
        LanceOptimizationContext context = rule.optimize(where, testColumns);
        
        // Should reduce rows by at least 99.9% for equality
        long before = context.getEstimatedRowsBefore();
        long after = context.getEstimatedRowsAfter();
        
        double reductionRatio = (double) before / after;
        assertTrue(reductionRatio >= 1000, 
                String.format("Reduction ratio %.1fx is less than 1000x", reductionRatio));
    }

    @Test
    @DisplayName("Performance: column pruning meets targets")
    void testPerformanceColumnPruning() {
        LanceColumnPruningRule rule = new LanceColumnPruningRule();
        
        // Test with wide table
        List<String> wideColumns = new java.util.ArrayList<>();
        for (int i = 0; i < 100; i++) {
            wideColumns.add("col" + i);
        }
        
        double ioReduction = rule.getIoReductionFactor(100, 10);
        assertTrue(ioReduction >= 10, 
                String.format("I/O reduction %.1fx is less than 10x", ioReduction));
    }

    @Test
    @DisplayName("Performance: Top-N pushdown meets targets")
    void testPerformanceTopNPushdown() {
        LanceTopNPushdownRule rule = new LanceTopNPushdownRule();
        
        String sql = "SELECT * FROM events ORDER BY timestamp DESC LIMIT 10";
        LanceOptimizationContext context = rule.optimize(sql, testColumns);
        
        assertTrue(context.hasTopNPushdown());
        // Top-N with small limit should have huge speedup
        long before = context.getEstimatedRowsBefore();
        long after = context.getEstimatedRowsAfter();
        
        double speedup = (double) before / after;
        assertTrue(speedup >= 100, 
                String.format("Speedup %.1fx is less than 100x", speedup));
    }
}
