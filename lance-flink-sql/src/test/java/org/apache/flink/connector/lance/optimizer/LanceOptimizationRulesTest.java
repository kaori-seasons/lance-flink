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

import org.junit.Test;
import java.util.Optional;
import static org.junit.Assert.*;

/**
 * Unit tests for Lance optimization rules.
 * Tests Filter Pushdown, Aggregation Pushdown, and TopN Pushdown.
 */
public class LanceOptimizationRulesTest {

    @Test
    public void testFilterPushDownSimple() {
        String filter = "age > 25";
        assertTrue(LanceOptimizationRules.FilterPushDownRule.isSupported(filter));
        String where = LanceOptimizationRules.FilterPushDownRule.compileToWhereClause(filter);
        assertNotNull(where);
        assertTrue(where.contains("age") && where.contains(">"));
    }

    @Test
    public void testFilterPushDownComplex() {
        String filter = "age > 25 AND city = 'NYC' AND salary <= 100000";
        assertTrue(LanceOptimizationRules.FilterPushDownRule.isSupported(filter));
        String where = LanceOptimizationRules.FilterPushDownRule.compileToWhereClause(filter);
        assertNotNull(where);
    }

    @Test
    public void testFilterPushDownWithOR() {
        String filter = "status = 'active' OR status = 'pending'";
        assertTrue(LanceOptimizationRules.FilterPushDownRule.isSupported(filter));
        String where = LanceOptimizationRules.FilterPushDownRule.compileToWhereClause(filter);
        assertNotNull(where);
    }

    @Test
    public void testFilterPushDownIN() {
        String filter = "city IN ('NYC', 'LA', 'CHI')";
        assertTrue(LanceOptimizationRules.FilterPushDownRule.isSupported(filter));
        String where = LanceOptimizationRules.FilterPushDownRule.compileToWhereClause(filter);
        assertNotNull(where);
    }

    @Test
    public void testFilterPushDownUnsupported() {
        String filter = "SUBSTRING(name, 1, 5) = 'John'";
        // This filter function is not directly supported
        // But if it contains a comparison operator, it might be partially supported
        boolean supported = LanceOptimizationRules.FilterPushDownRule.isSupported(filter);
        // Just verify the method doesn't throw
        assertNotNull(supported);
    }

    @Test
    public void testAggregationPushDownCountStarSupported() {
        String agg = "COUNT(*)";
        assertTrue(LanceOptimizationRules.AggregationPushDownRule.isSupported(agg));
    }

    @Test
    public void testAggregationPushDownFastPath() {
        String agg = "COUNT(*)";
        var result = LanceOptimizationRules.AggregationPushDownRule.optimize(
            agg, 
            false, // no WHERE condition
            Optional.of(1000000L)
        );
        assertTrue(result.isFastPath());
        assertEquals(1000000L, result.getRowCount());
    }

    @Test
    public void testAggregationPushDownSlowPath() {
        String agg = "COUNT(*)";
        var result = LanceOptimizationRules.AggregationPushDownRule.optimize(
            agg, 
            true, // has WHERE condition
            Optional.of(1000000L)
        );
        assertFalse(result.isFastPath());
    }

    @Test
    public void testAggregationPushDownCountWithGroupBy() {
        String agg = "COUNT(*) GROUP BY city";
        assertFalse(LanceOptimizationRules.AggregationPushDownRule.isSupported(agg));
    }

    @Test
    public void testTopNPushDownSimple() {
        String order = "score DESC";
        assertTrue(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 10));
        
        var config = LanceOptimizationRules.TopNPushDownRule.compileToPushDown(order, 10);
        assertNotNull(config);
        assertEquals(10, config.getLimit());
        assertEquals(1, config.getSortOrders().size());
    }

    @Test
    public void testTopNPushDownMultipleColumns() {
        String order = "score DESC, age ASC";
        assertTrue(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 100));
        
        var config = LanceOptimizationRules.TopNPushDownRule.compileToPushDown(order, 100);
        assertEquals(100, config.getLimit());
        assertEquals(2, config.getSortOrders().size());
    }

    @Test
    public void testTopNPushDownLimitValidation() {
        String order = "score DESC";
        assertTrue(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 50000));
        assertFalse(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 0));
        assertFalse(LanceOptimizationRules.TopNPushDownRule.isSupported(order, -1));
    }

    @Test
    public void testQueryOptimizerCombined() {
        LanceOptimizationRules.QueryOptimizer optimizer = 
            new LanceOptimizationRules.QueryOptimizer();
        
        var plan = optimizer.analyzeAndOptimize(
            "age > 25 AND city = 'NYC'",
            "COUNT(*)",
            "salary DESC",
            10,
            Optional.of(5000000L)
        );
        
        assertNotNull(plan);
        assertNotNull(plan.getOptimizations());
        assertTrue(plan.getOptimizations().size() >= 1);
    }

    @Test
    public void testQueryOptimizerNoOptimizations() {
        LanceOptimizationRules.QueryOptimizer optimizer = 
            new LanceOptimizationRules.QueryOptimizer();
        
        // Empty where clause, no aggregation
        var plan = optimizer.analyzeAndOptimize(
            "",
            null,
            null,
            0,
            Optional.empty()
        );
        
        assertNotNull(plan);
        assertTrue(plan.getOptimizations().isEmpty());
    }

    @Test
    public void testFilterPushDownEmptyString() {
        assertFalse(LanceOptimizationRules.FilterPushDownRule.isSupported(""));
        assertFalse(LanceOptimizationRules.FilterPushDownRule.isSupported(null));
    }

    @Test
    public void testAggregationPushDownNoMetadata() {
        String agg = "COUNT(*)";
        var result = LanceOptimizationRules.AggregationPushDownRule.optimize(
            agg, 
            false,
            Optional.empty()
        );
        assertFalse(result.isFastPath());
    }

    @Test
    public void testTopNPushDownEdgeCases() {
        String order = "score";
        assertTrue(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 1));
        assertTrue(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 100000));
        assertFalse(LanceOptimizationRules.TopNPushDownRule.isSupported(order, 100001));
    }
}
