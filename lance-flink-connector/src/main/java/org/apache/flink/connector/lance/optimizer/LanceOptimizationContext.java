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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Context for Lance-specific query optimizations.
 * Tracks optimization opportunities and their status.
 *
 * Supports:
 * - Predicate pushdown (WHERE conditions)
 * - Column pruning (SELECT specific columns)
 * - Top-N pushdown (LIMIT with ORDER BY)
 */
public class LanceOptimizationContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Predicates that can be pushed down to storage layer */
    private Optional<String> pushdownPredicate = Optional.empty();

    /** Columns to read (if null/empty, read all) */
    private Set<String> projectedColumns = new HashSet<>();

    /** Top-N limit (if present) */
    private Optional<Long> topNLimit = Optional.empty();

    /** Order by column for Top-N */
    private Optional<String> orderByColumn = Optional.empty();

    /** Is order descending for Top-N */
    private boolean descending = false;

    /** Statistics: estimated rows before optimization */
    private long estimatedRowsBefore = -1L;

    /** Statistics: estimated rows after optimization */
    private long estimatedRowsAfter = -1L;

    /** Optimization enabled */
    private boolean optimizationEnabled = true;

    public LanceOptimizationContext() {
        // Default: all optimizations enabled
    }

    // ====== Predicate Pushdown ======

    public void setPushdownPredicate(String predicate) {
        this.pushdownPredicate = Optional.of(predicate);
    }

    public Optional<String> getPushdownPredicate() {
        return pushdownPredicate;
    }

    public boolean hasPushdownPredicate() {
        return pushdownPredicate.isPresent();
    }

    // ====== Column Pruning ======

    public void addProjectedColumn(String columnName) {
        projectedColumns.add(columnName);
    }

    public void setProjectedColumns(Set<String> columns) {
        this.projectedColumns = new HashSet<>(columns);
    }

    public Set<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean hasColumnPruning() {
        return !projectedColumns.isEmpty();
    }

    // ====== Top-N Pushdown ======

    public void setTopNLimit(long limit) {
        this.topNLimit = Optional.of(limit);
    }

    public Optional<Long> getTopNLimit() {
        return topNLimit;
    }

    public void setOrderByColumn(String columnName, boolean descending) {
        this.orderByColumn = Optional.of(columnName);
        this.descending = descending;
    }

    public Optional<String> getOrderByColumn() {
        return orderByColumn;
    }

    public boolean isDescending() {
        return descending;
    }

    public boolean hasTopNPushdown() {
        return topNLimit.isPresent() && orderByColumn.isPresent();
    }

    // ====== Statistics ======

    public void setEstimatedRowsBefore(long rows) {
        this.estimatedRowsBefore = rows;
    }

    public void setEstimatedRowsAfter(long rows) {
        this.estimatedRowsAfter = rows;
    }

    public long getEstimatedRowsBefore() {
        return estimatedRowsBefore;
    }

    public long getEstimatedRowsAfter() {
        return estimatedRowsAfter;
    }

    public double getOptimizationRatio() {
        if (estimatedRowsBefore <= 0) {
            return 1.0;
        }
        return (double) estimatedRowsAfter / estimatedRowsBefore;
    }

    // ====== Configuration ======

    public void setOptimizationEnabled(boolean enabled) {
        this.optimizationEnabled = enabled;
    }

    public boolean isOptimizationEnabled() {
        return optimizationEnabled;
    }

    public boolean hasAnyOptimization() {
        return hasPushdownPredicate() || hasColumnPruning() || hasTopNPushdown();
    }

    @Override
    public String toString() {
        return "LanceOptimizationContext{" +
                "pushdownPredicate=" + pushdownPredicate +
                ", projectedColumns=" + projectedColumns +
                ", topNLimit=" + topNLimit +
                ", orderByColumn=" + orderByColumn +
                ", descending=" + descending +
                ", estimatedRowsBefore=" + estimatedRowsBefore +
                ", estimatedRowsAfter=" + estimatedRowsAfter +
                ", optimizationRatio=" + String.format("%.2f%%", getOptimizationRatio() * 100) +
                '}';
    }
}
