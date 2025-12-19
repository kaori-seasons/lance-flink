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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Column Pruning Rule for Lance.
 * 
 * Optimizes queries by only reading necessary columns from storage layer.
 * Lance is a columnar format, so reading only required columns significantly
 * improves performance and reduces memory usage.
 * 
 * Eliminates:
 * - Columns not in SELECT clause
 * - Columns not referenced in WHERE/ORDER BY/GROUP BY
 * 
 * Performance impact:
 * - Reduces I/O by ~column_count ratio
 * - With 100 columns, selecting 10: ~10x I/O reduction
 * - Typical improvement: 2-5x for wide tables
 */
public class LanceColumnPruningRule {
    private static final Logger LOG = LoggerFactory.getLogger(LanceColumnPruningRule.class);

    /**
     * Extracts projected columns from a SQL SELECT statement.
     * 
     * @param selectSql SQL SELECT clause (including SELECT keyword)
     * @param fromColumns all available columns in the table
     * @return optimization context with projected columns
     */
    public LanceOptimizationContext optimize(String selectSql, List<String> fromColumns) {
        LanceOptimizationContext context = new LanceOptimizationContext();

        if (selectSql == null || selectSql.trim().isEmpty()) {
            LOG.debug("No SELECT clause to analyze");
            return context;
        }

        try {
            Set<String> selectedColumns = extractSelectedColumns(selectSql, fromColumns);
            
            if (selectedColumns.isEmpty()) {
                LOG.debug("No columns selected, using all columns");
                return context;
            }

            context.setProjectedColumns(selectedColumns);

            // Estimate compression ratio
            double compressionRatio = (double) selectedColumns.size() / fromColumns.size();
            long estimatedRowsBefore = 100_000_000_000L; // 100B rows (PB-scale)
            long estimatedRowsAfter = (long) (estimatedRowsBefore * compressionRatio);

            context.setEstimatedRowsBefore(estimatedRowsBefore);
            context.setEstimatedRowsAfter(estimatedRowsAfter);

            LOG.info("Column pruning optimized: selected {}/{} columns (compression: {:.2f}x)",
                    selectedColumns.size(), fromColumns.size(),
                    1.0 / compressionRatio);

            return context;
        } catch (Exception e) {
            LOG.warn("Failed to optimize column pruning: {}", selectSql, e);
            // Return all columns on error
            context.setProjectedColumns(new HashSet<>(fromColumns));
            return context;
        }
    }

    /**
     * Extracts column names from a SELECT clause.
     * Handles: *, column_name, table.column_name, aliased columns
     */
    private Set<String> extractSelectedColumns(String selectSql, List<String> availableColumns) {
        Set<String> columns = new HashSet<>();
        
        // Normalize SQL
        selectSql = selectSql.trim();
        if (selectSql.toUpperCase().startsWith("SELECT")) {
            selectSql = selectSql.substring(6).trim();
        }

        // Handle SELECT *
        if (selectSql.trim().equals("*")) {
            columns.addAll(availableColumns);
            return columns;
        }

        // Split by comma to handle multiple columns
        String[] parts = selectSql.split(",");
        
        for (String part : parts) {
            String columnExpr = part.trim();
            
            // Handle aliases (column AS alias, column alias)
            String[] tokens = columnExpr.split("(?i)\\s+as\\s+|\\s+(?!where|from|order|group)");
            String columnName = tokens[0].trim();

            // Remove table prefix (table.column -> column)
            if (columnName.contains(".")) {
                columnName = columnName.substring(columnName.lastIndexOf(".") + 1);
            }

            // Remove quotes if present
            columnName = columnName.replaceAll("['\"`]", "");

            // Check if column is in available columns
            if (isValidColumn(columnName, availableColumns)) {
                columns.add(columnName);
            } else {
                // It might be a function or expression, try to extract column references
                extractColumnsFromExpression(columnName, availableColumns, columns);
            }
        }

        // If no columns found, use all available
        if (columns.isEmpty()) {
            LOG.warn("No valid columns found in SELECT clause, using all columns");
            columns.addAll(availableColumns);
        }

        return columns;
    }

    /**
     * Extracts column references from function calls or complex expressions.
     * Example: SUBSTRING(col1, 1, 10) -> col1
     */
    private void extractColumnsFromExpression(String expression, List<String> availableColumns, Set<String> result) {
        // Simple regex to find identifiers
        Pattern pattern = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b");
        Matcher matcher = pattern.matcher(expression);

        while (matcher.find()) {
            String token = matcher.group(1);
            
            // Skip SQL keywords and common functions
            if (!isSqlKeywordOrFunction(token) && isValidColumn(token, availableColumns)) {
                result.add(token);
            }
        }
    }

    /**
     * Determines if a token is a SQL keyword or built-in function.
     */
    private boolean isSqlKeywordOrFunction(String token) {
        String upper = token.toUpperCase();
        
        // SQL keywords
        if (upper.matches("(?:SELECT|FROM|WHERE|AND|OR|NOT|AS|ON|JOIN|LEFT|RIGHT|INNER|OUTER|" +
                "GROUP|ORDER|BY|HAVING|LIMIT|OFFSET|UNION|CASE|WHEN|THEN|ELSE|END|NULL|" +
                "TRUE|FALSE|IN|EXISTS|BETWEEN|LIKE)")) {
            return true;
        }

        // Common functions
        if (upper.matches("(?:COUNT|SUM|AVG|MIN|MAX|LENGTH|SUBSTRING|UPPER|LOWER|CAST|" +
                "ROUND|CEIL|FLOOR|ABS|CONCAT|COALESCE|TRIM|LTRIM|RTRIM)")) {
            return true;
        }

        return false;
    }

    /**
     * Checks if a column name exists in available columns (case-insensitive).
     */
    private boolean isValidColumn(String columnName, List<String> availableColumns) {
        if (availableColumns == null || availableColumns.isEmpty()) {
            return false;
        }

        String lowerName = columnName.toLowerCase();
        for (String col : availableColumns) {
            if (col.toLowerCase().equals(lowerName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Merges columns from multiple sources (SELECT, WHERE, ORDER BY, GROUP BY).
     */
    public Set<String> mergeProjectedColumns(List<Set<String>> columnSets) {
        Set<String> merged = new HashSet<>();
        
        for (Set<String> columns : columnSets) {
            if (columns != null) {
                merged.addAll(columns);
            }
        }

        return merged;
    }

    /**
     * Extracts columns referenced in WHERE clause.
     */
    public Set<String> extractColumnsFromWhere(String whereSql, List<String> availableColumns) {
        Set<String> columns = new HashSet<>();
        
        if (whereSql == null || whereSql.isEmpty()) {
            return columns;
        }

        Pattern pattern = Pattern.compile("\\b([a-zA-Z_][a-zA-Z0-9_]*)\\b");
        Matcher matcher = pattern.matcher(whereSql);

        while (matcher.find()) {
            String token = matcher.group(1);
            
            if (!isSqlKeywordOrFunction(token) && isValidColumn(token, availableColumns)) {
                columns.add(token);
            }
        }

        return columns;
    }

    /**
     * Extracts columns referenced in ORDER BY clause.
     */
    public Set<String> extractColumnsFromOrderBy(String orderBySql, List<String> availableColumns) {
        Set<String> columns = new HashSet<>();
        
        if (orderBySql == null || orderBySql.isEmpty()) {
            return columns;
        }

        // Remove ORDER BY keyword if present
        String sql = orderBySql.trim();
        if (sql.toUpperCase().startsWith("ORDER BY")) {
            sql = sql.substring(8).trim();
        }

        // Split by comma for multiple ORDER BY columns
        String[] parts = sql.split(",");
        
        for (String part : parts) {
            String columnExpr = part.trim();
            
            // Remove ASC/DESC
            columnExpr = columnExpr.replaceAll("(?i)\\s+(asc|desc)$", "");
            
            // Remove table prefix
            if (columnExpr.contains(".")) {
                columnExpr = columnExpr.substring(columnExpr.lastIndexOf(".") + 1);
            }

            if (isValidColumn(columnExpr, availableColumns)) {
                columns.add(columnExpr);
            }
        }

        return columns;
    }

    /**
     * Extracts columns referenced in GROUP BY clause.
     */
    public Set<String> extractColumnsFromGroupBy(String groupBySql, List<String> availableColumns) {
        Set<String> columns = new HashSet<>();
        
        if (groupBySql == null || groupBySql.isEmpty()) {
            return columns;
        }

        // Remove GROUP BY keyword if present
        String sql = groupBySql.trim();
        if (sql.toUpperCase().startsWith("GROUP BY")) {
            sql = sql.substring(8).trim();
        }

        // Split by comma
        String[] parts = sql.split(",");
        
        for (String part : parts) {
            String columnExpr = part.trim();
            
            // Remove table prefix
            if (columnExpr.contains(".")) {
                columnExpr = columnExpr.substring(columnExpr.lastIndexOf(".") + 1);
            }

            if (isValidColumn(columnExpr, availableColumns)) {
                columns.add(columnExpr);
            }
        }

        return columns;
    }

    /**
     * Validates column list against available columns.
     */
    public boolean validateColumns(Set<String> columns, List<String> availableColumns) {
        if (columns == null || columns.isEmpty()) {
            return true; // Empty projection is valid (means all columns)
        }

        for (String col : columns) {
            if (!isValidColumn(col, availableColumns)) {
                LOG.warn("Column '{}' not found in available columns", col);
                return false;
            }
        }

        return true;
    }

    /**
     * Gets the estimated I/O reduction factor based on column count.
     * Formula: (total_columns) / (selected_columns)
     */
    public double getIoReductionFactor(int totalColumns, int selectedColumns) {
        if (selectedColumns == 0 || totalColumns == 0) {
            return 1.0;
        }
        return (double) totalColumns / selectedColumns;
    }
}
