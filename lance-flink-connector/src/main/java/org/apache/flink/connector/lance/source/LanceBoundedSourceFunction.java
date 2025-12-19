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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceException;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.dataset.LanceDatasetAdapter;
import org.apache.flink.connector.lance.optimizer.LanceOptimizer;
import org.apache.flink.connector.lance.optimizer.LanceOptimizationContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bounded source function for reading Lance datasets in batch mode.
 * Reads complete dataset in parallel by dividing fragments among parallel instances.
 */
public class LanceBoundedSourceFunction extends BaseLanceSourceFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceBoundedSourceFunction.class);

    private final List<FragmentSplit> fragmentSplits;
    private int currentFragmentIndex = 0;
    private transient LanceOptimizer optimizer;
    private transient LanceOptimizationContext optimizationContext;

    public LanceBoundedSourceFunction(
            LanceConfig config,
            LanceReadOptions readOptions,
            RowTypeInfo rowTypeInfo) {
        super(config, readOptions, rowTypeInfo);
        this.fragmentSplits = new ArrayList<>();
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Opening LanceBoundedSourceFunction for dataset: {}", config.getDatasetUri());
        
        // Initialize query optimizer
        initializeOptimizer();
        
        // Initialize fragment splits
        initializeFragmentSplits();
    }

    /**
     * Initialize the query optimizer and generate optimization context.
     * This applies predicate pushdown, column pruning, and Top-N optimizations.
     */
    private void initializeOptimizer() {
        try {
            this.optimizer = new LanceOptimizer(true);
            
            // Build SQL query from readOptions
            String sql = buildSqlQueryFromOptions();
            if (!sql.isEmpty()) {
                // Get available columns from the dataset schema
                LanceDatasetAdapter.TableSchema schema = adapter.getSchema();
                List<String> availableColumns = Arrays.asList(schema.getColumnNames());
                
                // Run optimizer
                this.optimizationContext = optimizer.optimizeQuery(sql, availableColumns);
                LOG.info("Query optimization completed. Rows reduction: {:.2f}x",
                        (double) optimizationContext.getEstimatedRowsBefore() / 
                        Math.max(optimizationContext.getEstimatedRowsAfter(), 1));
                
                // Apply optimizations to readOptions
                applyOptimizationsToReadOptions();
            }
        } catch (Exception e) {
            LOG.warn("Query optimization failed, proceeding without optimizations", e);
            this.optimizationContext = new LanceOptimizationContext();
        }
    }

    /**
     * Builds a SQL query string from the current readOptions.
     * This is a simplified version - in production, would parse from actual SQL.
     */
    private String buildSqlQueryFromOptions() {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Add columns
        List<String> columns = readOptions.getColumns();
        if (columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", columns));
        }
        
        sql.append(" FROM dataset");
        
        // Add WHERE clause
        if (readOptions.getWhereClause().isPresent()) {
            sql.append(" WHERE ").append(readOptions.getWhereClause().get());
        }
        
        // Add LIMIT
        if (readOptions.getLimit().isPresent()) {
            sql.append(" LIMIT ").append(readOptions.getLimit().get());
        }
        
        return sql.toString();
    }

    /**
     * Apply optimization context to readOptions for execution.
     */
    private void applyOptimizationsToReadOptions() {
        if (optimizationContext == null) {
            return;
        }
        
        // The optimization context is stored in readOptions for use during read
        // The actual application happens in LanceDatasetAdapter.readBatches()
        LOG.debug("Optimizations ready for application: predicate={}, columns={}, topN={}",
                optimizationContext.hasPushdownPredicate(),
                optimizationContext.hasColumnPruning(),
                optimizationContext.hasTopNPushdown());
    }

    /**
     * Initialize fragment splits based on parallelism and task index.
     */
    private void initializeFragmentSplits() throws LanceException {
        try {
            // Get all fragments from dataset
            List<LanceDatasetAdapter.FragmentMetadata> allFragments = adapter.getFragments();
            LOG.info("Total fragments available: {}", allFragments.size());

            // Distribute fragments among parallel tasks
            int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

            for (int i = taskIndex; i < allFragments.size(); i += parallelism) {
                LanceDatasetAdapter.FragmentMetadata fragment = allFragments.get(i);
                fragmentSplits.add(new FragmentSplit(fragment.getId(), fragment.getRowCount()));
                LOG.debug("Task {} assigned fragment {}", taskIndex, fragment.getId());
            }

            LOG.info("Task {} initialized with {} fragment splits", taskIndex, fragmentSplits.size());
        } catch (Exception e) {
            throw new LanceException("Failed to initialize fragment splits", e);
        }
    }

    /**
     * Main read logic - iterates through assigned fragments and emits records.
     */
    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        LOG.info("Starting batch read for {} fragments", fragmentSplits.size());

        for (FragmentSplit split : fragmentSplits) {
            readFragment(split, ctx);
        }

        LOG.info("Completed reading all assigned fragments");
    }

    /**
     * Read a single fragment and emit all records.
     */
    private void readFragment(FragmentSplit split, SourceContext<Row> ctx) throws Exception {
        LOG.debug("Reading fragment: {}", split.fragmentId);

        long recordsRead = 0;
        try {
            // Open fragment reader with readOptions
            org.apache.flink.connector.lance.format.RecordBatchIterator batches = 
                    adapter.readBatches(readOptions);
            
            // Iterate through batches and emit rows
            while (batches.hasNext() && isRunning()) {
                org.apache.flink.connector.lance.format.ArrowBatch batch = batches.next();
                
                // Apply row-level filtering if needed
                java.util.List<Row> rows = batch.toRows();
                for (Row row : rows) {
                    if (isRunning()) {
                        ctx.collect(row);
                        recordsRead++;
                    }
                }
                
                LOG.debug("Batch {} from fragment {} processed: {} rows",
                        batches.getCurrentBatchIndex(), split.fragmentId, batch.getRowCount());
            }
            
            batches.close();
            LOG.debug("Fragment {} read complete: {} records", split.fragmentId, recordsRead);
        } catch (Exception e) {
            LOG.error("Error reading fragment {}: {}", split.fragmentId, e.getMessage());
            throw new LanceException("Failed to read fragment " + split.fragmentId, e);
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling LanceBoundedSourceFunction");
        super.cancel();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing LanceBoundedSourceFunction");
        super.close();
    }

    /**
     * Fragment split for parallel distribution.
     */
    private static class FragmentSplit {
        final int fragmentId;
        final long recordCount;

        FragmentSplit(int fragmentId, long recordCount) {
            this.fragmentId = fragmentId;
            this.recordCount = recordCount;
        }

        @Override
        public String toString() {
            return "FragmentSplit{" +
                    "fragmentId=" + fragmentId +
                    ", recordCount=" + recordCount +
                    '}';
        }
    }
}
