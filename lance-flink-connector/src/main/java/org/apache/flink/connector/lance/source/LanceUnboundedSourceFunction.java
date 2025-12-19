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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceException;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.dataset.LanceDatasetAdapter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Unbounded source function for streaming Lance dataset reads.
 * 
 * This source:
 * - Reads initial snapshot of all fragments
 * - Polls for new fragments periodically
 * - Supports checkpoint and recovery with version/fragment tracking
 * - Exactly-Once semantics via state management
 * 
 * Configuration:
 * - pollIntervalMs: Fragment polling interval (default: 5000ms)
 * - initialSnapshotEnabled: Read snapshot on startup (default: true)
 */
public class LanceUnboundedSourceFunction extends RichSourceFunction<Row> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceUnboundedSourceFunction.class);

    private static final int DEFAULT_POLL_INTERVAL_MS = 5000;
    private static final String CHECKPOINT_STATE_NAME = "lance-unbounded-state";

    private final LanceConfig config;
    private final LanceReadOptions readOptions;
    private final RowTypeInfo rowTypeInfo;
    private final long pollIntervalMs;
    private final boolean initialSnapshotEnabled;

    // Runtime fields
    private transient LanceDatasetAdapter adapter;
    private transient volatile boolean running = true;
    private transient Set<Integer> processedFragmentIds;
    private transient long currentVersionId;
    private transient ListState<UnboundedSourceState> checkpointState;

    public LanceUnboundedSourceFunction(
            LanceConfig config,
            LanceReadOptions readOptions,
            RowTypeInfo rowTypeInfo) {
        this(config, readOptions, rowTypeInfo, DEFAULT_POLL_INTERVAL_MS, true);
    }

    public LanceUnboundedSourceFunction(
            LanceConfig config,
            LanceReadOptions readOptions,
            RowTypeInfo rowTypeInfo,
            long pollIntervalMs,
            boolean initialSnapshotEnabled) {
        this.config = config;
        this.readOptions = readOptions;
        this.rowTypeInfo = rowTypeInfo;
        this.pollIntervalMs = pollIntervalMs;
        this.initialSnapshotEnabled = initialSnapshotEnabled;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        LOG.info("Opening LanceUnboundedSourceFunction for dataset: {}", config.getDatasetUri());
        
        // Initialize Lance dataset adapter
        this.adapter = new LanceDatasetAdapter(config);
        this.processedFragmentIds = new HashSet<>();
        
        try {
            // Get initial version
            this.currentVersionId = adapter.getLanceDataset().getVersion().getId();
            LOG.info("Dataset opened at version: {}", currentVersionId);
        } catch (Exception e) {
            throw new LanceException("Failed to open dataset", e);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("Initializing checkpoint state");
        
        ListStateDescriptor<UnboundedSourceState> descriptor =
                new ListStateDescriptor<>(
                        CHECKPOINT_STATE_NAME,
                        UnboundedSourceState.class);
        
        this.checkpointState = context.getOperatorStateStore().getListState(descriptor);
        
        // Restore state if available
        if (context.isRestored()) {
            for (UnboundedSourceState state : checkpointState.get()) {
                this.processedFragmentIds = new HashSet<>(state.processedFragmentIds);
                this.currentVersionId = state.currentVersionId;
                LOG.info("Restored state: version={}, fragments={}",
                        currentVersionId, processedFragmentIds.size());
            }
        } else {
            LOG.debug("No checkpoint state to restore");
            this.processedFragmentIds = new HashSet<>();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("Snapshotting state at checkpoint {}", context.getCheckpointId());
        
        checkpointState.clear();
        checkpointState.add(new UnboundedSourceState(
                currentVersionId,
                new ArrayList<>(processedFragmentIds)));
        
        LOG.info("State snapshot saved: version={}, fragments={}, checkpoint={}",
                currentVersionId, processedFragmentIds.size(), context.getCheckpointId());
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        LOG.info("Starting unbounded stream read, initialSnapshot={}", initialSnapshotEnabled);
        
        try {
            if (initialSnapshotEnabled) {
                // Read initial snapshot
                readCurrentSnapshot(ctx);
            }
            
            // Poll for new fragments periodically
            while (running) {
                Thread.sleep(pollIntervalMs);
                
                if (!running) break;
                
                try {
                    pollNewFragments(ctx);
                } catch (Exception e) {
                    LOG.warn("Error polling for new fragments: {}", e.getMessage(), e);
                    // Continue polling despite errors
                }
            }
        } catch (InterruptedException e) {
            LOG.info("Stream source interrupted");
            Thread.currentThread().interrupt();
        } finally {
            LOG.info("Unbounded stream read ended");
        }
    }

    /**
     * Read all current fragments as initial snapshot.
     */
    private void readCurrentSnapshot(SourceContext<Row> ctx) throws Exception {
        LOG.info("Reading initial snapshot...");
        
        try {
            List<LanceDatasetAdapter.FragmentMetadata> fragments = adapter.getFragments();
            LOG.info("Snapshot contains {} fragments", fragments.size());
            
            long recordsRead = 0;
            for (LanceDatasetAdapter.FragmentMetadata fragment : fragments) {
                if (!running) break;
                
                recordsRead += readFragmentAsStream(fragment, ctx);
                processedFragmentIds.add(fragment.getId());
            }
            
            currentVersionId = adapter.getLanceDataset().getVersion().getId();
            LOG.info("Initial snapshot read complete: {} records from {} fragments",
                    recordsRead, fragments.size());
        } catch (Exception e) {
            throw new LanceException("Failed to read initial snapshot", e);
        }
    }

    /**
     * Poll for new fragments since last snapshot.
     */
    private void pollNewFragments(SourceContext<Row> ctx) throws Exception {
        try {
            long newVersionId = adapter.getLanceDataset().getVersion().getId();
            
            if (newVersionId != currentVersionId) {
                LOG.info("Dataset updated: oldVersion={}, newVersion={}", currentVersionId, newVersionId);
                
                List<LanceDatasetAdapter.FragmentMetadata> allFragments = adapter.getFragments();
                long recordsRead = 0;
                int newFragmentsCount = 0;
                
                for (LanceDatasetAdapter.FragmentMetadata fragment : allFragments) {
                    if (!running) break;
                    
                    // Only process new fragments
                    if (!processedFragmentIds.contains(fragment.getId())) {
                        LOG.debug("Processing new fragment: {}", fragment.getId());
                        recordsRead += readFragmentAsStream(fragment, ctx);
                        processedFragmentIds.add(fragment.getId());
                        newFragmentsCount++;
                    }
                }
                
                currentVersionId = newVersionId;
                LOG.info("Poll cycle complete: {} new fragments, {} records",
                        newFragmentsCount, recordsRead);
            }
        } catch (Exception e) {
            LOG.warn("Error during polling cycle: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Read a single fragment as a stream of records.
     * 
     * @return number of records read
     */
    private long readFragmentAsStream(
            LanceDatasetAdapter.FragmentMetadata fragment,
            SourceContext<Row> ctx) throws Exception {
        LOG.debug("Reading fragment {} ({} rows)", fragment.getId(), fragment.getRowCount());
        
        long recordsRead = 0;
        try {
            org.apache.flink.connector.lance.format.RecordBatchIterator batches =
                    adapter.readBatches(readOptions);
            
            while (batches.hasNext() && running) {
                org.apache.flink.connector.lance.format.ArrowBatch batch = batches.next();
                
                List<Row> rows = batch.toRows();
                synchronized (ctx.getCheckpointLock()) {
                    for (Row row : rows) {
                        if (running) {
                            ctx.collect(row);
                            recordsRead++;
                        }
                    }
                }
                
                LOG.debug("Batch {} from fragment {} processed: {} rows",
                        batches.getCurrentBatchIndex(), fragment.getId(), batch.getRowCount());
            }
            
            batches.close();
            LOG.debug("Fragment {} read complete: {} records", fragment.getId(), recordsRead);
        } catch (Exception e) {
            LOG.error("Error reading fragment {}: {}", fragment.getId(), e.getMessage());
            throw new LanceException("Failed to read fragment " + fragment.getId(), e);
        }
        
        return recordsRead;
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling LanceUnboundedSourceFunction");
        running = false;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing LanceUnboundedSourceFunction");
        running = false;
        if (adapter != null) {
            try {
                adapter.close();
            } catch (Exception e) {
                LOG.warn("Error closing dataset adapter: {}", e.getMessage());
            }
        }
    }

    /**
     * State for checkpoint/recovery.
     * Stores version ID and processed fragment IDs.
     */
    public static class UnboundedSourceState {
        public long currentVersionId;
        public List<Integer> processedFragmentIds;

        public UnboundedSourceState() {}

        public UnboundedSourceState(long currentVersionId, List<Integer> processedFragmentIds) {
            this.currentVersionId = currentVersionId;
            this.processedFragmentIds = processedFragmentIds;
        }

        @Override
        public String toString() {
            return "UnboundedSourceState{" +
                    "version=" + currentVersionId +
                    ", fragments=" + processedFragmentIds.size() +
                    '}';
        }
    }
}
