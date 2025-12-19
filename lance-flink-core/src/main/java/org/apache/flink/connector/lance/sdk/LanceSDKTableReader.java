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

package org.apache.flink.connector.lance.sdk;

import org.apache.flink.connector.lance.common.LanceException;
import org.apache.flink.connector.lance.format.ArrowBatch;
import org.apache.flink.connector.lance.format.RecordBatchIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Production-ready implementation of LanceTableReader using Lance Java SDK with direct API calls.
 * 
 * This implementation provides direct access to Lance datasets with support for:
 * - Predicate pushdown (WHERE clause filtering at storage level)
 * - Column pruning (selective column reading)
 * - Result limiting
 * - Fragment management
 * 
 * Uses direct API calls to Lance Java SDK (org.lance:lance-core:2.0.0-beta.3)
 * NO reflection - compiled-time type safety via JNI bindings.
 */
public class LanceSDKTableReader implements LanceTableReader {
    private static final Logger LOG = LoggerFactory.getLogger(LanceSDKTableReader.class);

    private final Dataset lanceDataset;  // Direct API reference
    private final BufferAllocator allocator;  // Direct API reference
    private boolean closed = false;

    /**
     * Creates a LanceSDKTableReader from a Lance dataset.
     *
     * @param lanceDataset the underlying Lance dataset from Lance SDK
     * @param allocator buffer allocator for Arrow operations
     */
    public LanceSDKTableReader(Dataset lanceDataset, BufferAllocator allocator) {
        this.lanceDataset = lanceDataset;
        this.allocator = allocator;
    }

    /**
     * Creates a LanceSDKTableReader by opening a Lance dataset from the specified URI.
     * Uses direct Lance Java SDK API calls (no reflection).
     *
     * @param datasetUri URI of the Lance dataset (local path or cloud storage)
     * @return LanceSDKTableReader instance
     * @throws LanceException if dataset cannot be opened
     */
    public static LanceSDKTableReader open(String datasetUri) throws LanceException {
        try {
            // Direct API call - create allocator
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            
            // Direct API call - open dataset
            Dataset dataset = Dataset.open(datasetUri);
            
            LOG.info("Successfully opened Lance dataset from: {}", datasetUri);
            return new LanceSDKTableReader(dataset, allocator);
        } catch (Exception e) {
            throw new LanceException("Failed to open Lance dataset: " + datasetUri, e);
        }
    }

    @Override
    public RecordBatchIterator readBatches(
            Optional<String> whereClause,
            Optional<List<String>> columns,
            Optional<Long> limit) throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        try {
            LOG.debug("Reading batches from Lance SDK: where={}, columns={}, limit={}",
                    whereClause.orElse("none"), columns.map(List::size).orElse(-1), limit.orElse(-1L));

            // Direct API call - create ScanOptions builder
            ScanOptions.Builder scanBuilder = new ScanOptions.Builder();
            
            // Add filter (WHERE clause) - direct API
            if (whereClause.isPresent()) {
                scanBuilder.filter(whereClause.get());
                LOG.info("Applied predicate pushdown: {}", whereClause.get());
            }

            // Add column selection - direct API
            if (columns.isPresent() && !columns.get().isEmpty()) {
                scanBuilder.columns(columns.get());
                LOG.info("Selected {} columns", columns.get().size());
            }

            // Add limit - direct API
            if (limit.isPresent()) {
                scanBuilder.limit(limit.get());
                LOG.info("Applied limit: {}", limit.get());
            }

            // Build scan options - direct API
            ScanOptions scanOptions = scanBuilder.build();

            // Create scanner - direct API call
            LanceScanner scanner = LanceScanner.create(lanceDataset, scanOptions, allocator);
            
            // Get arrow reader from scanner - direct API call (returns ArrowReader)
            ArrowReader arrowReader = scanner.scanBatches();

            return new SDKRecordBatchIterator(arrowReader, allocator, scanner);
        } catch (Exception e) {
            throw new LanceException("Failed to read batches from Lance dataset", e);
        }
    }

    @Override
    public byte[] getSchema() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        try {
            // Direct API call
            Schema schema = lanceDataset.getSchema();
            return serializeSchema(schema);
        } catch (Exception e) {
            throw new LanceException("Failed to get schema from Lance dataset", e);
        }
    }

    @Override
    public long getRowCount() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        try {
            // Direct API call
            return lanceDataset.countRows();
        } catch (Exception e) {
            throw new LanceException("Failed to get row count from Lance dataset", e);
        }
    }

    @Override
    public List<Integer> listFragments() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        try {
            // Direct API call
            java.util.List<Fragment> fragments = lanceDataset.getFragments();
            
            List<Integer> fragmentIds = new ArrayList<>();
            for (Fragment fragment : fragments) {
                // Direct API call
                fragmentIds.add(fragment.getId());
            }
            LOG.info("Listed {} fragments", fragmentIds.size());
            return fragmentIds;
        } catch (Exception e) {
            throw new LanceException("Failed to list fragments from Lance dataset", e);
        }
    }

    @Override
    public long getVersion() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        try {
            // Direct API call - Dataset.getVersion() returns Version object
            return lanceDataset.getVersion().getId();
        } catch (Exception e) {
            throw new LanceException("Failed to get dataset version", e);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing LanceSDKTableReader");
        if (!closed && lanceDataset != null) {
            try {
                // Direct API call
                lanceDataset.close();
            } catch (Exception e) {
                LOG.warn("Error closing Lance dataset", e);
            }
        }
        closed = true;
    }

    /**
     * Serializes Arrow schema to bytes using direct API calls.
     */
    private byte[] serializeSchema(Schema schema) {
        try {
            // Direct API call - get fields from schema
            List<org.apache.arrow.vector.types.pojo.Field> fields = schema.getFields();
            
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4096);
            buffer.putInt(fields.size());

            for (org.apache.arrow.vector.types.pojo.Field field : fields) {
                // Direct API call - get field name
                String name = field.getName();
                byte[] nameBytes = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buffer.putInt(nameBytes.length);
                buffer.put(nameBytes);
                buffer.put((byte) 7); // VARCHAR
            }

            byte[] result = new byte[buffer.position()];
            buffer.flip();
            buffer.get(result);
            return result;
        } catch (Exception e) {
            LOG.warn("Failed to serialize schema, returning empty bytes", e);
            return new byte[0];
        }
    }

    /**
     * Implementation of RecordBatchIterator using Lance Scanner's ArrowReader.
     * Uses direct API calls with compiled-time type safety.
     */
    private static class SDKRecordBatchIterator implements RecordBatchIterator {
        private final ArrowReader arrowReader;
        private final BufferAllocator allocator;
        private final LanceScanner scanner;
        private int currentBatchIndex = 0;
        private long totalRowCount = 0;
        private VectorSchemaRoot currentRoot = null;
        private boolean closed = false;

        SDKRecordBatchIterator(ArrowReader arrowReader, BufferAllocator allocator, LanceScanner scanner) throws Exception {
            this.arrowReader = arrowReader;
            this.allocator = allocator;
            this.scanner = scanner;
            
            // Load first batch via direct API
            if (arrowReader.loadNextBatch()) {
                this.currentRoot = arrowReader.getVectorSchemaRoot();
                this.totalRowCount = currentRoot.getRowCount();
            }
        }

        @Override
        public boolean hasNext() {
            return currentRoot != null && !closed;
        }

        @Override
        public ArrowBatch next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException("No more batches");
            }

            try {
                // Direct API calls - no reflection
                VectorSchemaRoot root = currentRoot;
                Schema schema = root.getSchema();
                List<org.apache.arrow.vector.types.pojo.Field> fields = schema.getFields();
                
                int rowCount = root.getRowCount();

                String[] columnNames = new String[fields.size()];
                int[] columnTypes = new int[fields.size()];
                
                for (int i = 0; i < fields.size(); i++) {
                    columnNames[i] = fields.get(i).getName();
                    columnTypes[i] = 7; // VARCHAR
                }

                byte[] batchData = serializeBatch(root);

                // Load next batch - direct API
                currentBatchIndex++;
                if (arrowReader.loadNextBatch()) {
                    currentRoot = arrowReader.getVectorSchemaRoot();
                } else {
                    currentRoot = null;
                }

                return new ArrowBatch(batchData, columnNames, columnTypes, rowCount);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read batch from Lance", e);
            }
        }

        @Override
        public long getTotalRowCount() {
            return totalRowCount;
        }

        @Override
        public int getCurrentBatchIndex() {
            return currentBatchIndex;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                try {
                    // Direct API calls - close resources
                    if (arrowReader != null) {
                        arrowReader.close();
                    }
                    if (scanner != null) {
                        scanner.close();
                    }
                } catch (Exception e) {
                    throw new IOException("Failed to close resources", e);
                }
            }
            closed = true;
        }

        private byte[] serializeBatch(VectorSchemaRoot root) throws Exception {
            try (
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                org.apache.arrow.vector.ipc.ArrowStreamWriter writer = 
                    new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, baos)
            ) {
                // Direct API call - write batch
                writer.writeBatch();
                return baos.toByteArray();
            }
        }
    }
}
