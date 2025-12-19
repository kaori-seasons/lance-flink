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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Production-ready implementation of LanceTableReader using Lance Java SDK.
 * 
 * This implementation provides direct access to Lance datasets with support for:
 * - Predicate pushdown (WHERE clause filtering at storage level)
 * - Column pruning (selective column reading)
 * - Result limiting
 * - Fragment management
 * 
 * Requires Lance Java SDK dependency: org.lance:lance-java:0.4.0
 */
public class LanceSDKTableReader implements LanceTableReader {
    private static final Logger LOG = LoggerFactory.getLogger(LanceSDKTableReader.class);

    private final Object lanceDataset;  // org.lance.Dataset instance
    private final Object allocator;     // BufferAllocator instance
    private boolean closed = false;

    /**
     * Creates a LanceSDKTableReader from a Lance dataset.
     *
     * @param lanceDataset the underlying Lance dataset from Lance SDK
     * @param allocator buffer allocator for Arrow operations
     */
    public LanceSDKTableReader(Object lanceDataset, Object allocator) {
        this.lanceDataset = lanceDataset;
        this.allocator = allocator;
    }

    /**
     * Creates a LanceSDKTableReader by opening a Lance dataset from the specified URI.
     *
     * @param datasetUri URI of the Lance dataset (local path or cloud storage)
     * @return LanceSDKTableReader instance
     * @throws LanceException if dataset cannot be opened
     */
    public static LanceSDKTableReader open(String datasetUri) throws LanceException {
        try {
            // Load Lance SDK classes dynamically
            Class<?> rootAllocatorClass = Class.forName("org.apache.arrow.memory.RootAllocator");
            Class<?> datasetClass = Class.forName("org.lance.Dataset");
            
            // Create allocator: new RootAllocator(Long.MAX_VALUE)
            Object allocator = rootAllocatorClass.getConstructor(long.class).newInstance(Long.MAX_VALUE);
            
            // Open dataset: Dataset.open(uri)
            Object lanceDataset = datasetClass.getMethod("open", String.class)
                    .invoke(null, datasetUri);
            
            return new LanceSDKTableReader(lanceDataset, allocator);
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

            // Create scan options using reflection
            Object scanBuilder = createScanBuilder();
            
            // Add filter (WHERE clause)
            if (whereClause.isPresent()) {
                invokeMethod(scanBuilder, "setFilter", new Class[]{String.class}, new Object[]{whereClause.get()});
                LOG.debug("Applied predicate pushdown: {}", whereClause.get());
            }

            // Add column selection
            if (columns.isPresent() && !columns.get().isEmpty()) {
                invokeMethod(scanBuilder, "setColumns", new Class[]{List.class}, new Object[]{columns.get()});
                LOG.debug("Selected {} columns", columns.get().size());
            }

            // Add limit
            if (limit.isPresent()) {
                invokeMethod(scanBuilder, "setLimit", new Class[]{long.class}, new Object[]{limit.get()});
                LOG.debug("Applied limit: {}", limit.get());
            }

            // Build scan options
            Object scanOptions = invokeMethod(scanBuilder, "build", new Class[]{}, new Object[]{});

            // Create scanner
            Object scanner = invokeMethod(lanceDataset, "newScan", new Class[]{scanOptions.getClass()}, new Object[]{scanOptions});
            
            // Get Arrow reader
            Object arrowReader = invokeMethod(scanner, "scanBatches", new Class[]{}, new Object[]{});

            return new SDKRecordBatchIterator(arrowReader, allocator);
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
            Object schema = invokeMethod(lanceDataset, "getSchema", new Class[]{}, new Object[]{});
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
            return (long) invokeMethod(lanceDataset, "countRows", new Class[]{}, new Object[]{});
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
            @SuppressWarnings("unchecked")
            List<Object> fragments = (List<Object>) invokeMethod(lanceDataset, "getFragments", new Class[]{}, new Object[]{});
            
            List<Integer> fragmentIds = new ArrayList<>();
            for (Object fragment : fragments) {
                int id = (int) invokeMethod(fragment, "getId", new Class[]{}, new Object[]{});
                fragmentIds.add(id);
            }
            LOG.debug("Listed {} fragments", fragmentIds.size());
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
            return (long) invokeMethod(lanceDataset, "version", new Class[]{}, new Object[]{});
        } catch (Exception e) {
            throw new LanceException("Failed to get dataset version", e);
        }
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Closing LanceSDKTableReader");
        if (!closed && lanceDataset != null) {
            try {
                invokeMethod(lanceDataset, "close", new Class[]{}, new Object[]{});
            } catch (Exception e) {
                LOG.warn("Error closing Lance dataset", e);
            }
        }
        closed = true;
    }

    /**
     * Creates a ScanOptions.Builder instance using reflection.
     */
    private Object createScanBuilder() throws Exception {
        Class<?> builderClass = Class.forName("org.lance.ipc.ScanOptions$Builder");
        return builderClass.getConstructor().newInstance();
    }

    /**
     * Invokes a method using reflection.
     */
    private Object invokeMethod(Object obj, String methodName, Class<?>[] paramTypes, Object[] params) throws Exception {
        return obj.getClass().getMethod(methodName, paramTypes).invoke(obj, params);
    }

    /**
     * Serializes Arrow schema to bytes.
     */
    private byte[] serializeSchema(Object schema) {
        try {
            // Invoke getFields() on schema
            @SuppressWarnings("unchecked")
            List<Object> fields = (List<Object>) invokeMethod(schema, "getFields", new Class[]{}, new Object[]{});
            
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(4096);
            buffer.putInt(fields.size());

            for (Object field : fields) {
                // Get field name
                String name = (String) invokeMethod(field, "getName", new Class[]{}, new Object[]{});
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
     */
    private static class SDKRecordBatchIterator implements RecordBatchIterator {
        private final Object arrowReader;
        private final Object allocator;
        private int currentBatchIndex = 0;
        private long totalRowCount = 0;
        private boolean hasNextBatch = false;
        private boolean closed = false;

        SDKRecordBatchIterator(Object arrowReader, Object allocator) throws Exception {
            this.arrowReader = arrowReader;
            this.allocator = allocator;
            
            // Load next batch
            this.hasNextBatch = (boolean) arrowReader.getClass()
                    .getMethod("loadNextBatch").invoke(arrowReader);
            
            if (hasNextBatch) {
                Object root = arrowReader.getClass()
                        .getMethod("getVectorSchemaRoot").invoke(arrowReader);
                this.totalRowCount = (int) root.getClass()
                        .getMethod("getRowCount").invoke(root);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNextBatch && !closed;
        }

        @Override
        public ArrowBatch next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException("No more batches");
            }

            try {
                // Get VectorSchemaRoot
                Object root = arrowReader.getClass()
                        .getMethod("getVectorSchemaRoot").invoke(arrowReader);
                
                Object schema = root.getClass().getMethod("getSchema").invoke(root);
                @SuppressWarnings("unchecked")
                List<Object> fields = (List<Object>) schema.getClass()
                        .getMethod("getFields").invoke(schema);
                
                int rowCount = (int) root.getClass()
                        .getMethod("getRowCount").invoke(root);

                String[] columnNames = new String[fields.size()];
                int[] columnTypes = new int[fields.size()];
                
                for (int i = 0; i < fields.size(); i++) {
                    columnNames[i] = (String) fields.get(i).getClass()
                            .getMethod("getName").invoke(fields.get(i));
                    columnTypes[i] = 7; // VARCHAR
                }

                byte[] batchData = serializeBatch(root);

                // Load next batch
                currentBatchIndex++;
                hasNextBatch = (boolean) arrowReader.getClass()
                        .getMethod("loadNextBatch").invoke(arrowReader);

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
            if (!closed && arrowReader != null) {
                try {
                    arrowReader.getClass().getMethod("close").invoke(arrowReader);
                } catch (Exception e) {
                    throw new IOException("Failed to close Arrow reader", e);
                }
            }
            closed = true;
        }

        private byte[] serializeBatch(Object root) throws Exception {
            try (
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                java.io.OutputStream stream = baos
            ) {
                // Create ArrowStreamWriter
                Class<?> writerClass = Class.forName("org.apache.arrow.vector.ipc.ArrowStreamWriter");
                Object writer = writerClass.getConstructor(
                        root.getClass(),
                        Class.forName("org.apache.arrow.vector.dictionary.DictionaryProvider"),
                        java.io.OutputStream.class
                ).newInstance(root, null, stream);
                
                writerClass.getMethod("writeBatch").invoke(writer);
                ((java.io.Closeable) writer).close();
                
                return baos.toByteArray();
            }
        }
    }
}
