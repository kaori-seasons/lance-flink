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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Mock implementation of LanceTableReader for development and testing.
 * Used when Lance SDK is not available in the classpath.
 * 
 * For production use, deploy LanceSDKTableReader with Lance Java SDK dependency.
 */
public class MockLanceTableReader implements LanceTableReader {
    private static final Logger LOG = LoggerFactory.getLogger(MockLanceTableReader.class);

    private final String tableUri;
    private final long rowCount;
    private final List<Integer> fragmentIds;
    private final byte[] schemaBytes;
    private final long version;
    private final int batchSize;
    private boolean closed = false;

    public MockLanceTableReader(
            String tableUri,
            long rowCount,
            List<Integer> fragmentIds,
            byte[] schemaBytes,
            long version,
            int batchSize) {
        this.tableUri = tableUri;
        this.rowCount = rowCount;
        this.fragmentIds = new ArrayList<>(fragmentIds);
        this.schemaBytes = schemaBytes;
        this.version = version;
        this.batchSize = batchSize;
    }

    @Override
    public RecordBatchIterator readBatches(
            Optional<String> whereClause,
            Optional<List<String>> columns,
            Optional<Long> limit) throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }

        LOG.debug("Reading batches (mock) from {}: where={}, columns={}, limit={}",
                tableUri, whereClause.orElse("none"), columns.map(List::size).orElse(-1), limit.orElse(-1L));

        long effectiveLimit = limit.orElse(rowCount);
        long effectiveRowCount = Math.min(rowCount, effectiveLimit);

        return new MockRecordBatchIterator(
                tableUri,
                (int) effectiveRowCount,
                batchSize,
                schemaBytes,
                whereClause,
                columns);
    }

    @Override
    public byte[] getSchema() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
        return schemaBytes;
    }

    @Override
    public long getRowCount() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
        return rowCount;
    }

    @Override
    public List<Integer> listFragments() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
        return new ArrayList<>(fragmentIds);
    }

    @Override
    public long getVersion() throws LanceException {
        if (closed) {
            throw new IllegalStateException("Reader is closed");
        }
        return version;
    }

    @Override
    public void close() throws java.io.IOException {
        LOG.debug("Closing MockLanceTableReader for {}", tableUri);
        closed = true;
    }

    /**
     * Mock implementation of RecordBatchIterator.
     */
    private static class MockRecordBatchIterator implements RecordBatchIterator {
        private final String tableUri;
        private final int totalRows;
        private final int batchSize;
        private final byte[] schemaBytes;
        private final Optional<String> whereClause;
        private final Optional<List<String>> columns;
        
        private int currentBatchIndex = 0;
        private int rowsRead = 0;
        private boolean closed = false;

        MockRecordBatchIterator(
                String tableUri,
                int totalRows,
                int batchSize,
                byte[] schemaBytes,
                Optional<String> whereClause,
                Optional<List<String>> columns) {
            this.tableUri = tableUri;
            this.totalRows = totalRows;
            this.batchSize = batchSize;
            this.schemaBytes = schemaBytes;
            this.whereClause = whereClause;
            this.columns = columns;
        }

        @Override
        public boolean hasNext() {
            return rowsRead < totalRows && !closed;
        }

        @Override
        public ArrowBatch next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException("No more batches");
            }

            int remainingRows = totalRows - rowsRead;
            int currentBatchRowCount = Math.min(batchSize, remainingRows);

            // Create mock Arrow batch
            byte[] batchData = new byte[currentBatchRowCount * 100]; // Simulated data
            String[] columnNames = columns
                    .map(cols -> cols.toArray(new String[0]))
                    .orElse(new String[]{"col1", "col2", "col3"});
            int[] columnTypes = new int[columnNames.length];
            
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = 7; // VARCHAR type
            }

            rowsRead += currentBatchRowCount;
            currentBatchIndex++;

            LOG.debug("Created mock batch {} with {} rows for {}", 
                    currentBatchIndex, currentBatchRowCount, tableUri);

            return new ArrowBatch(batchData, columnNames, columnTypes, currentBatchRowCount);
        }

        @Override
        public long getTotalRowCount() {
            return totalRows;
        }

        @Override
        public int getCurrentBatchIndex() {
            return currentBatchIndex;
        }

        @Override
        public void close() throws java.io.IOException {
            LOG.debug("Closing MockRecordBatchIterator for {}", tableUri);
            closed = true;
        }
    }
}
