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

import java.io.Closeable;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.lance.common.LanceException;

/**
 * Interface for Lance table write operations.
 * 
 * Provides abstraction for writing data to Lance datasets in different modes:
 * - APPEND: Add new data to existing dataset
 * - UPSERT: Update or insert based on primary key
 * - OVERWRITE: Replace entire dataset
 */
public interface LanceTableWriter extends Closeable {
    
    /**
     * Writes Arrow data to Lance dataset in APPEND mode.
     * 
     * @param data VectorSchemaRoot containing data to write
     * @throws LanceException if write operation fails
     */
    void append(VectorSchemaRoot data) throws LanceException;
    
    /**
     * Upserts Arrow data to Lance dataset.
     * Updates existing rows based on primary key, inserts new ones.
     * 
     * @param data VectorSchemaRoot containing data to upsert
     * @param primaryKeyColumn Name of the primary key column
     * @throws LanceException if upsert operation fails
     */
    void upsert(VectorSchemaRoot data, String primaryKeyColumn) throws LanceException;
    
    /**
     * Overwrites entire Lance dataset with new data.
     * Deletes all existing fragments and writes new ones.
     * 
     * @param data VectorSchemaRoot containing new data
     * @throws LanceException if overwrite operation fails
     */
    void overwrite(VectorSchemaRoot data) throws LanceException;
    
    /**
     * Flushes any pending writes to storage.
     * 
     * @throws LanceException if flush operation fails
     */
    void flush() throws LanceException;
    
    /**
     * Gets write statistics.
     * 
     * @return WriteStatistics containing metrics about write operations
     */
    WriteStatistics getStatistics();
    
    /**
     * Write operation statistics.
     */
    class WriteStatistics {
        public long rowsWritten;
        public long bytesWritten;
        public int fragmentsCreated;
        public long durationMillis;
        
        public WriteStatistics(long rowsWritten, long bytesWritten, 
                              int fragmentsCreated, long durationMillis) {
            this.rowsWritten = rowsWritten;
            this.bytesWritten = bytesWritten;
            this.fragmentsCreated = fragmentsCreated;
            this.durationMillis = durationMillis;
        }
        
        @Override
        public String toString() {
            return "WriteStatistics{" +
                    "rowsWritten=" + rowsWritten +
                    ", bytesWritten=" + bytesWritten +
                    ", fragmentsCreated=" + fragmentsCreated +
                    ", durationMillis=" + durationMillis +
                    '}';
        }
    }
}
