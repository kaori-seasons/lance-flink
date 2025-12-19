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

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Abstraction of Lance table reader for batch operations.
 * Provides interface for reading data with optional filtering and column selection.
 */
public interface LanceTableReader extends Closeable {
    
    /**
     * Read batches with optional where clause and column selection.
     * 
     * @param whereClause optional SQL where clause for filtering
     * @param columns optional list of columns to read (null means all columns)
     * @param limit optional maximum number of rows to read
     * @return iterator of Arrow batches
     * @throws LanceException if read operation fails
     */
    RecordBatchIterator readBatches(
            Optional<String> whereClause,
            Optional<List<String>> columns,
            Optional<Long> limit) throws LanceException;
    
    /**
     * Get schema of the table as Arrow schema bytes.
     * 
     * @return Arrow schema in bytes
     * @throws LanceException if schema retrieval fails
     */
    byte[] getSchema() throws LanceException;
    
    /**
     * Get number of rows in the table.
     * 
     * @return total row count
     * @throws LanceException if count retrieval fails
     */
    long getRowCount() throws LanceException;
    
    /**
     * Get list of fragment IDs.
     * 
     * @return list of fragment IDs
     * @throws LanceException if fragment list retrieval fails
     */
    List<Integer> listFragments() throws LanceException;
    
    /**
     * Get current version of the table.
     * 
     * @return version number
     * @throws LanceException if version retrieval fails
     */
    long getVersion() throws LanceException;
}
