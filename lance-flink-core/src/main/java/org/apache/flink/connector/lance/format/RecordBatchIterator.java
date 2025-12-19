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

package org.apache.flink.connector.lance.format;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Iterator for reading Arrow batches sequentially.
 */
public interface RecordBatchIterator extends Iterator<ArrowBatch>, AutoCloseable, Serializable {
    
    /**
     * Get total number of rows across all batches.
     * May be expensive for streaming scenarios.
     * 
     * @return total row count, or -1 if unknown
     */
    long getTotalRowCount();
    
    /**
     * Get current batch index (0-based).
     * 
     * @return current batch number
     */
    int getCurrentBatchIndex();
}
