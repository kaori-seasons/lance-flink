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

package org.apache.flink.connector.lance.common;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for LanceConfig.
 */
public class LanceConfigTest {

    @Test
    public void testConfigWithMinimalOptions() throws LanceException {
        Map<String, String> options = new HashMap<>();
        options.put(LanceConfig.DATASET_URI, "s3://bucket/dataset");

        LanceConfig config = LanceConfig.fromMap(options);
        config.validate();

        assertEquals("s3://bucket/dataset", config.getDatasetUri());
        assertEquals("s3", config.getStorageBackend());
        assertEquals(100000, config.getFragmentSize());
        assertEquals(8192, config.getReadBatchSize());
        assertEquals(512, config.getWriteBatchSize());
        assertTrue(config.isEnablePredicatePushdown());
        assertTrue(config.isEnableColumnPruning());
    }

    @Test
    public void testConfigWithAllOptions() throws LanceException {
        Map<String, String> options = new HashMap<>();
        options.put(LanceConfig.DATASET_URI, "s3://bucket/dataset");
        options.put(LanceConfig.STORAGE_BACKEND, "hdfs");
        options.put(LanceConfig.AWS_ACCESS_KEY_ID, "key");
        options.put(LanceConfig.AWS_SECRET_ACCESS_KEY, "secret");
        options.put(LanceConfig.FRAGMENT_SIZE, "200000");
        options.put(LanceConfig.READ_BATCH_SIZE, "16384");
        options.put(LanceConfig.WRITE_BATCH_SIZE, "1024");
        options.put(LanceConfig.ENABLE_PREDICATE_PUSHDOWN, "false");
        options.put(LanceConfig.ENABLE_COLUMN_PRUNING, "false");
        options.put(LanceConfig.MAX_RETRIES, "5");
        options.put(LanceConfig.RETRY_WAIT_MILLIS, "2000");

        LanceConfig config = LanceConfig.fromMap(options);
        config.validate();

        assertEquals("s3://bucket/dataset", config.getDatasetUri());
        assertEquals("hdfs", config.getStorageBackend());
        assertEquals("key", config.getAwsAccessKeyId());
        assertEquals("secret", config.getAwsSecretAccessKey());
        assertEquals(200000, config.getFragmentSize());
        assertEquals(16384, config.getReadBatchSize());
        assertEquals(1024, config.getWriteBatchSize());
        assertFalse(config.isEnablePredicatePushdown());
        assertFalse(config.isEnableColumnPruning());
        assertEquals(5, config.getMaxRetries());
        assertEquals(2000, config.getRetryWaitMillis());
    }

    @Test(expected = LanceException.class)
    public void testConfigMissingDatasetUri() throws LanceException {
        Map<String, String> options = new HashMap<>();
        LanceConfig.fromMap(options);
    }

    @Test(expected = LanceException.class)
    public void testConfigInvalidNumberFormat() throws LanceException {
        Map<String, String> options = new HashMap<>();
        options.put(LanceConfig.DATASET_URI, "s3://bucket/dataset");
        options.put(LanceConfig.FRAGMENT_SIZE, "not_a_number");
        LanceConfig.fromMap(options);
    }

    @Test
    public void testConfigBuilder() throws LanceException {
        LanceConfig config = new LanceConfig.Builder("s3://bucket/dataset")
                .storageBackend("hdfs")
                .fragmentSize(150000)
                .readBatchSize(12288)
                .writeBatchSize(768)
                .enablePredicatePushdown(false)
                .build();

        config.validate();
        assertEquals("s3://bucket/dataset", config.getDatasetUri());
        assertEquals("hdfs", config.getStorageBackend());
        assertEquals(150000, config.getFragmentSize());
        assertEquals(12288, config.getReadBatchSize());
        assertEquals(768, config.getWriteBatchSize());
        assertFalse(config.isEnablePredicatePushdown());
    }

    @Test(expected = LanceException.class)
    public void testValidateEmptyUri() throws LanceException {
        LanceConfig config = new LanceConfig.Builder("")
                .build();
        config.validate();
    }

    @Test
    public void testValidateNegativeFragmentSize() throws LanceException {
        // When fragmentSize is -1, it defaults to 100000
        LanceConfig config = new LanceConfig.Builder("s3://bucket/dataset")
                .fragmentSize(-1)
                .build();
        config.validate();
        // Should not throw exception and should use default value
        assertEquals(100000, config.getFragmentSize());
    }

    @Test
    public void testConfigToString() throws LanceException {
        LanceConfig config = new LanceConfig.Builder("s3://bucket/dataset")
                .fragmentSize(100000)
                .build();
        
        String str = config.toString();
        assertTrue(str.contains("s3://bucket/dataset"));
        assertTrue(str.contains("100000"));
    }
}
