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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceWriteOptions;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for LanceAppendSinkFunction.
 */
public class LanceAppendSinkFunctionTest {

    private LanceConfig config;
    private LanceWriteOptions writeOptions;
    private RowTypeInfo rowTypeInfo;

    @Before
    public void setUp() {
        config = new LanceConfig.Builder("s3://test/dataset")
                .writeBatchSize(1024)
                .build();

        writeOptions = new LanceWriteOptions.Builder()
                .mode(LanceWriteOptions.WriteMode.APPEND)
                .build();

        rowTypeInfo = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        Types.LONG,
                        Types.STRING,
                        Types.INT
                },
                new String[]{"id", "name", "age"}
        );
    }

    @Test
    public void testSinkCreation() {
        LanceAppendSinkFunction sink = new LanceAppendSinkFunction(config, writeOptions, rowTypeInfo);
        assertNotNull(sink);
    }

    @Test
    public void testConfigValidation() throws Exception {
        LanceAppendSinkFunction sink = new LanceAppendSinkFunction(config, writeOptions, rowTypeInfo);

        config.validate();
        assertEquals("s3://test/dataset", config.getDatasetUri());
        assertEquals(1024, config.getWriteBatchSize());
    }

    @Test
    public void testWriteOptions() {
        LanceWriteOptions options = new LanceWriteOptions.Builder()
                .mode(LanceWriteOptions.WriteMode.APPEND)
                .maxBytesPerFile(268435456)
                .maxRowsPerGroup(8192)
                .build();

        assertEquals(LanceWriteOptions.WriteMode.APPEND, options.getMode());
        assertEquals(268435456, options.getMaxBytesPerFile());
        assertEquals(8192, options.getMaxRowsPerGroup());
    }

    @Test
    public void testRowCreation() {
        Row row = new Row(3);
        row.setField(0, 1L);
        row.setField(1, "Alice");
        row.setField(2, 30);

        assertEquals(1L, row.getField(0));
        assertEquals("Alice", row.getField(1));
        assertEquals(30, row.getField(2));
        assertEquals(3, row.getArity());
    }

    @Test
    public void testSinkMetrics() {
        LanceAppendSinkFunction.SinkMetrics metrics = 
            new LanceAppendSinkFunction.SinkMetrics(1000, 10, 50);

        assertEquals(1000, metrics.totalRecordsWritten);
        assertEquals(10, metrics.totalBatches);
        assertEquals(50, metrics.currentBufferSize);
    }

    @Test
    public void testWriteOptionsBuilder() {
        LanceWriteOptions options = new LanceWriteOptions.Builder()
                .mode(LanceWriteOptions.WriteMode.UPSERT)
                .compression("zstd")
                .maxBytesPerFile(512000000)
                .maxRowsPerGroup(16384)
                .build();

        assertEquals(LanceWriteOptions.WriteMode.UPSERT, options.getMode());
        assertEquals("zstd", options.getCompression());
        assertEquals(512000000, options.getMaxBytesPerFile());
        assertEquals(16384, options.getMaxRowsPerGroup());
    }
}
