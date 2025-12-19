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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Unit tests for LanceBoundedSourceFunction.
 */
public class LanceBoundedSourceFunctionTest {

    private LanceConfig config;
    private LanceReadOptions readOptions;
    private RowTypeInfo rowTypeInfo;

    @Before
    public void setUp() {
        config = new LanceConfig.Builder("s3://test/dataset")
                .readBatchSize(8192)
                .build();

        readOptions = new LanceReadOptions.Builder()
                .columns(Arrays.asList("id", "name", "age"))
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
    public void testSourceCreation() {
        LanceBoundedSourceFunction source = new LanceBoundedSourceFunction(
            config, readOptions, rowTypeInfo);
        assertNotNull(source);
    }

    @Test
    public void testConfigValidation() throws Exception {
        config.validate();
        assertEquals("s3://test/dataset", config.getDatasetUri());
        assertEquals(8192, config.getReadBatchSize());
    }

    @Test
    public void testReadOptions() {
        LanceReadOptions options = new LanceReadOptions.Builder()
                .columns(Arrays.asList("col1", "col2", "col3"))
                .whereClause("age > 18")
                .limit(1000)
                .version(42)
                .build();

        assertTrue(options.getWhereClause().isPresent());
        assertEquals("age > 18", options.getWhereClause().get());
        assertEquals(Arrays.asList("col1", "col2", "col3"), options.getColumns());
        assertTrue(options.getLimit().isPresent());
        assertEquals(1000L, (long) options.getLimit().get());
        assertTrue(options.getVersion().isPresent());
        assertEquals(42L, (long) options.getVersion().get());
    }

    @Test
    public void testReadOptionsOptional() {
        LanceReadOptions options = new LanceReadOptions.Builder()
                .build();

        assertFalse(options.getWhereClause().isPresent());
        assertFalse(options.getLimit().isPresent());
        assertFalse(options.getVersion().isPresent());
        assertTrue(options.getColumns().isEmpty());
    }

    @Test
    public void testRowTypeInfo() {
        String[] fieldNames = {"id", "name", "age"};
        RowTypeInfo typeInfo = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        Types.LONG,
                        Types.STRING,
                        Types.INT
                },
                fieldNames
        );

        assertEquals(3, typeInfo.getArity());
        assertEquals("id", typeInfo.getFieldNames()[0]);
        assertEquals("name", typeInfo.getFieldNames()[1]);
        assertEquals("age", typeInfo.getFieldNames()[2]);
    }

    @Test
    public void testBoundedSourceConfiguration() {
        LanceReadOptions options = new LanceReadOptions.Builder()
                .columns(Arrays.asList("*"))
                .limit(Long.MAX_VALUE)
                .build();

        assertEquals(1, options.getColumns().size());
        assertEquals("*", options.getColumns().get(0));
    }
}
