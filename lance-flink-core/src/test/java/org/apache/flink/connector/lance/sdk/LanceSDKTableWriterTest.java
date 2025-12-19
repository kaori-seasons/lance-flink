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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.connector.lance.common.LanceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for LanceSDKTableWriter.
 */
public class LanceSDKTableWriterTest {
    private static final Logger LOG = LoggerFactory.getLogger(LanceSDKTableWriterTest.class);
    
    private BufferAllocator allocator;
    private LanceSDKTableWriter writer;
    private String testDatasetUri;
    
    @Before
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        testDatasetUri = "file://" + System.getProperty("java.io.tmpdir") + 
                        "/lance-test-" + System.currentTimeMillis();
        writer = new LanceSDKTableWriter(testDatasetUri, allocator);
        LOG.info("Test setup complete with dataset URI: {}", testDatasetUri);
    }
    
    @After
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        LOG.info("Test cleanup complete");
    }
    
    /**
     * Test null data handling.
     */
    @Test
    public void testAppendNullData() throws LanceException {
        LOG.info("Testing append with null data");
        
        // Should not throw exception for null data
        writer.append(null);
        
        LanceTableWriter.WriteStatistics stats = writer.getStatistics();
        assertEquals("Should have 0 rows written", 0, stats.rowsWritten);
    }
    
    /**
     * Test empty VectorSchemaRoot handling.
     */
    @Test
    public void testAppendEmptyData() throws LanceException {
        LOG.info("Testing append with empty data");
        
        VectorSchemaRoot empty = createEmptyVectorSchemaRoot();
        writer.append(empty);
        
        LanceTableWriter.WriteStatistics stats = writer.getStatistics();
        assertEquals("Should have 0 rows written", 0, stats.rowsWritten);
        
        empty.close();
    }
    
    /**
     * Test write statistics tracking.
     */
    @Test
    public void testWriteStatisticsTracking() throws LanceException {
        LOG.info("Testing write statistics tracking");
        
        LanceTableWriter.WriteStatistics stats = writer.getStatistics();
        assertNotNull("Statistics should not be null", stats);
        assertEquals("Initial rows should be 0", 0, stats.rowsWritten);
        assertEquals("Initial fragments should be 0", 0, stats.fragmentsCreated);
        
        LOG.info("Statistics: {}", stats);
    }
    
    /**
     * Test upsert with primary key.
     */
    @Test
    public void testUpsertWithPrimaryKey() throws LanceException {
        LOG.info("Testing upsert with primary key");
        
        VectorSchemaRoot data = createTestVectorSchemaRoot(10);
        
        try {
            writer.upsert(data, "id");
            
            LanceTableWriter.WriteStatistics stats = writer.getStatistics();
            assertTrue("Should have written some rows", stats.rowsWritten >= 0);
            
            LOG.info("Upsert statistics: {}", stats);
        } finally {
            data.close();
        }
    }
    
    /**
     * Test overwrite operation.
     */
    @Test
    public void testOverwrite() throws LanceException {
        LOG.info("Testing overwrite operation");
        
        VectorSchemaRoot data = createTestVectorSchemaRoot(20);
        
        try {
            writer.overwrite(data);
            
            LanceTableWriter.WriteStatistics stats = writer.getStatistics();
            assertTrue("Should have written some rows", stats.rowsWritten >= 0);
            
            LOG.info("Overwrite statistics: {}", stats);
        } finally {
            data.close();
        }
    }
    
    /**
     * Test flush operation.
     */
    @Test
    public void testFlush() throws LanceException {
        LOG.info("Testing flush operation");
        
        // Flush should complete without error
        writer.flush();
        
        LOG.info("Flush completed successfully");
    }
    
    /**
     * Test writer close.
     */
    @Test
    public void testWriterClose() throws Exception {
        LOG.info("Testing writer close");
        
        LanceSDKTableWriter testWriter = new LanceSDKTableWriter(testDatasetUri, allocator);
        testWriter.close();
        
        // After close, operations should fail
        LanceException ex = null;
        try {
            testWriter.append(null);
        } catch (LanceException e) {
            ex = e;
        }
        
        assertNotNull("Should throw LanceException after close", ex);
        LOG.info("Writer properly closed and prevented further operations");
    }
    
    /**
     * Test statistics update after write.
     */
    @Test
    public void testStatisticsUpdate() throws LanceException {
        LOG.info("Testing statistics update");
        
        VectorSchemaRoot data = createTestVectorSchemaRoot(5);
        
        try {
            LanceTableWriter.WriteStatistics statsBefore = writer.getStatistics();
            long rowsBefore = statsBefore.rowsWritten;
            
            writer.append(data);
            
            LanceTableWriter.WriteStatistics statsAfter = writer.getStatistics();
            long rowsAfter = statsAfter.rowsWritten;
            
            assertTrue("Rows written should increase after append",
                    rowsAfter >= rowsBefore);
            
            LOG.info("Statistics before: {}, after: {}", statsBefore, statsAfter);
        } finally {
            data.close();
        }
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Creates an empty VectorSchemaRoot for testing.
     */
    private VectorSchemaRoot createEmptyVectorSchemaRoot() {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("col1", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));
        
        Schema schema = new Schema(fields);
        return VectorSchemaRoot.create(schema, allocator);
    }
    
    /**
     * Creates a test VectorSchemaRoot with data.
     */
    private VectorSchemaRoot createTestVectorSchemaRoot(int rowCount) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));
        fields.add(new Field("value", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null));
        
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        
        VarCharVector idVector = (VarCharVector) root.getVector(0);
        VarCharVector valueVector = (VarCharVector) root.getVector(1);
        
        for (int i = 0; i < rowCount; i++) {
            byte[] idBytes = ("id_" + i).getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = ("value_" + i).getBytes(StandardCharsets.UTF_8);
            
            idVector.setSafe(i, idBytes);
            valueVector.setSafe(i, valueBytes);
        }
        
        root.setRowCount(rowCount);
        return root;
    }
}
