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

package org.apache.flink.connector.lance.catalog;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Production-ready unit tests for LanceCatalogFactory.
 */
public class LanceCatalogFactoryTest {
    private String tempMetadataPath;
    
    @Before
    public void setUp() throws Exception {
        tempMetadataPath = Files.createTempDirectory("lance-catalog-factory-test").toString();
    }
    
    @After
    public void tearDown() throws Exception {
        LanceCatalogFactory.closeAllCatalogs();
        deleteDirectory(new File(tempMetadataPath));
    }
    
    @Test
    public void testCreateCatalog() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "test_catalog", tempMetadataPath
        );
        
        LanceCatalog catalog = LanceCatalogFactory.createCatalog(config);
        assertNotNull(catalog);
        assertEquals("test_catalog", catalog.getName());
    }
    
    @Test
    public void testGetExistingCatalog() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "test_catalog", tempMetadataPath
        );
        
        LanceCatalog catalog1 = LanceCatalogFactory.createCatalog(config);
        LanceCatalog catalog2 = LanceCatalogFactory.getCatalog("test_catalog");
        
        assertSame(catalog1, catalog2);
    }
    
    @Test
    public void testSingletonInstance() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "singleton_catalog", tempMetadataPath
        );
        
        LanceCatalog catalog1 = LanceCatalogFactory.createCatalog(config);
        LanceCatalog catalog2 = LanceCatalogFactory.createCatalog(config);
        
        assertSame(catalog1, catalog2);
    }
    
    @Test
    public void testCloseCatalog() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "closeable_catalog", tempMetadataPath
        );
        
        LanceCatalog catalog = LanceCatalogFactory.createCatalog(config);
        LanceCatalogFactory.closeCatalog("closeable_catalog");
        
        assertThrows(Exception.class, () -> LanceCatalogFactory.getCatalog("closeable_catalog"));
    }
    
    @Test
    public void testCloseAllCatalogs() throws Exception {
        Map<String, String> config1 = LanceCatalogFactory.buildConfig(
            "catalog1", tempMetadataPath
        );
        Map<String, String> config2 = LanceCatalogFactory.buildConfig(
            "catalog2", tempMetadataPath
        );
        
        LanceCatalogFactory.createCatalog(config1);
        LanceCatalogFactory.createCatalog(config2);
        
        LanceCatalogFactory.closeAllCatalogs();
        
        assertThrows(Exception.class, () -> LanceCatalogFactory.getCatalog("catalog1"));
        assertThrows(Exception.class, () -> LanceCatalogFactory.getCatalog("catalog2"));
    }
    
    @Test
    public void testBuildConfig() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "my_catalog", "/tmp/metadata"
        );
        
        assertEquals("my_catalog", config.get(LanceCatalogFactory.CATALOG_NAME));
        assertEquals("/tmp/metadata", config.get(LanceCatalogFactory.METADATA_PATH));
    }
    
    @Test
    public void testCatalogOperationsViFactory() throws Exception {
        Map<String, String> config = LanceCatalogFactory.buildConfig(
            "operational_catalog", tempMetadataPath
        );
        
        LanceCatalog catalog = LanceCatalogFactory.createCatalog(config);
        
        // Create database
        catalog.createDatabase("test_db");
        assertTrue(catalog.databaseExists("test_db"));
        
        // Create table
        LanceCatalog.LanceTableSchema schema = new LanceCatalog.LanceTableSchema(
            "test_table", "/tmp/lance/test_table"
        );
        schema.addColumn("id", "BIGINT");
        catalog.createTable("test_db", "test_table", schema);
        
        // Verify via same factory instance
        LanceCatalog sameInstance = LanceCatalogFactory.getCatalog("operational_catalog");
        assertTrue(sameInstance.tableExists("test_db", "test_table"));
    }
    
    @Test(expected = Exception.class)
    public void testGetNonExistentCatalog() throws Exception {
        LanceCatalogFactory.getCatalog("non_existent_catalog");
    }
    
    // ============ Helper Methods ============
    
    private void deleteDirectory(File dir) throws Exception {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }
    
    private void assertThrows(Class<? extends Exception> exceptionClass, Runnable runnable) {
        try {
            runnable.run();
            fail("Expected " + exceptionClass.getName() + " but no exception was thrown");
        } catch (Exception e) {
            if (!exceptionClass.isInstance(e)) {
                throw new AssertionError("Expected " + exceptionClass.getName() + 
                    " but got " + e.getClass().getName(), e);
            }
        }
    }
}
