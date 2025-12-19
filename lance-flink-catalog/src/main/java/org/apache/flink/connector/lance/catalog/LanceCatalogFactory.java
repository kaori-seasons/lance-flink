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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Production-ready factory for creating and managing Lance Catalogs.
 * 
 * Supports:
 * 1. Factory pattern for catalog creation
 * 2. Singleton catalog instances per name
 * 3. Configuration validation
 * 4. Lifecycle management
 */
public class LanceCatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogFactory.class);
    
    // Configuration keys
    public static final String CATALOG_NAME = "catalog_name";
    public static final String METADATA_PATH = "metadata_path";
    public static final String DEFAULT_METADATA_PATH = "/tmp/lance/catalogs";
    
    // Singleton instance holder
    private static final Map<String, LanceCatalog> CATALOG_INSTANCES = new HashMap<>();
    
    /**
     * Create or get a catalog instance.
     */
    public static synchronized LanceCatalog createCatalog(Map<String, String> config) 
            throws Exception {
        LOG.info("Creating Lance Catalog with config: {}", config);
        
        String catalogName = config.get(CATALOG_NAME);
        Objects.requireNonNull(catalogName, CATALOG_NAME + " is required");
        
        String metadataPath = config.getOrDefault(METADATA_PATH, DEFAULT_METADATA_PATH);
        
        // Return existing instance if already created
        if (CATALOG_INSTANCES.containsKey(catalogName)) {
            LOG.debug("Returning existing catalog instance: {}", catalogName);
            return CATALOG_INSTANCES.get(catalogName);
        }
        
        // Create new instance
        LanceCatalog catalog = new LanceCatalog(catalogName, metadataPath);
        catalog.open();
        CATALOG_INSTANCES.put(catalogName, catalog);
        
        LOG.info("Lance Catalog created successfully: {} (metadata: {})", catalogName, metadataPath);
        return catalog;
    }
    
    /**
     * Get an existing catalog instance.
     */
    public static LanceCatalog getCatalog(String catalogName) throws Exception {
        LanceCatalog catalog = CATALOG_INSTANCES.get(catalogName);
        if (catalog == null) {
            throw new Exception("Catalog not found: " + catalogName);
        }
        return catalog;
    }
    
    /**
     * Close a catalog instance.
     */
    public static synchronized void closeCatalog(String catalogName) throws Exception {
        LanceCatalog catalog = CATALOG_INSTANCES.get(catalogName);
        if (catalog != null) {
            catalog.close();
            CATALOG_INSTANCES.remove(catalogName);
            LOG.info("Catalog closed: {}", catalogName);
        }
    }
    
    /**
     * Close all catalog instances.
     */
    public static synchronized void closeAllCatalogs() throws Exception {
        for (Map.Entry<String, LanceCatalog> entry : CATALOG_INSTANCES.entrySet()) {
            try {
                entry.getValue().close();
                LOG.info("Closed catalog: {}", entry.getKey());
            } catch (Exception e) {
                LOG.warn("Error closing catalog: {}", entry.getKey(), e);
            }
        }
        CATALOG_INSTANCES.clear();
    }
    
    /**
     * Build configuration from properties.
     */
    public static Map<String, String> buildConfig(String catalogName, String metadataPath) {
        Map<String, String> config = new HashMap<>();
        config.put(CATALOG_NAME, catalogName);
        config.put(METADATA_PATH, metadataPath);
        return config;
    }
}
