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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceException;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.dataset.LanceDatasetAdapter;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Base class for Lance source functions.
 * Handles common initialization and resource management.
 */
public abstract class BaseLanceSourceFunction extends RichSourceFunction<Row> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected static final Logger LOG = LoggerFactory.getLogger(BaseLanceSourceFunction.class);

    protected final LanceConfig config;
    protected final LanceReadOptions readOptions;
    protected final RowTypeInfo rowTypeInfo;

    protected LanceDatasetAdapter adapter;
    private volatile boolean isRunning = true;

    public BaseLanceSourceFunction(
            LanceConfig config,
            LanceReadOptions readOptions,
            RowTypeInfo rowTypeInfo) {
        this.config = config;
        this.readOptions = readOptions;
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        LOG.info("Opening source function with config: {}", config);

        try {
            // Validate configuration
            config.validate();

            // Initialize Lance adapter
            adapter = new LanceDatasetAdapter(config);

            isRunning = true;
            LOG.info("Source function opened successfully");
        } catch (Exception e) {
            LOG.error("Failed to open source function", e);
            throw new LanceException("Failed to initialize source function", e);
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling source function");
        isRunning = false;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing source function");
        isRunning = false;

        if (adapter != null) {
            try {
                adapter.close();
            } catch (IOException e) {
                LOG.error("Error closing adapter", e);
            }
        }
    }

    protected boolean isRunning() {
        return isRunning;
    }

    /**
     * Get type information for the output rows.
     * Note: This method is informational only for the source function.
     */
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    /**
     * Helper method to create a Row from field values.
     */
    protected Row createRow(Object... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }
}
