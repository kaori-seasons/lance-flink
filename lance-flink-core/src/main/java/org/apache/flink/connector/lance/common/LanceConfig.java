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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for Lance connector.
 * Encapsulates all parameters needed to connect and interact with Lance datasets.
 */
public class LanceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DATASET_URI = "uri";
    public static final String STORAGE_BACKEND = "storage.backend";
    public static final String AWS_ACCESS_KEY_ID = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY = "aws.secret.access.key";
    public static final String FRAGMENT_SIZE = "fragment.size";
    public static final String READ_BATCH_SIZE = "read.batch.size";
    public static final String WRITE_BATCH_SIZE = "write.batch.size";
    public static final String ENABLE_PREDICATE_PUSHDOWN = "predicate.pushdown.enabled";
    public static final String ENABLE_COLUMN_PRUNING = "column.pruning.enabled";
    public static final String MAX_RETRIES = "max.retries";
    public static final String RETRY_WAIT_MILLIS = "retry.wait.millis";
    public static final String PRIMARY_KEY_COLUMN = "primary.key";

    private final String datasetUri;
    private final String storageBackend;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final int fragmentSize;
    private final long readBatchSize;
    private final long writeBatchSize;
    private final boolean enablePredicatePushdown;
    private final boolean enableColumnPruning;
    private final int maxRetries;
    private final long retryWaitMillis;
    private final String primaryKeyColumn;
    private final Map<String, String> originalOptions;

    private LanceConfig(Builder builder) {
        this.datasetUri = Objects.requireNonNull(builder.datasetUri, "datasetUri cannot be null");
        this.storageBackend = builder.storageBackend != null ? builder.storageBackend : "s3";
        this.awsAccessKeyId = builder.awsAccessKeyId;
        this.awsSecretAccessKey = builder.awsSecretAccessKey;
        this.fragmentSize = builder.fragmentSize > 0 ? builder.fragmentSize : 100000;
        this.readBatchSize = builder.readBatchSize > 0 ? builder.readBatchSize : 8192;
        this.writeBatchSize = builder.writeBatchSize > 0 ? builder.writeBatchSize : 512;
        this.enablePredicatePushdown = builder.enablePredicatePushdown;
        this.enableColumnPruning = builder.enableColumnPruning;
        this.maxRetries = builder.maxRetries > 0 ? builder.maxRetries : 3;
        this.retryWaitMillis = builder.retryWaitMillis > 0 ? builder.retryWaitMillis : 1000L;
        this.primaryKeyColumn = builder.primaryKeyColumn;
        this.originalOptions = new HashMap<>(builder.originalOptions);
    }

    public static LanceConfig fromMap(Map<String, String> options) throws LanceException {
        if (!options.containsKey(DATASET_URI)) {
            throw new LanceException("Configuration missing required parameter: " + DATASET_URI);
        }

        Builder builder = new Builder(options.get(DATASET_URI));

        if (options.containsKey(STORAGE_BACKEND)) {
            builder.storageBackend(options.get(STORAGE_BACKEND));
        }
        if (options.containsKey(AWS_ACCESS_KEY_ID)) {
            builder.awsAccessKeyId(options.get(AWS_ACCESS_KEY_ID));
        }
        if (options.containsKey(AWS_SECRET_ACCESS_KEY)) {
            builder.awsSecretAccessKey(options.get(AWS_SECRET_ACCESS_KEY));
        }

        try {
            if (options.containsKey(FRAGMENT_SIZE)) {
                builder.fragmentSize(Integer.parseInt(options.get(FRAGMENT_SIZE)));
            }
            if (options.containsKey(READ_BATCH_SIZE)) {
                builder.readBatchSize(Long.parseLong(options.get(READ_BATCH_SIZE)));
            }
            if (options.containsKey(WRITE_BATCH_SIZE)) {
                builder.writeBatchSize(Long.parseLong(options.get(WRITE_BATCH_SIZE)));
            }
            if (options.containsKey(MAX_RETRIES)) {
                builder.maxRetries(Integer.parseInt(options.get(MAX_RETRIES)));
            }
            if (options.containsKey(RETRY_WAIT_MILLIS)) {
                builder.retryWaitMillis(Long.parseLong(options.get(RETRY_WAIT_MILLIS)));
            }
        } catch (NumberFormatException e) {
            throw new LanceException("Failed to parse numeric configuration: " + e.getMessage(), e);
        }

        if (options.containsKey(ENABLE_PREDICATE_PUSHDOWN)) {
            builder.enablePredicatePushdown(Boolean.parseBoolean(options.get(ENABLE_PREDICATE_PUSHDOWN)));
        }
        if (options.containsKey(ENABLE_COLUMN_PRUNING)) {
            builder.enableColumnPruning(Boolean.parseBoolean(options.get(ENABLE_COLUMN_PRUNING)));
        }
        if (options.containsKey(PRIMARY_KEY_COLUMN)) {
            builder.primaryKeyColumn(options.get(PRIMARY_KEY_COLUMN));
        }

        builder.originalOptions(options);
        return builder.build();
    }

    public void validate() throws LanceException {
        if (datasetUri == null || datasetUri.trim().isEmpty()) {
            throw new LanceException("Dataset URI cannot be empty");
        }
        if (fragmentSize <= 0) {
            throw new LanceException("Fragment size must be positive");
        }
        if (readBatchSize <= 0) {
            throw new LanceException("Read batch size must be positive");
        }
        if (writeBatchSize <= 0) {
            throw new LanceException("Write batch size must be positive");
        }
    }

    // Getters
    public String getDatasetUri() {
        return datasetUri;
    }

    public String getStorageBackend() {
        return storageBackend;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public String getAwsSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public int getFragmentSize() {
        return fragmentSize;
    }

    public long getReadBatchSize() {
        return readBatchSize;
    }

    public long getWriteBatchSize() {
        return writeBatchSize;
    }

    public boolean isEnablePredicatePushdown() {
        return enablePredicatePushdown;
    }

    public boolean isEnableColumnPruning() {
        return enableColumnPruning;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryWaitMillis() {
        return retryWaitMillis;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public Map<String, String> getOriginalOptions() {
        return new HashMap<>(originalOptions);
    }

    /**
     * Builder for LanceConfig.
     */
    public static class Builder {
        private final String datasetUri;
        private String storageBackend;
        private String awsAccessKeyId;
        private String awsSecretAccessKey;
        private int fragmentSize = -1;
        private long readBatchSize = -1;
        private long writeBatchSize = -1;
        private boolean enablePredicatePushdown = true;
        private boolean enableColumnPruning = true;
        private int maxRetries = -1;
        private long retryWaitMillis = -1;
        private String primaryKeyColumn;
        private Map<String, String> originalOptions = new HashMap<>();

        public Builder(String datasetUri) {
            this.datasetUri = Objects.requireNonNull(datasetUri);
        }

        public Builder storageBackend(String storageBackend) {
            this.storageBackend = storageBackend;
            return this;
        }

        public Builder awsAccessKeyId(String awsAccessKeyId) {
            this.awsAccessKeyId = awsAccessKeyId;
            return this;
        }

        public Builder awsSecretAccessKey(String awsSecretAccessKey) {
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        public Builder fragmentSize(int fragmentSize) {
            this.fragmentSize = fragmentSize;
            return this;
        }

        public Builder readBatchSize(long readBatchSize) {
            this.readBatchSize = readBatchSize;
            return this;
        }

        public Builder writeBatchSize(long writeBatchSize) {
            this.writeBatchSize = writeBatchSize;
            return this;
        }

        public Builder enablePredicatePushdown(boolean enable) {
            this.enablePredicatePushdown = enable;
            return this;
        }

        public Builder enableColumnPruning(boolean enable) {
            this.enableColumnPruning = enable;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryWaitMillis(long retryWaitMillis) {
            this.retryWaitMillis = retryWaitMillis;
            return this;
        }

        public Builder primaryKeyColumn(String primaryKeyColumn) {
            this.primaryKeyColumn = primaryKeyColumn;
            return this;
        }

        public Builder originalOptions(Map<String, String> originalOptions) {
            this.originalOptions = new HashMap<>(originalOptions);
            return this;
        }

        public LanceConfig build() {
            return new LanceConfig(this);
        }
    }

    @Override
    public String toString() {
        return "LanceConfig{" +
                "datasetUri='" + datasetUri + '\'' +
                ", storageBackend='" + storageBackend + '\'' +
                ", fragmentSize=" + fragmentSize +
                ", readBatchSize=" + readBatchSize +
                ", writeBatchSize=" + writeBatchSize +
                ", enablePredicatePushdown=" + enablePredicatePushdown +
                ", enableColumnPruning=" + enableColumnPruning +
                '}';
    }
}
