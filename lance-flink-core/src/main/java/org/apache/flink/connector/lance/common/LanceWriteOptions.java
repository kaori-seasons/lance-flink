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

/**
 * Options for writing to Lance dataset.
 */
public class LanceWriteOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum WriteMode {
        APPEND,      // Add new fragments to the dataset
        UPSERT,      // Update based on primary key
        OVERWRITE    // Replace entire dataset
    }

    private final WriteMode mode;
    private final String compression;
    private final int maxBytesPerFile;
    private final int maxRowsPerGroup;

    private LanceWriteOptions(Builder builder) {
        this.mode = builder.mode != null ? builder.mode : WriteMode.APPEND;
        this.compression = builder.compression;
        this.maxBytesPerFile = builder.maxBytesPerFile > 0 ? builder.maxBytesPerFile : 268435456; // 256MB
        this.maxRowsPerGroup = builder.maxRowsPerGroup > 0 ? builder.maxRowsPerGroup : 8192;
    }

    public WriteMode getMode() {
        return mode;
    }

    public String getCompression() {
        return compression;
    }

    public int getMaxBytesPerFile() {
        return maxBytesPerFile;
    }

    public int getMaxRowsPerGroup() {
        return maxRowsPerGroup;
    }

    /**
     * Builder for LanceWriteOptions.
     */
    public static class Builder {
        private WriteMode mode = WriteMode.APPEND;
        private String compression;
        private int maxBytesPerFile = -1;
        private int maxRowsPerGroup = -1;

        public Builder mode(WriteMode mode) {
            this.mode = mode;
            return this;
        }

        public Builder compression(String compression) {
            this.compression = compression;
            return this;
        }

        public Builder maxBytesPerFile(int maxBytesPerFile) {
            this.maxBytesPerFile = maxBytesPerFile;
            return this;
        }

        public Builder maxRowsPerGroup(int maxRowsPerGroup) {
            this.maxRowsPerGroup = maxRowsPerGroup;
            return this;
        }

        public LanceWriteOptions build() {
            return new LanceWriteOptions(this);
        }
    }

    @Override
    public String toString() {
        return "LanceWriteOptions{" +
                "mode=" + mode +
                ", compression='" + compression + '\'' +
                ", maxBytesPerFile=" + maxBytesPerFile +
                ", maxRowsPerGroup=" + maxRowsPerGroup +
                '}';
    }
}
