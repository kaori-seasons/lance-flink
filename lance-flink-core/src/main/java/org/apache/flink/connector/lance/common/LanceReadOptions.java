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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Options for reading from Lance dataset.
 */
public class LanceReadOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Optional<String> whereClause;
    private final List<String> columns;
    private final Optional<Long> limit;
    private final Optional<Long> version;
    private final Optional<Long> offset;
    // 优化参数：通过 Object 避免循环依赖
    private final Optional<Object> optimizationContext;

    private LanceReadOptions(Builder builder) {
        this.whereClause = Optional.ofNullable(builder.whereClause);
        this.columns = new ArrayList<>(builder.columns);
        this.limit = Optional.ofNullable(builder.limit);
        this.version = Optional.ofNullable(builder.version);
        this.offset = Optional.ofNullable(builder.offset);
        this.optimizationContext = Optional.ofNullable(builder.optimizationContext);
    }

    public Optional<String> getWhereClause() {
        return whereClause;
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    public Optional<Long> getLimit() {
        return limit;
    }

    public Optional<Long> getVersion() {
        return version;
    }

    public Optional<Long> getOffset() {
        return offset;
    }

    /**
     * Gets the optimization context applied to this read operation.
     * Returns the context as Object to avoid circular dependencies.
     * Cast to LanceOptimizationContext when needed.
     *
     * @return optimization context if present, empty otherwise
     */
    public Optional<Object> getOptimizationContext() {
        return optimizationContext;
    }

    /**
     * Checks if optimization context is present.
     *
     * @return true if optimization context has been set
     */
    public boolean hasOptimizationContext() {
        return optimizationContext.isPresent();
    }

    /**
     * Builder for LanceReadOptions.
     */
    public static class Builder {
        private String whereClause;
        private List<String> columns = new ArrayList<>();
        private Long limit;
        private Long version;
        private Long offset;
        private Object optimizationContext;

        public Builder whereClause(String whereClause) {
            this.whereClause = whereClause;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = new ArrayList<>(columns);
            return this;
        }

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Sets the optimization context for this read operation.
         * Accepts Object type to avoid circular module dependencies.
         *
         * @param context the optimization context
         * @return this builder
         */
        public Builder optimizationContext(Object context) {
            this.optimizationContext = context;
            return this;
        }

        public LanceReadOptions build() {
            return new LanceReadOptions(this);
        }
    }

    @Override
    public String toString() {
        return "LanceReadOptions{" +
                "whereClause=" + whereClause +
                ", columns=" + columns +
                ", limit=" + limit +
                ", offset=" + offset +
                ", version=" + version +
                ", hasOptimizationContext=" + optimizationContext.isPresent() +
                '}';
    }
}
