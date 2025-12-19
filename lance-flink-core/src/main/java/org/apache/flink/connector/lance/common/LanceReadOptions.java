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

/**
 * Options for reading from Lance dataset.
 */
public class LanceReadOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Optional<String> whereClause;
    private final List<String> columns;
    private final Optional<Long> limit;
    private final Optional<Long> version;

    private LanceReadOptions(Builder builder) {
        this.whereClause = Optional.ofNullable(builder.whereClause);
        this.columns = new ArrayList<>(builder.columns);
        this.limit = Optional.ofNullable(builder.limit);
        this.version = Optional.ofNullable(builder.version);
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

    /**
     * Builder for LanceReadOptions.
     */
    public static class Builder {
        private String whereClause;
        private List<String> columns = new ArrayList<>();
        private Long limit;
        private Long version;

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
                ", version=" + version +
                '}';
    }
}
