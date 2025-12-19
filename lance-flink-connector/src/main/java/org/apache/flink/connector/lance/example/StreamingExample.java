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

package org.apache.flink.connector.lance.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.lance.common.LanceConfig;
import org.apache.flink.connector.lance.common.LanceReadOptions;
import org.apache.flink.connector.lance.sink.LanceUpsertSinkFunction;
import org.apache.flink.connector.lance.source.LanceUnboundedSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Example of streaming Lance dataset processing with Flink.
 * 
 * Demonstrates:
 * - Reading Lance datasets as streaming sources (with fragment polling)
 * - Exactly-Once semantics via checkpointing
 * - Writing data back via upsert sink
 * - Fragment tracking and recovery
 * 
 * Usage:
 *   java StreamingExample \
 *     --dataset-uri s3://mybucket/mydata \
 *     --output-uri s3://mybucket/output \
 *     --poll-interval-ms 5000 \
 *     --batch-size 1024
 */
public class StreamingExample {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingExample.class);

    public static void main(String[] args) throws Exception {
        LOG.info("=== Flink Lance Streaming Example ===");
        
        // Parse arguments
        String datasetUri = "file:///tmp/lance_dataset";
        String outputUri = "file:///tmp/lance_output";
        long pollIntervalMs = 5000L;
        int batchSize = 1024;

        for (int i = 0; i < args.length; i++) {
            if ("--dataset-uri".equals(args[i]) && i + 1 < args.length) {
                datasetUri = args[++i];
            } else if ("--output-uri".equals(args[i]) && i + 1 < args.length) {
                outputUri = args[++i];
            } else if ("--poll-interval-ms".equals(args[i]) && i + 1 < args.length) {
                pollIntervalMs = Long.parseLong(args[++i]);
            } else if ("--batch-size".equals(args[i]) && i + 1 < args.length) {
                batchSize = Integer.parseInt(args[++i]);
            }
        }

        LOG.info("Configuration:");
        LOG.info("  Dataset URI: {}", datasetUri);
        LOG.info("  Output URI: {}", outputUri);
        LOG.info("  Poll Interval: {} ms", pollIntervalMs);
        LOG.info("  Batch Size: {}", batchSize);

        // Set up the streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpoint for exactly-once semantics
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        
        LOG.info("Checkpoint enabled: interval=5000ms, mode=EXACTLY_ONCE");

        // Create Lance configuration
        LanceConfig sourceConfig = new LanceConfig.Builder(datasetUri)
                .readBatchSize(256)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();
        
        LanceConfig sinkConfig = new LanceConfig.Builder(outputUri)
                .writeBatchSize(batchSize)
                .enablePredicatePushdown(true)
                .enableColumnPruning(true)
                .build();

        // Define schema for rows
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new org.apache.flink.api.common.typeinfo.TypeInformation<?>[]{
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING
                },
                new String[]{"id", "name", "value"}
        );

        // Create streaming source from Lance dataset
        LOG.info("Creating unbounded source...");
        LanceReadOptions readOptions = new LanceReadOptions.Builder()
                .columns(Arrays.asList("id", "name", "value"))
                .build();
        
        LanceUnboundedSourceFunction source = new LanceUnboundedSourceFunction(
                sourceConfig, 
                readOptions, 
                rowTypeInfo,
                pollIntervalMs,  // Poll interval
                true              // Enable initial snapshot
        );

        DataStream<Row> sourceStream = env.addSource(source)
                .name("LanceUnboundedSource");

        // Apply transformations
        LOG.info("Setting up stream transformations...");
        DataStream<Row> processedStream = sourceStream
                // Filter: keep only rows with non-null id
                .filter((FilterFunction<Row>) row -> row.getField(0) != null)
                .name("FilterNullIds")
                
                // Map: add timestamp
                .map((MapFunction<Row, Row>) row -> {
                    Row newRow = new Row(4);
                    newRow.setField(0, row.getField(0));  // id
                    newRow.setField(1, row.getField(1));  // name
                    newRow.setField(2, row.getField(2));  // value
                    newRow.setField(3, System.currentTimeMillis()); // timestamp
                    return newRow;
                })
                .name("AddTimestamp");

        // Sink: write back to Lance dataset via upsert
        LOG.info("Setting up upsert sink...");
        LanceUpsertSinkFunction sink = new LanceUpsertSinkFunction(
                sinkConfig,
                "id",      // Primary key column
                batchSize
        );

        processedStream
                .addSink(sink)
                .name("LanceUpsertSink");

        // Print for debugging
        sourceStream.print("Lance-Source");

        // Execute the pipeline
        LOG.info("Starting streaming pipeline...");
        env.execute("Lance Streaming Example");
    }
}
