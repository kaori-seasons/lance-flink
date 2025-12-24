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

package org.apache.flink.connector.lance.integration.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for collecting and reporting performance metrics during tests.
 *
 * <p>Provides methods for:
 * <ul>
 *   <li>Timing measurements (start/stop timers)</li>
 *   <li>Throughput calculations (rows/second)</li>
 *   <li>Metric recording and reporting</li>
 *   <li>Statistical analysis (min, max, avg)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * TestMetricsCollector metrics = new TestMetricsCollector();
 * metrics.startTimer();
 * 
 * // Perform operation
 * long durationMs = metrics.stopTimer();
 * 
 * double throughput = metrics.getThroughput(100000, durationMs);
 * metrics.recordMetric("read_throughput", throughput);
 * metrics.reportMetrics();
 * }</pre>
 */
public class TestMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(TestMetricsCollector.class);

    // Timer management
    private long startTime = 0;
    private long stopTime = 0;
    private boolean timerRunning = false;

    // Metrics storage
    private final Map<String, DoubleMetric> metrics = new HashMap<>();
    private final Map<String, Long> timingMetrics = new HashMap<>();

    // ============================================================================
    // Timer Management
    // ============================================================================

    /**
     * Start a performance timer.
     * Can be called multiple times; each call resets the timer.
     */
    public void startTimer() {
        startTime = System.currentTimeMillis();
        stopTime = 0;
        timerRunning = true;
        LOG.debug("Timer started");
    }

    /**
     * Stop the performance timer and return the elapsed time.
     *
     * @return Elapsed time in milliseconds
     * @throws IllegalStateException if timer was not started
     */
    public long stopTimer() {
        if (!timerRunning) {
            throw new IllegalStateException("Timer was not started");
        }
        
        stopTime = System.currentTimeMillis();
        timerRunning = false;
        
        long elapsed = getElapsedTimeMillis();
        LOG.debug("Timer stopped. Elapsed: {} ms", elapsed);
        return elapsed;
    }

    /**
     * Get elapsed time without stopping the timer.
     * Useful for progress reporting during long-running operations.
     *
     * @return Elapsed time in milliseconds since timer started
     * @throws IllegalStateException if timer was not started
     */
    public long getElapsedTimeWithoutStopping() {
        if (startTime == 0) {
            throw new IllegalStateException("Timer was not started");
        }
        return System.currentTimeMillis() - startTime;
    }

    /**
     * Get elapsed time (only after stopping the timer).
     *
     * @return Elapsed time in milliseconds
     * @throws IllegalStateException if timer is still running or was not started
     */
    public long getElapsedTimeMillis() {
        if (timerRunning) {
            throw new IllegalStateException("Timer is still running");
        }
        if (startTime == 0) {
            throw new IllegalStateException("Timer was not started");
        }
        return stopTime - startTime;
    }

    /**
     * Get elapsed time in seconds.
     */
    public double getElapsedTimeSeconds() {
        return getElapsedTimeMillis() / 1000.0;
    }

    /**
     * Reset the timer.
     */
    public void resetTimer() {
        startTime = 0;
        stopTime = 0;
        timerRunning = false;
    }

    // ============================================================================
    // Throughput Calculations
    // ============================================================================

    /**
     * Calculate throughput (rows per second).
     *
     * @param rowCount Total number of rows processed
     * @param durationMillis Duration in milliseconds
     * @return Throughput in rows per second
     */
    public static double getThroughput(long rowCount, long durationMillis) {
        if (durationMillis == 0) {
            throw new IllegalArgumentException("Duration cannot be zero");
        }
        return (rowCount * 1000.0) / durationMillis;
    }

    /**
     * Calculate throughput based on current timer.
     *
     * @param rowCount Total number of rows processed
     * @return Throughput in rows per second
     * @throws IllegalStateException if timer is not stopped
     */
    public double calculateThroughput(long rowCount) {
        return getThroughput(rowCount, getElapsedTimeMillis());
    }

    /**
     * Calculate latency per row.
     *
     * @param rowCount Total number of rows processed
     * @param durationMillis Duration in milliseconds
     * @return Latency per row in milliseconds
     */
    public static double getLatencyPerRow(long rowCount, long durationMillis) {
        if (rowCount == 0) {
            throw new IllegalArgumentException("Row count cannot be zero");
        }
        return durationMillis / (double) rowCount;
    }

    /**
     * Calculate latency based on current timer.
     *
     * @param rowCount Total number of rows processed
     * @return Latency per row in milliseconds
     */
    public double calculateLatencyPerRow(long rowCount) {
        return getLatencyPerRow(rowCount, getElapsedTimeMillis());
    }

    // ============================================================================
    // Metric Recording
    // ============================================================================

    /**
     * Record a metric value.
     * If metric already exists, stores multiple occurrences for statistical analysis.
     *
     * @param name Metric name
     * @param value Metric value
     */
    public void recordMetric(String name, double value) {
        metrics.computeIfAbsent(name, k -> new DoubleMetric())
                .addValue(value);
        LOG.debug("Recorded metric: {} = {}", name, value);
    }

    /**
     * Record a timing metric.
     *
     * @param name Metric name
     * @param valueMillis Value in milliseconds
     */
    public void recordTimingMetric(String name, long valueMillis) {
        timingMetrics.put(name, valueMillis);
        LOG.debug("Recorded timing metric: {} = {} ms", name, valueMillis);
    }

    /**
     * Record throughput metric directly.
     *
     * @param name Metric name
     * @param rowCount Number of rows processed
     * @param durationMillis Duration in milliseconds
     */
    public void recordThroughputMetric(String name, long rowCount, long durationMillis) {
        double throughput = getThroughput(rowCount, durationMillis);
        recordMetric(name, throughput);
    }

    // ============================================================================
    // Metric Retrieval
    // ============================================================================

    /**
     * Get the value of a recorded metric.
     *
     * @param name Metric name
     * @return Metric value (average if multiple values recorded)
     * @throws IllegalArgumentException if metric not found
     */
    public double getMetric(String name) {
        if (!metrics.containsKey(name)) {
            throw new IllegalArgumentException("Metric not found: " + name);
        }
        return metrics.get(name).getAverage();
    }

    /**
     * Get the average value of a metric.
     */
    public double getMetricAverage(String name) {
        if (!metrics.containsKey(name)) {
            throw new IllegalArgumentException("Metric not found: " + name);
        }
        return metrics.get(name).getAverage();
    }

    /**
     * Get the minimum value of a metric (if recorded multiple times).
     */
    public double getMetricMin(String name) {
        if (!metrics.containsKey(name)) {
            throw new IllegalArgumentException("Metric not found: " + name);
        }
        return metrics.get(name).getMin();
    }

    /**
     * Get the maximum value of a metric (if recorded multiple times).
     */
    public double getMetricMax(String name) {
        if (!metrics.containsKey(name)) {
            throw new IllegalArgumentException("Metric not found: " + name);
        }
        return metrics.get(name).getMax();
    }

    /**
     * Get the count of times a metric was recorded.
     */
    public int getMetricCount(String name) {
        if (!metrics.containsKey(name)) {
            throw new IllegalArgumentException("Metric not found: " + name);
        }
        return metrics.get(name).getCount();
    }

    /**
     * Get a timing metric value.
     */
    public long getTimingMetric(String name) {
        if (!timingMetrics.containsKey(name)) {
            throw new IllegalArgumentException("Timing metric not found: " + name);
        }
        return timingMetrics.get(name);
    }

    // ============================================================================
    // Metric Reporting
    // ============================================================================

    /**
     * Print all recorded metrics to the log.
     */
    public void reportMetrics() {
        LOG.info("=== Performance Metrics Report ===");
        
        // Report double metrics
        if (!metrics.isEmpty()) {
            LOG.info("--- Double Metrics ---");
            for (Map.Entry<String, DoubleMetric> entry : metrics.entrySet()) {
                DoubleMetric metric = entry.getValue();
                LOG.info("{}: avg={}, min={}, max={}, count={}",
                        entry.getKey(),
                        String.format("%.2f", metric.getAverage()),
                        String.format("%.2f", metric.getMin()),
                        String.format("%.2f", metric.getMax()),
                        metric.getCount());
            }
        }
        
        // Report timing metrics
        if (!timingMetrics.isEmpty()) {
            LOG.info("--- Timing Metrics (ms) ---");
            for (Map.Entry<String, Long> entry : timingMetrics.entrySet()) {
                LOG.info("{}: {}", entry.getKey(), entry.getValue());
            }
        }
        
        LOG.info("=== End of Report ===");
    }

    /**
     * Export metrics as a CSV-formatted string.
     *
     * @return CSV string with metrics
     */
    public String exportAsCSV() {
        StringBuilder sb = new StringBuilder();
        sb.append("metric_name,metric_type,value,min,max,count\n");
        
        // Export double metrics
        for (Map.Entry<String, DoubleMetric> entry : metrics.entrySet()) {
            DoubleMetric metric = entry.getValue();
            sb.append(entry.getKey()).append(",double,")
                    .append(String.format("%.2f", metric.getAverage())).append(",")
                    .append(String.format("%.2f", metric.getMin())).append(",")
                    .append(String.format("%.2f", metric.getMax())).append(",")
                    .append(metric.getCount()).append("\n");
        }
        
        // Export timing metrics
        for (Map.Entry<String, Long> entry : timingMetrics.entrySet()) {
            sb.append(entry.getKey()).append(",timing,")
                    .append(entry.getValue()).append(",,,\n");
        }
        
        return sb.toString();
    }

    /**
     * Clear all recorded metrics and reset timers.
     */
    public void reset() {
        metrics.clear();
        timingMetrics.clear();
        resetTimer();
        LOG.debug("Metrics collector reset");
    }

    // ============================================================================
    // Inner Classes
    // ============================================================================

    /**
     * Helper class to track multiple values of a metric.
     */
    private static class DoubleMetric {
        private double sum = 0;
        private double min = Double.MAX_VALUE;
        private double max = Double.MIN_VALUE;
        private int count = 0;

        void addValue(double value) {
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        double getAverage() {
            return count > 0 ? sum / count : 0;
        }

        double getMin() {
            return count > 0 ? min : 0;
        }

        double getMax() {
            return count > 0 ? max : 0;
        }

        int getCount() {
            return count;
        }
    }

    // ============================================================================
    // Convenience Methods
    // ============================================================================

    /**
     * Convenience method to measure and record throughput for an operation.
     *
     * @param metricName Name of the metric to record
     * @param rowCount Number of rows processed
     * @param operation The operation to measure
     */
    public void measureThroughput(String metricName, long rowCount, Runnable operation) {
        startTimer();
        operation.run();
        long duration = stopTimer();
        recordThroughputMetric(metricName, rowCount, duration);
    }

    /**
     * Format a throughput value with units.
     *
     * @param rowsPerSecond Throughput value
     * @return Formatted string with appropriate unit
     */
    public static String formatThroughput(double rowsPerSecond) {
        if (rowsPerSecond >= 1_000_000) {
            return String.format("%.2f M rows/sec", rowsPerSecond / 1_000_000);
        } else if (rowsPerSecond >= 1_000) {
            return String.format("%.2f K rows/sec", rowsPerSecond / 1_000);
        } else {
            return String.format("%.2f rows/sec", rowsPerSecond);
        }
    }

    /**
     * Format a timing value with appropriate units.
     *
     * @param milliseconds Time in milliseconds
     * @return Formatted string with appropriate unit
     */
    public static String formatTiming(long milliseconds) {
        if (milliseconds >= 60_000) {
            return String.format("%.2f min", milliseconds / 60_000.0);
        } else if (milliseconds >= 1_000) {
            return String.format("%.2f sec", milliseconds / 1_000.0);
        } else {
            return String.format("%d ms", milliseconds);
        }
    }
}
