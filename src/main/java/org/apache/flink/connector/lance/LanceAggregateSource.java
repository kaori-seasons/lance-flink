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

package org.apache.flink.connector.lance;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.aggregate.AggregateExecutor;
import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * 支持聚合下推的 Lance 数据源。
 * 
 * <p>在数据源端执行聚合计算，支持 COUNT、SUM、AVG、MIN、MAX 等聚合函数。
 * 
 * <p>使用示例：
 * <pre>{@code
 * AggregateInfo aggInfo = AggregateInfo.builder()
 *     .addCountStar("cnt")
 *     .addSum("amount", "total_amount")
 *     .groupBy("category")
 *     .build();
 * 
 * LanceAggregateSource source = new LanceAggregateSource(options, rowType, aggInfo);
 * }</pre>
 */
public class LanceAggregateSource extends RichParallelSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceAggregateSource.class);

    private final LanceOptions options;
    private final RowType sourceRowType;
    private final AggregateInfo aggregateInfo;
    private final String[] selectedColumns;

    private transient volatile boolean running;
    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient AggregateExecutor aggregateExecutor;

    /**
     * 创建 LanceAggregateSource
     *
     * @param options Lance 配置选项
     * @param sourceRowType 源表的 RowType
     * @param aggregateInfo 聚合信息
     */
    public LanceAggregateSource(LanceOptions options, RowType sourceRowType, AggregateInfo aggregateInfo) {
        this.options = options;
        this.sourceRowType = sourceRowType;
        this.aggregateInfo = aggregateInfo;
        
        // 计算需要读取的列
        List<String> requiredColumns = aggregateInfo.getRequiredColumns();
        this.selectedColumns = requiredColumns.isEmpty() ? null : requiredColumns.toArray(new String[0]);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("打开 Lance 聚合数据源: {}", options.getPath());
        LOG.info("聚合信息: {}", aggregateInfo);
        
        this.running = true;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        // 打开 Lance 数据集
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance 数据集路径不能为空");
        }
        
        try {
            this.dataset = Dataset.open(datasetPath, allocator);
        } catch (Exception e) {
            throw new IOException("无法打开 Lance 数据集: " + datasetPath, e);
        }
        
        // 初始化 RowDataConverter（使用源表 Schema）
        RowType actualRowType = this.sourceRowType;
        if (actualRowType == null) {
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        
        // 初始化聚合执行器
        this.aggregateExecutor = new AggregateExecutor(aggregateInfo, actualRowType);
        this.aggregateExecutor.init();
        
        LOG.info("Lance 聚合数据源已打开");
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LOG.info("开始聚合读取 Lance 数据集: {}", options.getPath());
        
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        
        // 聚合操作只在第一个子任务执行，避免重复聚合
        if (subtaskIndex != 0) {
            LOG.info("子任务 {} 跳过（聚合模式下只有子任务 0 执行）", subtaskIndex);
            return;
        }
        
        String filter = options.getReadFilter();
        
        // 读取所有数据并进行聚合
        if (filter != null && !filter.isEmpty()) {
            readAndAggregateWithFilter(ctx);
        } else {
            readAndAggregateAll(ctx);
        }
        
        LOG.info("Lance 聚合数据源读取完成");
    }

    /**
     * 带过滤条件的聚合读取
     */
    private void readAndAggregateWithFilter(SourceContext<RowData> ctx) throws Exception {
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            LOG.info("应用过滤条件: {}", filter);
            scanOptionsBuilder.filter(filter);
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // 第一阶段：读取数据并累积聚合
        try (LanceScanner scanner = dataset.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    List<RowData> rows = converter.toRowDataList(root);
                    
                    for (RowData row : rows) {
                        aggregateExecutor.accumulate(row);
                    }
                }
            }
        }
        
        // 第二阶段：输出聚合结果
        outputAggregateResults(ctx);
    }

    /**
     * 读取所有数据并聚合（无过滤条件）
     */
    private void readAndAggregateAll(SourceContext<RowData> ctx) throws Exception {
        List<Fragment> fragments = dataset.getFragments();
        LOG.info("数据集共有 {} 个 Fragment", fragments.size());
        
        // 第一阶段：读取所有 Fragment 并累积聚合
        for (Fragment fragment : fragments) {
            if (!running) {
                break;
            }
            readAndAggregateFragment(fragment);
        }
        
        // 第二阶段：输出聚合结果
        outputAggregateResults(ctx);
    }

    /**
     * 读取单个 Fragment 并累积聚合
     */
    private void readAndAggregateFragment(Fragment fragment) throws Exception {
        LOG.debug("读取 Fragment: {}", fragment.getId());
        
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        try (LanceScanner scanner = fragment.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    List<RowData> rows = converter.toRowDataList(root);
                    
                    for (RowData row : rows) {
                        aggregateExecutor.accumulate(row);
                    }
                }
            }
        }
    }

    /**
     * 输出聚合结果
     */
    private void outputAggregateResults(SourceContext<RowData> ctx) {
        List<RowData> results = aggregateExecutor.getResults();
        LOG.info("聚合完成，共 {} 个结果行", results.size());
        
        synchronized (ctx.getCheckpointLock()) {
            for (RowData result : results) {
                ctx.collect(result);
            }
        }
    }

    @Override
    public void cancel() {
        LOG.info("取消 Lance 聚合数据源");
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭 Lance 聚合数据源");
        
        this.running = false;
        
        if (aggregateExecutor != null) {
            aggregateExecutor.reset();
        }
        
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("关闭 Lance 数据集时出错", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭内存分配器时出错", e);
            }
            allocator = null;
        }
        
        super.close();
    }

    /**
     * 获取聚合信息
     */
    public AggregateInfo getAggregateInfo() {
        return aggregateInfo;
    }

    /**
     * 获取配置选项
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * 获取源表 RowType
     */
    public RowType getSourceRowType() {
        return sourceRowType;
    }

    /**
     * 获取聚合结果的 RowType
     */
    public RowType getResultRowType() {
        if (aggregateExecutor != null) {
            return aggregateExecutor.buildResultRowType();
        }
        return null;
    }
}
