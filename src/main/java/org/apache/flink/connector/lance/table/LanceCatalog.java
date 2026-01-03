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

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Lance Catalog 实现。
 * 
 * <p>实现 Flink Catalog 接口，支持管理 Lance 数据集作为 Flink 表。
 * 支持本地文件系统和 S3 协议的对象存储。
 * 
 * <p>使用示例（本地路径）：
 * <pre>{@code
 * CREATE CATALOG lance_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = '/path/to/warehouse',
 *     'default-database' = 'default'
 * );
 * }</pre>
 * 
 * <p>使用示例（S3 路径）：
 * <pre>{@code
 * CREATE CATALOG lance_s3_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = 's3://bucket-name/warehouse',
 *     'default-database' = 'default',
 *     's3-access-key' = 'your-access-key',
 *     's3-secret-key' = 'your-secret-key',
 *     's3-region' = 'us-east-1'
 * );
 * }</pre>
 */
public class LanceCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    private final String warehouse;
    private final Map<String, String> storageOptions;
    private final boolean isRemoteStorage;
    private transient BufferAllocator allocator;
    
    // 用于远程存储时缓存已知的数据库和表
    private final Set<String> knownDatabases = ConcurrentHashMap.newKeySet();
    private final Set<String> knownTables = ConcurrentHashMap.newKeySet();

    /**
     * 创建 LanceCatalog（本地存储）
     *
     * @param name Catalog 名称
     * @param defaultDatabase 默认数据库名称
     * @param warehouse 仓库路径
     */
    public LanceCatalog(String name, String defaultDatabase, String warehouse) {
        this(name, defaultDatabase, warehouse, Collections.emptyMap());
    }

    /**
     * 创建 LanceCatalog（支持远程存储）
     *
     * @param name Catalog 名称
     * @param defaultDatabase 默认数据库名称
     * @param warehouse 仓库路径（本地路径或 S3 URI）
     * @param storageOptions 存储配置选项（如 S3 凭证）
     */
    public LanceCatalog(String name, String defaultDatabase, String warehouse, Map<String, String> storageOptions) {
        super(name, defaultDatabase);
        this.warehouse = normalizeWarehousePath(warehouse);
        this.storageOptions = storageOptions != null ? new HashMap<>(storageOptions) : Collections.emptyMap();
        this.isRemoteStorage = isRemotePath(warehouse);
    }

    /**
     * 判断是否是远程存储路径
     */
    private boolean isRemotePath(String path) {
        if (path == null) {
            return false;
        }
        String lowerPath = path.toLowerCase();
        return lowerPath.startsWith("s3://") || 
               lowerPath.startsWith("s3a://") || 
               lowerPath.startsWith("gs://") || 
               lowerPath.startsWith("az://") ||
               lowerPath.startsWith("https://") ||
               lowerPath.startsWith("http://");
    }

    /**
     * 标准化仓库路径
     */
    private String normalizeWarehousePath(String path) {
        if (path == null) {
            return null;
        }
        // 移除末尾的斜杠
        while (path.endsWith("/") && path.length() > 1) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("打开 Lance Catalog: {}, 仓库路径: {}, 远程存储: {}", getName(), warehouse, isRemoteStorage);
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        if (isRemoteStorage) {
            // 远程存储：初始化默认数据库记录
            knownDatabases.add(getDefaultDatabase());
            LOG.info("远程存储模式已启用，存储配置项数量: {}", storageOptions.size());
        } else {
            // 本地存储：确保仓库目录存在
            Path warehousePath = Paths.get(warehouse);
            if (!Files.exists(warehousePath)) {
                try {
                    Files.createDirectories(warehousePath);
                } catch (IOException e) {
                    throw new CatalogException("无法创建仓库目录: " + warehouse, e);
                }
            }
            
            // 确保默认数据库存在
            Path defaultDbPath = warehousePath.resolve(getDefaultDatabase());
            if (!Files.exists(defaultDbPath)) {
                try {
                    Files.createDirectories(defaultDbPath);
                } catch (IOException e) {
                    throw new CatalogException("无法创建默认数据库目录: " + defaultDbPath, e);
                }
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("关闭 Lance Catalog: {}", getName());
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭分配器失败", e);
            }
            allocator = null;
        }
        
        knownDatabases.clear();
        knownTables.clear();
    }

    // ==================== Database 操作 ====================

    @Override
    public List<String> listDatabases() throws CatalogException {
        if (isRemoteStorage) {
            // 远程存储：返回已知数据库列表
            return new ArrayList<>(knownDatabases);
        }
        
        try {
            Path warehousePath = Paths.get(warehouse);
            if (!Files.exists(warehousePath)) {
                return Collections.emptyList();
            }
            
            return Files.list(warehousePath)
                    .filter(Files::isDirectory)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("列举数据库失败", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        return new CatalogDatabaseImpl(Collections.emptyMap(), "Lance Database: " + databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (isRemoteStorage) {
            // 远程存储：检查已知数据库或尝试列出表来验证
            if (knownDatabases.contains(databaseName)) {
                return true;
            }
            // 尝试通过检查是否有表来确认数据库存在
            try {
                String dbPath = getDatabasePath(databaseName);
                // 对于远程存储，我们假设数据库总是存在（实际表操作时会验证）
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        
        Path dbPath = Paths.get(warehouse, databaseName);
        return Files.exists(dbPath) && Files.isDirectory(dbPath);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (isRemoteStorage) {
            // 远程存储：只记录数据库名称，实际目录在创建表时自动创建
            if (knownDatabases.contains(name)) {
                if (!ignoreIfExists) {
                    throw new DatabaseAlreadyExistException(getName(), name);
                }
                return;
            }
            knownDatabases.add(name);
            LOG.info("注册远程数据库: {}", name);
            return;
        }
        
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            Files.createDirectories(dbPath);
            LOG.info("创建数据库: {}", name);
        } catch (IOException e) {
            throw new CatalogException("创建数据库失败: " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (isRemoteStorage) {
            // 远程存储：移除数据库记录
            if (!knownDatabases.contains(name)) {
                if (!ignoreIfNotExists) {
                    throw new DatabaseNotExistException(getName(), name);
                }
                return;
            }
            
            // 检查是否有表
            List<String> tables = listTables(name);
            if (!tables.isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            // 如果 cascade，删除所有表
            if (cascade) {
                for (String table : tables) {
                    try {
                        dropTable(new ObjectPath(name, table), true);
                    } catch (TableNotExistException e) {
                        // 忽略
                    }
                }
            }
            
            knownDatabases.remove(name);
            LOG.info("移除远程数据库记录: {}", name);
            return;
        }
        
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            List<String> tables = listTables(name);
            if (!tables.isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            // 删除数据库目录
            deleteDirectory(dbPath);
            LOG.info("删除数据库: {}", name);
        } catch (IOException e) {
            throw new CatalogException("删除数据库失败: " + name, e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        // Lance 数据库不支持修改属性
        LOG.warn("Lance Catalog 不支持修改数据库属性");
    }

    // ==================== Table 操作 ====================

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        if (isRemoteStorage) {
            // 远程存储：返回已知表列表
            String prefix = databaseName + "/";
            return knownTables.stream()
                    .filter(t -> t.startsWith(prefix))
                    .map(t -> t.substring(prefix.length()))
                    .collect(Collectors.toList());
        }
        
        try {
            Path dbPath = Paths.get(warehouse, databaseName);
            return Files.list(dbPath)
                    .filter(Files::isDirectory)
                    .filter(path -> Files.exists(path.resolve("_versions"))) // Lance 数据集标识
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("列举表失败", e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        // Lance 不支持视图
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        try {
            // 对于远程存储，通过环境变量配置 S3 凭证
            if (isRemoteStorage) {
                configureStorageEnvironment();
            }
            Dataset dataset = Dataset.open(datasetPath, allocator);
            
            try {
                // 从 Lance Schema 推断 Flink Schema
                org.apache.arrow.vector.types.pojo.Schema arrowSchema = dataset.getSchema();
                RowType rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
                
                // 构建 CatalogTable
                Schema.Builder schemaBuilder = Schema.newBuilder();
                for (RowType.RowField field : rowType.getFields()) {
                    DataType dataType = LanceTypeConverter.toDataType(field.getType());
                    schemaBuilder.column(field.getName(), dataType);
                }
                
                Map<String, String> options = new HashMap<>();
                options.put("connector", LanceDynamicTableFactory.IDENTIFIER);
                options.put("path", datasetPath);
                
                // 如果是远程存储，添加存储配置到表选项
                if (isRemoteStorage) {
                    options.putAll(getStorageOptionsForTable());
                }
                
                return CatalogTable.of(
                        schemaBuilder.build(),
                        "Lance Table: " + tablePath.getFullName(),
                        Collections.emptyList(),
                        options
                );
            } finally {
                dataset.close();
            }
        } catch (Exception e) {
            throw new CatalogException("获取表信息失败: " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            return false;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        if (isRemoteStorage) {
            // 远程存储：检查已知表或尝试打开数据集
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            if (knownTables.contains(tableKey)) {
                return true;
            }
            
            // 尝试打开数据集来验证是否存在
            try {
                configureStorageEnvironment();
                Dataset dataset = Dataset.open(datasetPath, allocator);
                dataset.close();
                knownTables.add(tableKey);
                return true;
            } catch (Exception e) {
                LOG.debug("表不存在或无法访问: {}", datasetPath, e);
                return false;
            }
        }
        
        Path path = Paths.get(datasetPath);
        
        // 检查是否是有效的 Lance 数据集
        return Files.exists(path) && Files.isDirectory(path) && 
               Files.exists(path.resolve("_versions"));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        if (isRemoteStorage) {
            // 远程存储：目前 Lance Java SDK 不直接支持删除远程数据集
            // 这里只移除记录，实际删除需要使用云存储 API
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            knownTables.remove(tableKey);
            LOG.warn("远程存储模式下，表记录已移除，但实际数据需要手动从存储中删除: {}", datasetPath);
            return;
        }
        
        try {
            deleteDirectory(Paths.get(datasetPath));
            LOG.info("删除表: {}", tablePath);
        } catch (IOException e) {
            throw new CatalogException("删除表失败: " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tableExists(newTablePath)) {
            throw new TableAlreadyExistException(getName(), newTablePath);
        }
        
        if (isRemoteStorage) {
            // 远程存储：不支持重命名
            throw new CatalogException("远程存储模式下不支持重命名表");
        }
        
        String oldPath = getDatasetPath(tablePath);
        String newPath = getDatasetPath(newTablePath);
        
        try {
            Files.move(Paths.get(oldPath), Paths.get(newPath));
            LOG.info("重命名表: {} -> {}", tablePath, newTablePath);
        } catch (IOException e) {
            throw new CatalogException("重命名表失败: " + tablePath, e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }
        
        if (isRemoteStorage) {
            // 远程存储：记录表信息，实际创建在写入时完成
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            knownTables.add(tableKey);
        }
        
        // 表的实际创建在第一次写入时完成
        // 这里只记录表的元数据
        LOG.info("注册表: {}（实际数据集将在写入时创建）", tablePath);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        // Lance 不支持修改表结构
        throw new CatalogException("Lance Catalog 不支持修改表结构");
    }

    // ==================== 分区操作（Lance 不支持分区）====================

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    // ==================== 函数操作（Lance 不支持用户自定义函数）====================

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    // ==================== 统计信息操作 ====================

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // 不支持
    }

    // ==================== 工具方法 ====================

    /**
     * 配置存储环境变量（用于 S3 等远程存储）
     * 
     * <p>Lance 通过环境变量配置 S3 凭证：
     * <ul>
     *   <li>AWS_ACCESS_KEY_ID - AWS 访问密钥 ID</li>
     *   <li>AWS_SECRET_ACCESS_KEY - AWS 秘密访问密钥</li>
     *   <li>AWS_DEFAULT_REGION - AWS 区域</li>
     *   <li>AWS_ENDPOINT - 自定义端点 URL（用于兼容 S3 的存储）</li>
     * </ul>
     */
    private void configureStorageEnvironment() {
        if (!isRemoteStorage || storageOptions.isEmpty()) {
            return;
        }
        
        // 设置环境变量用于 Lance SDK 的 object_store 配置
        // 注意：由于 Java 不能直接修改环境变量，这里使用系统属性作为备选方案
        // Lance 的 Rust 底层会读取这些环境变量
        
        if (storageOptions.containsKey("aws_access_key_id")) {
            System.setProperty("AWS_ACCESS_KEY_ID", storageOptions.get("aws_access_key_id"));
        }
        if (storageOptions.containsKey("aws_secret_access_key")) {
            System.setProperty("AWS_SECRET_ACCESS_KEY", storageOptions.get("aws_secret_access_key"));
        }
        if (storageOptions.containsKey("aws_region")) {
            System.setProperty("AWS_DEFAULT_REGION", storageOptions.get("aws_region"));
        }
        if (storageOptions.containsKey("aws_endpoint")) {
            System.setProperty("AWS_ENDPOINT", storageOptions.get("aws_endpoint"));
        }
        if (storageOptions.containsKey("aws_virtual_hosted_style_request")) {
            System.setProperty("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", 
                    storageOptions.get("aws_virtual_hosted_style_request"));
        }
        if (storageOptions.containsKey("allow_http")) {
            System.setProperty("AWS_ALLOW_HTTP", storageOptions.get("allow_http"));
        }
        
        LOG.debug("已配置远程存储环境变量");
    }

    /**
     * 获取数据库路径
     */
    private String getDatabasePath(String databaseName) {
        if (isRemoteStorage) {
            return warehouse + "/" + databaseName;
        }
        return Paths.get(warehouse, databaseName).toString();
    }

    /**
     * 获取数据集路径
     */
    private String getDatasetPath(ObjectPath tablePath) {
        if (isRemoteStorage) {
            return warehouse + "/" + tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
        }
        return Paths.get(warehouse, tablePath.getDatabaseName(), tablePath.getObjectName()).toString();
    }

    /**
     * 获取用于表配置的存储选项
     */
    private Map<String, String> getStorageOptionsForTable() {
        Map<String, String> options = new HashMap<>();
        
        // 转换存储选项为表配置格式
        if (storageOptions.containsKey("aws_access_key_id")) {
            options.put("s3-access-key", storageOptions.get("aws_access_key_id"));
        }
        if (storageOptions.containsKey("aws_secret_access_key")) {
            options.put("s3-secret-key", storageOptions.get("aws_secret_access_key"));
        }
        if (storageOptions.containsKey("aws_region")) {
            options.put("s3-region", storageOptions.get("aws_region"));
        }
        if (storageOptions.containsKey("aws_endpoint")) {
            options.put("s3-endpoint", storageOptions.get("aws_endpoint"));
        }
        
        return options;
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("删除文件失败: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    /**
     * 获取仓库路径
     */
    public String getWarehouse() {
        return warehouse;
    }

    /**
     * 获取存储配置选项
     */
    public Map<String, String> getStorageOptions() {
        return Collections.unmodifiableMap(storageOptions);
    }

    /**
     * 是否为远程存储
     */
    public boolean isRemoteStorage() {
        return isRemoteStorage;
    }
}
