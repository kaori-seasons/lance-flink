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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Catalog S3 集成测试。
 * 
 * <p>本测试类分为两部分：
 * <ul>
 *   <li>不需要 MinIO 连接的单元测试（始终运行）</li>
 *   <li>需要 MinIO 连接的集成测试（需要配置外部 MinIO 服务）</li>
 * </ul>
 * 
 * <p>要运行需要 MinIO 的测试，请设置以下环境变量：
 * <ul>
 *   <li>MINIO_ENDPOINT - MinIO 服务地址，例如 http://localhost:9000</li>
 *   <li>MINIO_ACCESS_KEY - MinIO 访问密钥（默认：minioadmin）</li>
 *   <li>MINIO_SECRET_KEY - MinIO 密钥（默认：minioadmin）</li>
 *   <li>MINIO_BUCKET - 测试用 bucket 名称（默认：lance-test-bucket）</li>
 * </ul>
 * 
 * <p>启动 MinIO 的快速方法（使用 Docker）：
 * <pre>
 * docker run -p 9000:9000 -p 9001:9001 \
 *   -e "MINIO_ROOT_USER=minioadmin" \
 *   -e "MINIO_ROOT_PASSWORD=minioadmin" \
 *   minio/minio server /data --console-address ":9001"
 * </pre>
 * 
 * <p>或者使用本地安装的 MinIO 服务。
 */
class LanceCatalogS3Test {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalogS3Test.class);

    // MinIO 配置 - 从环境变量或系统属性读取
    private static String minioEndpoint;
    private static String minioAccessKey;
    private static String minioSecretKey;
    private static String testBucket;
    private static boolean minioAvailable = false;

    /**
     * 检查 MinIO 是否可用
     */
    static boolean isMinioAvailable() {
        return minioAvailable;
    }

    @BeforeAll
    static void initMinioConfig() {
        // 从环境变量读取配置
        minioEndpoint = getConfigValue("MINIO_ENDPOINT", "minio.endpoint", null);
        minioAccessKey = getConfigValue("MINIO_ACCESS_KEY", "minio.access.key", "minioadmin");
        minioSecretKey = getConfigValue("MINIO_SECRET_KEY", "minio.secret.key", "minioadmin");
        testBucket = getConfigValue("MINIO_BUCKET", "minio.bucket", "lance-test-bucket");

        if (minioEndpoint != null && !minioEndpoint.isEmpty()) {
            LOG.info("MinIO 配置已检测到:");
            LOG.info("  Endpoint: {}", minioEndpoint);
            LOG.info("  Bucket: {}", testBucket);
            
            // 尝试连接 MinIO 验证可用性
            try {
                minioAvailable = checkMinioConnection();
                if (minioAvailable) {
                    LOG.info("MinIO 连接验证成功，集成测试将被启用");
                } else {
                    LOG.warn("MinIO 连接验证失败，集成测试将被跳过");
                }
            } catch (Exception e) {
                LOG.warn("MinIO 连接检查失败: {}，集成测试将被跳过", e.getMessage());
                minioAvailable = false;
            }
        } else {
            LOG.info("未检测到 MinIO 配置（MINIO_ENDPOINT 环境变量未设置），集成测试将被跳过");
            LOG.info("要启用 MinIO 集成测试，请设置以下环境变量:");
            LOG.info("  export MINIO_ENDPOINT=http://localhost:9000");
            LOG.info("  export MINIO_ACCESS_KEY=minioadmin");
            LOG.info("  export MINIO_SECRET_KEY=minioadmin");
            LOG.info("  export MINIO_BUCKET=lance-test-bucket");
        }
    }

    /**
     * 从环境变量或系统属性获取配置值
     */
    private static String getConfigValue(String envKey, String propKey, String defaultValue) {
        String value = System.getenv(envKey);
        if (value == null || value.isEmpty()) {
            value = System.getProperty(propKey, defaultValue);
        }
        return value;
    }

    /**
     * 检查 MinIO 连接是否可用
     */
    private static boolean checkMinioConnection() {
        try {
            // 尝试创建一个简单的 HTTP 连接来检查 MinIO 服务是否可用
            java.net.URL url = new java.net.URL(minioEndpoint + "/minio/health/live");
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            int responseCode = connection.getResponseCode();
            connection.disconnect();
            return responseCode == 200;
        } catch (Exception e) {
            LOG.debug("MinIO 健康检查失败: {}", e.getMessage());
            return false;
        }
    }

    // ==================== 不需要 MinIO 的单元测试（始终运行） ====================

    /**
     * 不需要 MinIO 连接的单元测试
     */
    @Nested
    @DisplayName("单元测试 - 不需要 MinIO")
    class UnitTests {

        // ==================== 远程路径判断测试 ====================

        @Test
        @DisplayName("测试远程路径识别 - S3 协议")
        void testRemotePathDetectionS3() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "s3://bucket/path");
            assertThat(catalog.isRemoteStorage()).isTrue();
        }

        @Test
        @DisplayName("测试远程路径识别 - S3A 协议")
        void testRemotePathDetectionS3A() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "s3a://bucket/path");
            assertThat(catalog.isRemoteStorage()).isTrue();
        }

        @Test
        @DisplayName("测试远程路径识别 - GCS 协议")
        void testRemotePathDetectionGCS() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "gs://bucket/path");
            assertThat(catalog.isRemoteStorage()).isTrue();
        }

        @Test
        @DisplayName("测试远程路径识别 - Azure 协议")
        void testRemotePathDetectionAzure() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "az://container/path");
            assertThat(catalog.isRemoteStorage()).isTrue();
        }

        @Test
        @DisplayName("测试本地路径识别")
        void testLocalPathDetection() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "/tmp/local/path");
            assertThat(catalog.isRemoteStorage()).isFalse();
        }

        // ==================== 工厂测试 ====================

        @Test
        @DisplayName("测试 LanceCatalogFactory S3 配置选项")
        void testCatalogFactoryS3Options() {
            LanceCatalogFactory factory = new LanceCatalogFactory();

            Set<String> optionalOptionKeys = new HashSet<>();
            factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

            // 验证 S3 相关选项存在
            assertThat(optionalOptionKeys).contains(
                    "s3-access-key",
                    "s3-secret-key",
                    "s3-region",
                    "s3-endpoint",
                    "s3-virtual-hosted-style",
                    "s3-allow-http"
            );
        }

        @Test
        @DisplayName("测试 S3 配置选项默认值")
        void testS3ConfigOptionsDefaults() {
            assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.defaultValue()).isTrue();
            assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.defaultValue()).isFalse();
        }

        @Test
        @DisplayName("测试 S3 配置选项描述")
        void testS3ConfigOptionsDescriptions() {
            // 验证配置选项存在且有描述
            assertThat(LanceCatalogFactory.S3_ACCESS_KEY.key()).isEqualTo("s3-access-key");
            assertThat(LanceCatalogFactory.S3_SECRET_KEY.key()).isEqualTo("s3-secret-key");
            assertThat(LanceCatalogFactory.S3_REGION.key()).isEqualTo("s3-region");
            assertThat(LanceCatalogFactory.S3_ENDPOINT.key()).isEqualTo("s3-endpoint");
            
            // 验证描述不为空
            assertThat(LanceCatalogFactory.S3_ACCESS_KEY.description()).isNotNull();
            assertThat(LanceCatalogFactory.S3_SECRET_KEY.description()).isNotNull();
            assertThat(LanceCatalogFactory.S3_REGION.description()).isNotNull();
            assertThat(LanceCatalogFactory.S3_ENDPOINT.description()).isNotNull();
        }

        // ==================== 路径标准化测试 ====================

        @Test
        @DisplayName("测试仓库路径标准化 - 移除末尾斜杠")
        void testWarehousePathNormalization() {
            LanceCatalog catalog1 = new LanceCatalog("test", "default", "s3://bucket/path/");
            assertThat(catalog1.getWarehouse()).isEqualTo("s3://bucket/path");

            LanceCatalog catalog2 = new LanceCatalog("test", "default", "s3://bucket/path///");
            assertThat(catalog2.getWarehouse()).isEqualTo("s3://bucket/path");
        }

        @Test
        @DisplayName("测试仓库路径标准化 - 保留根路径")
        void testWarehousePathNormalizationRoot() {
            LanceCatalog catalog = new LanceCatalog("test", "default", "s3://bucket");
            assertThat(catalog.getWarehouse()).isEqualTo("s3://bucket");
        }

        // ==================== 边界条件测试 ====================

        @Test
        @DisplayName("测试空存储选项的 S3 路径")
        void testS3PathWithEmptyOptions() {
            LanceCatalog catalog = new LanceCatalog(
                    "test", "default",
                    "s3://bucket/path",
                    Collections.emptyMap());

            assertThat(catalog.isRemoteStorage()).isTrue();
            assertThat(catalog.getStorageOptions()).isEmpty();
        }

        @Test
        @DisplayName("测试 null 存储选项")
        void testNullStorageOptions() {
            LanceCatalog catalog = new LanceCatalog(
                    "test", "default",
                    "s3://bucket/path",
                    null);

            assertThat(catalog.isRemoteStorage()).isTrue();
            assertThat(catalog.getStorageOptions()).isEmpty();
        }

        @Test
        @DisplayName("测试存储选项不可变性")
        void testStorageOptionsImmutability() {
            Map<String, String> originalOptions = new HashMap<>();
            originalOptions.put("key", "value");

            LanceCatalog catalog = new LanceCatalog(
                    "test", "default",
                    "s3://bucket/path",
                    originalOptions);

            // 修改原始 map 不应影响 catalog 内部的选项
            originalOptions.put("new_key", "new_value");

            assertThat(catalog.getStorageOptions()).doesNotContainKey("new_key");
        }

        @Test
        @DisplayName("测试获取存储选项返回不可变 Map")
        void testGetStorageOptionsReturnsUnmodifiable() {
            Map<String, String> storageOptions = new HashMap<>();
            storageOptions.put("key", "value");

            LanceCatalog catalog = new LanceCatalog(
                    "test", "default",
                    "s3://bucket/path",
                    storageOptions);

            Map<String, String> returnedOptions = catalog.getStorageOptions();

            // 尝试修改返回的 map 应该抛出异常
            assertThatThrownBy(() -> returnedOptions.put("new_key", "new_value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("测试 S3 Catalog 基本属性（无需连接）")
        void testS3CatalogBasicProperties() {
            Map<String, String> storageOptions = new HashMap<>();
            storageOptions.put("aws_access_key_id", "test_key");
            storageOptions.put("aws_secret_access_key", "test_secret");
            storageOptions.put("aws_region", "us-east-1");

            LanceCatalog catalog = new LanceCatalog(
                    "test_catalog", "default",
                    "s3://test-bucket/warehouse",
                    storageOptions);

            assertThat(catalog.getName()).isEqualTo("test_catalog");
            assertThat(catalog.getDefaultDatabase()).isEqualTo("default");
            assertThat(catalog.getWarehouse()).isEqualTo("s3://test-bucket/warehouse");
            assertThat(catalog.isRemoteStorage()).isTrue();
            assertThat(catalog.getStorageOptions()).containsEntry("aws_access_key_id", "test_key");
        }
    }

    // ==================== 需要 MinIO 的集成测试 ====================

    /**
     * 需要 MinIO 连接的集成测试
     * 仅在设置了 MINIO_ENDPOINT 环境变量且 MinIO 服务可用时运行
     */
    @Nested
    @DisplayName("集成测试 - 需要 MinIO")
    @EnabledIf("org.apache.flink.connector.lance.table.LanceCatalogS3Test#isMinioAvailable")
    class MinioIntegrationTests {

        private LanceCatalog s3Catalog;
        private String warehousePath;
        private String testId;

        @BeforeEach
        void setUp() throws Exception {
            // 为每个测试生成唯一的路径，避免测试之间的干扰
            testId = UUID.randomUUID().toString().substring(0, 8);
            warehousePath = String.format("s3://%s/lance-warehouse-%s", testBucket, testId);

            // 创建带 S3 配置的 Catalog
            Map<String, String> storageOptions = new HashMap<>();
            storageOptions.put("aws_access_key_id", minioAccessKey);
            storageOptions.put("aws_secret_access_key", minioSecretKey);
            storageOptions.put("aws_region", "us-east-1");
            storageOptions.put("aws_endpoint", minioEndpoint);
            storageOptions.put("aws_virtual_hosted_style_request", "false");
            storageOptions.put("allow_http", "true");

            s3Catalog = new LanceCatalog("lance_s3_catalog", "default", warehousePath, storageOptions);
            s3Catalog.open();

            LOG.info("测试 Catalog 已创建，warehouse: {}", warehousePath);
        }

        @AfterEach
        void tearDown() throws Exception {
            if (s3Catalog != null) {
                s3Catalog.close();
            }
        }

        // ==================== 基本属性测试 ====================

        @Test
        @DisplayName("测试 S3 Catalog 基本属性")
        void testS3CatalogProperties() {
            assertThat(s3Catalog.getName()).isEqualTo("lance_s3_catalog");
            assertThat(s3Catalog.getDefaultDatabase()).isEqualTo("default");
            assertThat(s3Catalog.getWarehouse()).isEqualTo(warehousePath);
            assertThat(s3Catalog.isRemoteStorage()).isTrue();
        }

        @Test
        @DisplayName("测试 S3 存储选项配置")
        void testS3StorageOptions() {
            Map<String, String> options = s3Catalog.getStorageOptions();

            assertThat(options).containsEntry("aws_access_key_id", minioAccessKey);
            assertThat(options).containsEntry("aws_secret_access_key", minioSecretKey);
            assertThat(options).containsEntry("aws_region", "us-east-1");
            assertThat(options).containsEntry("aws_endpoint", minioEndpoint);
            assertThat(options).containsEntry("allow_http", "true");
        }

        // ==================== 数据库操作测试 ====================

        @Test
        @DisplayName("测试 S3 Catalog 默认数据库存在")
        void testDefaultDatabaseExists() throws Exception {
            assertThat(s3Catalog.databaseExists("default")).isTrue();
        }

        @Test
        @DisplayName("测试 S3 Catalog 列举数据库")
        void testListDatabases() throws Exception {
            List<String> databases = s3Catalog.listDatabases();
            assertThat(databases).contains("default");
        }

        @Test
        @DisplayName("测试 S3 Catalog 创建数据库")
        void testCreateDatabase() throws Exception {
            String dbName = "test_s3_db_" + testId;

            // 创建数据库
            s3Catalog.createDatabase(dbName, null, false);

            // 验证数据库存在
            assertThat(s3Catalog.databaseExists(dbName)).isTrue();

            // 验证数据库在列表中
            List<String> databases = s3Catalog.listDatabases();
            assertThat(databases).contains(dbName);
        }

        @Test
        @DisplayName("测试 S3 Catalog 创建已存在数据库（ignoreIfExists=false）")
        void testCreateExistingDatabaseWithoutIgnore() throws Exception {
            String dbName = "existing_db_" + testId;
            s3Catalog.createDatabase(dbName, null, false);

            // 再次创建应该抛出异常
            assertThatThrownBy(() -> s3Catalog.createDatabase(dbName, null, false))
                    .isInstanceOf(DatabaseAlreadyExistException.class);
        }

        @Test
        @DisplayName("测试 S3 Catalog 创建已存在数据库（ignoreIfExists=true）")
        void testCreateExistingDatabaseWithIgnore() throws Exception {
            String dbName = "existing_db_2_" + testId;
            s3Catalog.createDatabase(dbName, null, false);

            // 再次创建应该不抛出异常
            s3Catalog.createDatabase(dbName, null, true);

            assertThat(s3Catalog.databaseExists(dbName)).isTrue();
        }

        @Test
        @DisplayName("测试 S3 Catalog 获取数据库")
        void testGetDatabase() throws Exception {
            String dbName = "get_db_test_" + testId;
            s3Catalog.createDatabase(dbName, null, false);

            CatalogDatabase database = s3Catalog.getDatabase(dbName);
            assertThat(database).isNotNull();
            assertThat(database.getComment()).contains("Lance Database");
        }

        @Test
        @DisplayName("测试 S3 Catalog 获取不存在的数据库")
        void testGetNonExistingDatabase() {
            assertThatThrownBy(() -> s3Catalog.getDatabase("non_existing_db_" + testId))
                    .isInstanceOf(DatabaseNotExistException.class);
        }

        @Test
        @DisplayName("测试 S3 Catalog 删除数据库")
        void testDropDatabase() throws Exception {
            String dbName = "drop_db_test_" + testId;
            s3Catalog.createDatabase(dbName, null, false);
            assertThat(s3Catalog.databaseExists(dbName)).isTrue();

            // 删除数据库
            s3Catalog.dropDatabase(dbName, false, false);

            // 验证数据库不在列表中
            List<String> databases = s3Catalog.listDatabases();
            assertThat(databases).doesNotContain(dbName);
        }

        @Test
        @DisplayName("测试 S3 Catalog 删除不存在数据库（ignoreIfNotExists=false）")
        void testDropNonExistingDatabaseWithoutIgnore() {
            assertThatThrownBy(() -> s3Catalog.dropDatabase("non_existing_drop_db_" + testId, false, false))
                    .isInstanceOf(DatabaseNotExistException.class);
        }

        @Test
        @DisplayName("测试 S3 Catalog 删除不存在数据库（ignoreIfNotExists=true）")
        void testDropNonExistingDatabaseWithIgnore() throws Exception {
            // 不应该抛出异常
            s3Catalog.dropDatabase("non_existing_drop_db_2_" + testId, true, false);
        }

        // ==================== 表操作测试 ====================

        @Test
        @DisplayName("测试 S3 Catalog 列举表（空数据库）")
        void testListTablesEmpty() throws Exception {
            String dbName = "empty_tables_db_" + testId;
            s3Catalog.createDatabase(dbName, null, false);

            List<String> tables = s3Catalog.listTables(dbName);
            assertThat(tables).isEmpty();
        }

        @Test
        @DisplayName("测试 S3 Catalog 表不存在")
        void testTableNotExists() throws Exception {
            String dbName = "table_check_db_" + testId;
            s3Catalog.createDatabase(dbName, null, false);

            assertThat(s3Catalog.tableExists(new org.apache.flink.table.catalog.ObjectPath(dbName, "non_existing_table")))
                    .isFalse();
        }

        // ==================== SQL DDL 创建 Catalog 测试 ====================

        @Test
        @DisplayName("测试通过 SQL DDL 创建 S3 Catalog")
        void testCreateS3CatalogViaSql() throws Exception {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inBatchMode()
                    .build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);

            String catalogName = "lance_s3_sql_" + testId;

            // 使用 SQL 创建 S3 Catalog
            String createCatalogSql = String.format(
                    "CREATE CATALOG %s WITH (" +
                            "'type' = 'lance', " +
                            "'warehouse' = '%s', " +
                            "'default-database' = 'default', " +
                            "'s3-access-key' = '%s', " +
                            "'s3-secret-key' = '%s', " +
                            "'s3-region' = 'us-east-1', " +
                            "'s3-endpoint' = '%s', " +
                            "'s3-allow-http' = 'true', " +
                            "'s3-virtual-hosted-style' = 'false'" +
                            ")",
                    catalogName, warehousePath, minioAccessKey, minioSecretKey, minioEndpoint);

            tableEnv.executeSql(createCatalogSql);

            // 验证 Catalog 已创建
            String[] catalogs = tableEnv.listCatalogs();
            assertThat(catalogs).contains(catalogName);

            // 使用 Catalog
            tableEnv.useCatalog(catalogName);
            assertThat(tableEnv.getCurrentCatalog()).isEqualTo(catalogName);

            // 验证默认数据库
            assertThat(tableEnv.getCurrentDatabase()).isEqualTo("default");
        }

        @Test
        @DisplayName("测试通过 SQL DDL 在 S3 Catalog 中创建数据库")
        void testCreateDatabaseViaSql() throws Exception {
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inBatchMode()
                    .build();
            TableEnvironment tableEnv = TableEnvironment.create(settings);

            String catalogName = "lance_s3_db_sql_" + testId;

            // 创建 S3 Catalog
            String createCatalogSql = String.format(
                    "CREATE CATALOG %s WITH (" +
                            "'type' = 'lance', " +
                            "'warehouse' = '%s', " +
                            "'s3-access-key' = '%s', " +
                            "'s3-secret-key' = '%s', " +
                            "'s3-region' = 'us-east-1', " +
                            "'s3-endpoint' = '%s', " +
                            "'s3-allow-http' = 'true', " +
                            "'s3-virtual-hosted-style' = 'false'" +
                            ")",
                    catalogName, warehousePath, minioAccessKey, minioSecretKey, minioEndpoint);

            tableEnv.executeSql(createCatalogSql);
            tableEnv.useCatalog(catalogName);

            // 创建数据库
            String dbName = "test_database_" + testId;
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + dbName);

            // 验证数据库已创建
            String[] databases = tableEnv.listDatabases();
            assertThat(databases).contains(dbName);
        }

        // ==================== 多 Catalog 测试 ====================

        @Test
        @DisplayName("测试多个 S3 Catalog 实例")
        void testMultipleS3Catalogs() throws Exception {
            Map<String, String> storageOptions = new HashMap<>();
            storageOptions.put("aws_access_key_id", minioAccessKey);
            storageOptions.put("aws_secret_access_key", minioSecretKey);
            storageOptions.put("aws_region", "us-east-1");
            storageOptions.put("aws_endpoint", minioEndpoint);
            storageOptions.put("allow_http", "true");

            // 创建第一个 Catalog
            LanceCatalog catalog1 = new LanceCatalog(
                    "catalog1", "default",
                    "s3://" + testBucket + "/warehouse1_" + testId,
                    storageOptions);
            catalog1.open();

            // 创建第二个 Catalog
            LanceCatalog catalog2 = new LanceCatalog(
                    "catalog2", "default",
                    "s3://" + testBucket + "/warehouse2_" + testId,
                    storageOptions);
            catalog2.open();

            try {
                // 验证两个 Catalog 独立工作
                assertThat(catalog1.getWarehouse()).isNotEqualTo(catalog2.getWarehouse());

                String db1 = "db1_" + testId;
                String db2 = "db2_" + testId;
                
                catalog1.createDatabase(db1, null, false);
                catalog2.createDatabase(db2, null, false);

                assertThat(catalog1.listDatabases()).contains(db1);
                assertThat(catalog2.listDatabases()).contains(db2);
                assertThat(catalog1.listDatabases()).doesNotContain(db2);
                assertThat(catalog2.listDatabases()).doesNotContain(db1);
            } finally {
                catalog1.close();
                catalog2.close();
            }
        }
    }
}
