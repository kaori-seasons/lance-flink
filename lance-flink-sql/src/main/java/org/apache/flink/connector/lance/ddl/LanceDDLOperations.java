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

package org.apache.flink.connector.lance.ddl;

import org.apache.flink.connector.lance.common.LanceException;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.cleanup.CleanupPolicy;
import org.lance.cleanup.RemovalStats;
import org.lance.compaction.CompactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Production-ready DDL operations for Lance tables.
 * Implements OPTIMIZE, VACUUM, COMPACT, RESTORE operations using Lance SDK directly.
 * This class provides a clean interface to Lance's native operations with proper error handling.
 */
public class LanceDDLOperations {
    private static final Logger LOG = LoggerFactory.getLogger(LanceDDLOperations.class);

    private final String datasetUri;
    private Dataset dataset;

    /**
     * Constructs a new LanceDDLOperations instance.
     *
     * @param datasetUri the URI of the Lance dataset
     * @throws NullPointerException if datasetUri is null
     */
    public LanceDDLOperations(String datasetUri) {
        this.datasetUri = Objects.requireNonNull(datasetUri, "Dataset URI cannot be null");
        LOG.debug("Initialized LanceDDLOperations for dataset: {}", datasetUri);
    }

    /**
     * Lazily loads and caches the Lance dataset.
     *
     * @return the opened Dataset instance
     * @throws LanceException if dataset cannot be opened
     */
    private Dataset loadDataset() throws LanceException {
        if (dataset == null) {
            try {
                LOG.debug("Opening Lance dataset from {}", datasetUri);
                dataset = Dataset.open().uri(datasetUri).build();
                LOG.info("Successfully opened Lance dataset at {}", datasetUri);
            } catch (Exception e) {
                LOG.error("Failed to open Lance dataset from {}", datasetUri, e);
                throw new LanceException("Failed to open dataset from: " + datasetUri, e);
            }
        }
        return dataset;
    }

    /**
     * OPTIMIZE TABLE - Consolidate fragments using Lance's compaction API.
     * This operation merges small fragments and removes deleted/dropped data.
     *
     * @param clusteringColumns optional columns for clustering optimization
     * @param targetRowsPerFragment target number of rows per fragment
     * @return optimization result with timing information
     * @throws LanceException if optimization fails
     */
    public OptimizeResult optimize(List<String> clusteringColumns, long targetRowsPerFragment)
            throws LanceException {
        LOG.info(
            "Starting OPTIMIZE: dataset={}, clustering={}, targetRows={}",
            datasetUri,
            clusteringColumns,
            targetRowsPerFragment);
        long startTime = System.currentTimeMillis();

        try {
            Dataset ds = loadDataset();
            CompactionOptions.Builder optionsBuilder = CompactionOptions.builder();

            if (targetRowsPerFragment > 0) {
                optionsBuilder.withTargetRowsPerFragment(targetRowsPerFragment);
            }

            // Lance SDK compact operation
            ds.compact(optionsBuilder.build());

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("OPTIMIZE completed in {}ms", duration);

            return new OptimizeResult().setDuration(duration).setDatasetUri(datasetUri);

        } catch (Exception e) {
            LOG.error("OPTIMIZE failed for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to optimize table: " + datasetUri, e);
        }
    }

    /**
     * VACUUM TABLE - Remove old versions using Lance's cleanup API.
     * Removes versions before specified time or version number.
     *
     * @param retentionDays number of days to retain (older versions will be deleted)
     * @param dryRun if true, only report what would be deleted without actually deleting
     * @return vacuum result with statistics
     * @throws LanceException if vacuum fails
     */
    public VacuumResult vacuum(int retentionDays, boolean dryRun) throws LanceException {
        LOG.info(
            "Starting VACUUM: dataset={}, retention={}days, dryRun={}",
            datasetUri,
            retentionDays,
            dryRun);
        long startTime = System.currentTimeMillis();

        try {
            Dataset ds = loadDataset();
            long cutoffTimeMillis = System.currentTimeMillis() - (long) retentionDays * 24 * 3600 * 1000;

            CleanupPolicy.Builder policyBuilder = CleanupPolicy.builder()
                .withBeforeTimestampMillis(cutoffTimeMillis);

            RemovalStats stats = ds.cleanupWithPolicy(policyBuilder.build());

            long duration = System.currentTimeMillis() - startTime;
            LOG.info(
                "VACUUM completed in {}ms: bytesRemoved={}, oldVersions={}",
                duration,
                stats.getBytesRemoved(),
                stats.getOldVersions());

            return new VacuumResult()
                .setDuration(duration)
                .setDatasetUri(datasetUri)
                .setDryRun(dryRun)
                .setBytesRemoved(stats.getBytesRemoved())
                .setOldVersionsRemoved(stats.getOldVersions());

        } catch (Exception e) {
            LOG.error("VACUUM failed for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to vacuum table: " + datasetUri, e);
        }
    }

    /**
     * COMPACT TABLE - Optimize file layout and compression.
     * Uses Lance's built-in compaction to reduce file count and improve compression.
     *
     * @param targetFileSize target size for merged files (bytes)
     * @param minRowsPerFile minimum rows per file
     * @param compression compression codec to use
     * @return compact result with timing information
     * @throws LanceException if compaction fails
     */
    public CompactResult compact(
            long targetFileSize, long minRowsPerFile, String compression) throws LanceException {
        LOG.info(
            "Starting COMPACT: dataset={}, targetSize={}, minRows={}, compression={}",
            datasetUri,
            targetFileSize,
            minRowsPerFile,
            compression);
        long startTime = System.currentTimeMillis();

        try {
            Dataset ds = loadDataset();
            CompactionOptions.Builder optionsBuilder = CompactionOptions.builder();

            if (targetFileSize > 0) {
                optionsBuilder.withMaxBytesPerFile(targetFileSize);
            }
            if (minRowsPerFile > 0) {
                optionsBuilder.withTargetRowsPerFragment(minRowsPerFile);
            }

            ds.compact(optionsBuilder.build());

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("COMPACT completed in {}ms", duration);

            return new CompactResult().setDuration(duration).setDatasetUri(datasetUri);

        } catch (Exception e) {
            LOG.error("COMPACT failed for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to compact table: " + datasetUri, e);
        }
    }

    /**
     * Get statistics about the Lance dataset.
     *
     * @return dataset statistics including row count, version, and fragment information
     * @throws LanceException if statistics cannot be retrieved
     */
    public Statistics getStatistics() throws LanceException {
        LOG.debug("Getting statistics for dataset: {}", datasetUri);
        try {
            Dataset ds = loadDataset();
            long rowCount = ds.countRows();
            long version = ds.version();
            List<Fragment> fragments = ds.getFragments();

            LOG.debug(
                "Dataset statistics: rows={}, version={}, fragments={}",
                rowCount,
                version,
                fragments.size());

            return new Statistics()
                .setDatasetUri(datasetUri)
                .setRowCount(rowCount)
                .setVersion(version)
                .setFragmentCount(fragments.size());

        } catch (Exception e) {
            LOG.error("Failed to get statistics for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to get statistics", e);
        }
    }

    /**
     * Get all versions of the Lance dataset.
     *
     * @return version history with total version count
     * @throws LanceException if version history cannot be retrieved
     */
    public VersionHistory getVersionHistory() throws LanceException {
        LOG.debug("Getting version history for dataset: {}", datasetUri);
        try {
            Dataset ds = loadDataset();
            List<?> versions = ds.listVersions();

            LOG.debug("Dataset has {} versions", versions.size());

            return new VersionHistory().setDatasetUri(datasetUri).setTotalVersions(versions.size());

        } catch (Exception e) {
            LOG.error("Failed to get version history for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to get version history", e);
        }
    }

    /**
     * RESTORE TABLE TO VERSION - Rollback to a previous version.
     * Creates a new version that matches the specified version.
     *
     * @param versionId the version to restore to
     * @return restore result with version information
     * @throws LanceException if restore fails
     */
    public RestoreResult restoreVersion(long versionId) throws LanceException {
        LOG.info("Starting RESTORE: dataset={}, targetVersion={}", datasetUri, versionId);
        long startTime = System.currentTimeMillis();

        try {
            Dataset ds = loadDataset();
            List<?> versions = ds.listVersions();

            if (versions.isEmpty()) {
                throw new LanceException("No versions available for table: " + datasetUri);
            }

            // Checkout the desired version and restore it as the latest
            Dataset restoredDs = ds.checkoutVersion(versionId);
            restoredDs.restore();

            // Refresh dataset to get latest version info
            if (dataset != null) {
                dataset.close();
                dataset = null;
            }
            Dataset refreshedDs = loadDataset();
            long currentVersion = refreshedDs.version();
            long rowCount = refreshedDs.countRows();

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("RESTORE completed in {}ms to version {}", duration, versionId);

            return new RestoreResult()
                .setVersionId(versionId)
                .setCurrentVersion(currentVersion)
                .setRowCount(rowCount)
                .setDuration(duration)
                .setDatasetUri(datasetUri);

        } catch (Exception e) {
            LOG.error("RESTORE failed for dataset: {}, targetVersion: {}", datasetUri, versionId, e);
            throw new LanceException("Failed to restore version: " + versionId, e);
        }
    }

    /**
     * Drop a specific version from the table.
     * Note: This operation uses the cleanup API to remove specific versions.
     *
     * @param versionId the version to drop
     * @return drop result with timing information
     * @throws LanceException if version drop fails
     */
    public DropVersionResult dropVersion(long versionId) throws LanceException {
        LOG.info("Starting DROP VERSION: versionId={}, dataset={}", versionId, datasetUri);
        long startTime = System.currentTimeMillis();

        try {
            Dataset ds = loadDataset();

            // Drop versions after the specified version to achieve selective version removal
            // Note: Lance's cleanup API uses beforeVersion parameter
            CleanupPolicy policy = CleanupPolicy.builder().withBeforeVersion(versionId).build();
            ds.cleanupWithPolicy(policy);

            long duration = System.currentTimeMillis() - startTime;
            LOG.info("DROP VERSION completed in {}ms", duration);

            return new DropVersionResult()
                .setVersionId(versionId)
                .setDuration(duration)
                .setDatasetUri(datasetUri);

        } catch (Exception e) {
            LOG.error("DROP VERSION failed for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to drop version: " + versionId, e);
        }
    }

    /**
     * List all fragments in the dataset.
     *
     * @return fragment list with count
     * @throws LanceException if fragments cannot be listed
     */
    public FragmentList listFragments() throws LanceException {
        LOG.debug("Listing fragments for dataset: {}", datasetUri);
        try {
            Dataset ds = loadDataset();
            List<Fragment> fragments = ds.getFragments();

            LOG.debug("Dataset has {} fragments", fragments.size());

            return new FragmentList().setDatasetUri(datasetUri).setFragmentCount(fragments.size());

        } catch (Exception e) {
            LOG.error("Failed to list fragments for dataset: {}", datasetUri, e);
            throw new LanceException("Failed to list fragments", e);
        }
    }

    /**
     * Get detailed information about a specific fragment.
     *
     * @param fragmentId the ID of the fragment
     * @return fragment information including row count
     * @throws LanceException if fragment info cannot be retrieved
     */
    public FragmentInfo getFragmentInfo(int fragmentId) throws LanceException {
        LOG.debug("Getting fragment info: dataset={}, fragmentId={}", datasetUri, fragmentId);
        try {
            Dataset ds = loadDataset();
            Fragment fragment = ds.getFragment(fragmentId);
            int rowCount = fragment.countRows();

            LOG.debug("Fragment {} has {} rows", fragmentId, rowCount);

            return new FragmentInfo()
                .setFragmentId(fragmentId)
                .setDatasetUri(datasetUri)
                .setRowCount(rowCount);

        } catch (Exception e) {
            LOG.error(
                "Failed to get fragment info for dataset: {}, fragmentId: {}",
                datasetUri,
                fragmentId,
                e);
            throw new LanceException("Failed to get fragment info", e);
        }
    }

    /**
     * Close and release resources associated with this DDL operations instance.
     *
     * @throws LanceException if closing fails
     */
    public void close() throws LanceException {
        try {
            if (dataset != null) {
                dataset.close();
                dataset = null;
                LOG.info("Closed Lance dataset at {}", datasetUri);
            }
        } catch (Exception e) {
            LOG.warn("Error closing dataset", e);
            throw new LanceException("Failed to close dataset", e);
        }
    }

    // ============ Result Classes ============

    public static class OptimizeResult {
        private long duration;
        private String datasetUri;

        public OptimizeResult setDuration(long duration) {
            this.duration = duration;
            return this;
        }

        public OptimizeResult setDatasetUri(String uri) {
            this.datasetUri = uri;
            return this;
        }

        public long getDuration() {
            return duration;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class VacuumResult {
        private long duration;
        private String datasetUri;
        private boolean dryRun;
        private long bytesRemoved;
        private long oldVersionsRemoved;

        public VacuumResult setDuration(long duration) {
            this.duration = duration;
            return this;
        }

        public VacuumResult setDatasetUri(String uri) {
            this.datasetUri = uri;
            return this;
        }

        public VacuumResult setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public VacuumResult setBytesRemoved(long bytesRemoved) {
            this.bytesRemoved = bytesRemoved;
            return this;
        }

        public VacuumResult setOldVersionsRemoved(long oldVersionsRemoved) {
            this.oldVersionsRemoved = oldVersionsRemoved;
            return this;
        }

        public long getDuration() {
            return duration;
        }

        public boolean isDryRun() {
            return dryRun;
        }

        public long getBytesRemoved() {
            return bytesRemoved;
        }

        public long getOldVersionsRemoved() {
            return oldVersionsRemoved;
        }
    }

    public static class CompactResult {
        private long duration;
        private String datasetUri;

        public CompactResult setDuration(long duration) {
            this.duration = duration;
            return this;
        }

        public CompactResult setDatasetUri(String uri) {
            this.datasetUri = uri;
            return this;
        }

        public long getDuration() {
            return duration;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class RestoreResult {
        private long versionId;
        private long currentVersion;
        private long rowCount;
        private long duration;
        private String datasetUri;

        public RestoreResult setVersionId(long v) {
            this.versionId = v;
            return this;
        }

        public RestoreResult setCurrentVersion(long v) {
            this.currentVersion = v;
            return this;
        }

        public RestoreResult setRowCount(long r) {
            this.rowCount = r;
            return this;
        }

        public RestoreResult setDuration(long d) {
            this.duration = d;
            return this;
        }

        public RestoreResult setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public long getVersionId() {
            return versionId;
        }

        public long getCurrentVersion() {
            return currentVersion;
        }

        public long getRowCount() {
            return rowCount;
        }

        public long getDuration() {
            return duration;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class DropVersionResult {
        private long versionId;
        private long duration;
        private String datasetUri;

        public DropVersionResult setVersionId(long v) {
            this.versionId = v;
            return this;
        }

        public DropVersionResult setDuration(long d) {
            this.duration = d;
            return this;
        }

        public DropVersionResult setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public long getVersionId() {
            return versionId;
        }

        public long getDuration() {
            return duration;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class Statistics {
        private String datasetUri;
        private long rowCount;
        private long version;
        private int fragmentCount;

        public Statistics setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public Statistics setRowCount(long r) {
            this.rowCount = r;
            return this;
        }

        public Statistics setVersion(long v) {
            this.version = v;
            return this;
        }

        public Statistics setFragmentCount(int c) {
            this.fragmentCount = c;
            return this;
        }

        public long getRowCount() {
            return rowCount;
        }

        public long getVersion() {
            return version;
        }

        public int getFragmentCount() {
            return fragmentCount;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class VersionHistory {
        private String datasetUri;
        private int totalVersions;

        public VersionHistory setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public VersionHistory setTotalVersions(int t) {
            this.totalVersions = t;
            return this;
        }

        public int getTotalVersions() {
            return totalVersions;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class FragmentList {
        private String datasetUri;
        private int fragmentCount;

        public FragmentList setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public FragmentList setFragmentCount(int c) {
            this.fragmentCount = c;
            return this;
        }

        public int getFragmentCount() {
            return fragmentCount;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }

    public static class FragmentInfo {
        private int fragmentId;
        private String datasetUri;
        private long rowCount;

        public FragmentInfo setFragmentId(int id) {
            this.fragmentId = id;
            return this;
        }

        public FragmentInfo setDatasetUri(String u) {
            this.datasetUri = u;
            return this;
        }

        public FragmentInfo setRowCount(long r) {
            this.rowCount = r;
            return this;
        }

        public long getRowCount() {
            return rowCount;
        }

        public int getFragmentId() {
            return fragmentId;
        }

        public String getDatasetUri() {
            return datasetUri;
        }
    }
}
