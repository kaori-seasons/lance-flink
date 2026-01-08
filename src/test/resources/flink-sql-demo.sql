-- ================================================================================
-- Flink SQL Lance Connector Test Script
-- ================================================================================
-- This script demonstrates how to use Flink SQL to operate Lance vector datasets
-- Usage: Execute the following statements in Flink SQL Client
-- ================================================================================

-- ================================================================================
-- 1. Create Basic Vector Table
-- ================================================================================

-- Create a simple vector storage table
CREATE TABLE lance_vectors (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/vectors',
    'write.batch-size' = '1024',
    'write.mode' = 'overwrite'
);

-- Insert test data
INSERT INTO lance_vectors VALUES
    (1, 'Hello World', ARRAY[0.1, 0.2, 0.3, 0.4]),
    (2, 'Machine Learning', ARRAY[0.2, 0.3, 0.4, 0.5]),
    (3, 'Deep Learning', ARRAY[0.3, 0.4, 0.5, 0.6]),
    (4, 'Neural Networks', ARRAY[0.4, 0.5, 0.6, 0.7]),
    (5, 'Vector Database', ARRAY[0.5, 0.6, 0.7, 0.8]);

-- Query data
SELECT * FROM lance_vectors;

-- ================================================================================
-- 2. Create Table with Vector Index Configuration
-- ================================================================================

-- Create vector table with IVF_PQ index
CREATE TABLE document_embeddings (
    doc_id BIGINT COMMENT 'Document ID',
    title STRING COMMENT 'Document title',
    content STRING COMMENT 'Document content',
    embedding ARRAY<FLOAT> COMMENT '768-dim document vector',
    category STRING COMMENT 'Document category',
    create_time TIMESTAMP(3) COMMENT 'Creation time'
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/documents',
    -- Write configuration
    'write.batch-size' = '2048',
    'write.mode' = 'append',
    'write.max-rows-per-file' = '100000',
    -- Vector index configuration (IVF_PQ)
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.num-sub-vectors' = '16',
    'index.num-bits' = '8',
    -- Vector search configuration
    'vector.column' = 'embedding',
    'vector.metric' = 'COSINE',
    'vector.nprobes' = '20'
);

-- ================================================================================
-- 3. Different Index Type Examples
-- ================================================================================

-- IVF_PQ index (recommended, balances accuracy and speed)
CREATE TABLE vectors_ivf_pq (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_pq',
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.num-sub-vectors' = '16',
    'index.num-bits' = '8',
    'vector.metric' = 'L2'
);

-- IVF_HNSW index (high accuracy)
CREATE TABLE vectors_ivf_hnsw (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_hnsw',
    'index.type' = 'IVF_HNSW',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.max-level' = '7',
    'index.m' = '16',
    'index.ef-construction' = '100',
    'vector.metric' = 'COSINE'
);

-- IVF_FLAT index (highest accuracy, suitable for small datasets)
CREATE TABLE vectors_ivf_flat (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_flat',
    'index.type' = 'IVF_FLAT',
    'index.column' = 'embedding',
    'index.num-partitions' = '64',
    'vector.metric' = 'DOT'
);

-- ================================================================================
-- 4. Create and Use Lance Catalog
-- ================================================================================

-- Create Lance Catalog
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/tmp/lance/warehouse',
    'default-database' = 'default'
);

-- Switch to Lance Catalog
USE CATALOG lance_catalog;

-- Create database
CREATE DATABASE IF NOT EXISTS vector_db;

-- Use database
USE vector_db;

-- Create table in Catalog
CREATE TABLE embeddings (
    id BIGINT,
    text STRING,
    vector ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/warehouse/vector_db/embeddings'
);

-- List databases
SHOW DATABASES;

-- List tables
SHOW TABLES;

-- ================================================================================
-- 5. Data Operation Examples
-- ================================================================================

-- Batch insert data
INSERT INTO document_embeddings VALUES
    (1, 'Apache Flink Getting Started', 'Flink is a distributed stream processing framework...',
     ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], 'tutorial', TIMESTAMP '2024-01-01 10:00:00'),
    (2, 'Flink SQL Deep Dive', 'Using SQL syntax for stream processing...',
     ARRAY[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 'tutorial', TIMESTAMP '2024-01-02 11:00:00'),
    (3, 'Vector Database Comparison', 'Comparison analysis of common vector databases...',
     ARRAY[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], 'analysis', TIMESTAMP '2024-01-03 12:00:00'),
    (4, 'Lance Format Deep Dive', 'Lance is an efficient vector storage format...',
     ARRAY[0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1], 'format', TIMESTAMP '2024-01-04 13:00:00'),
    (5, 'RAG System in Practice', 'Knowledge base Q&A system based on vector search...',
     ARRAY[0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2], 'practice', TIMESTAMP '2024-01-05 14:00:00');

-- Simple query
SELECT doc_id, title, category FROM document_embeddings;

-- Conditional query
SELECT doc_id, title, category, create_time
FROM document_embeddings
WHERE category = 'tutorial'
ORDER BY create_time DESC;

-- Aggregation query
SELECT category, COUNT(*) as doc_count
FROM document_embeddings
GROUP BY category
ORDER BY doc_count DESC;

-- Time range query
SELECT doc_id, title, create_time
FROM document_embeddings
WHERE create_time >= TIMESTAMP '2024-01-02 00:00:00'
  AND create_time < TIMESTAMP '2024-01-05 00:00:00';

-- ================================================================================
-- 6. Streaming Processing Example
-- ================================================================================

-- Create data generator table (simulating real-time data)
CREATE TABLE realtime_events (
    event_id BIGINT,
    event_type STRING,
    embedding ARRAY<FLOAT>,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',
    'fields.event_id.kind' = 'sequence',
    'fields.event_id.start' = '1',
    'fields.event_id.end' = '1000000',
    'fields.event_type.length' = '10'
);

-- Create Lance Sink table
CREATE TABLE lance_events (
    event_id BIGINT,
    event_type STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/events',
    'write.batch-size' = '1000',
    'write.mode' = 'append'
);

-- Streaming write
INSERT INTO lance_events
SELECT event_id, event_type, ARRAY[RAND(), RAND(), RAND(), RAND()] as embedding
FROM realtime_events;

-- ================================================================================
-- 7. Vector Search UDF Usage Example
-- ================================================================================

-- Register vector search UDF
CREATE FUNCTION vector_search AS 'org.apache.flink.connector.lance.table.LanceVectorSearchFunction';

-- Use UDF for vector search (example syntax)
-- SELECT * FROM vector_search(
--     '/tmp/lance/documents',  -- Dataset path
--     'embedding',             -- Vector column name
--     ARRAY[0.1, 0.2, 0.3],   -- Query vector
--     10,                      -- Return count
--     'L2'                     -- Distance metric
-- );

-- ================================================================================
-- 8. Common Management Commands
-- ================================================================================

-- View table structure
DESCRIBE lance_vectors;

-- View table details
SHOW CREATE TABLE lance_vectors;

-- Drop table
DROP TABLE IF EXISTS lance_vectors;

-- Drop database
DROP DATABASE IF EXISTS vector_db CASCADE;

-- Drop Catalog
DROP CATALOG IF EXISTS lance_catalog;

-- ================================================================================
-- Configuration Options Reference
-- ================================================================================
--
-- Basic configuration:
--   connector           = 'lance'        -- Connector type (required)
--   path                = '/path'        -- Dataset path (required)
--
-- Read configuration:
--   read.batch-size     = 1024           -- Read batch size
--   read.columns        = 'col1,col2'    -- Columns to read (comma separated)
--   read.filter         = 'id > 10'      -- Filter condition
--
-- Write configuration:
--   write.batch-size    = 1024           -- Write batch size
--   write.mode          = 'append'       -- Write mode: append/overwrite
--   write.max-rows-per-file = 1000000    -- Max rows per file
--
-- Index configuration:
--   index.type          = 'IVF_PQ'       -- Index type: IVF_PQ/IVF_HNSW/IVF_FLAT
--   index.column        = 'embedding'    -- Index column name
--   index.num-partitions = 256           -- IVF partition count
--   index.num-sub-vectors = 16           -- PQ sub-vector count
--   index.num-bits      = 8              -- Quantization bits
--   index.max-level     = 7              -- HNSW max level
--   index.m             = 16             -- HNSW connection count
--   index.ef-construction = 100          -- HNSW construction ef parameter
--
-- Vector search configuration:
--   vector.column       = 'embedding'    -- Vector column name
--   vector.metric       = 'L2'           -- Distance metric: L2/COSINE/DOT
--   vector.nprobes      = 20             -- Search probe count
--   vector.ef           = 100            -- HNSW search ef parameter
--   vector.refine-factor = null          -- Refine factor
--
-- ================================================================================
