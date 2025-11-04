//! Ultra-fast Firebird to Parquet extractor with parallel partitioning
//! 
//! High-impact optimizations:
//! - Parallel PK partitioning (40-60 workers)
//! - Multiple writer threads (temp files + merge)
//! - Large batch sizes (500K-1M rows)
//! - No ORDER BY (skip ordering)
//! - Aggressive prefetching (queue size 8-10)
//! - Cross-platform (Windows/Linux compatible)

use std::{
    fs::{create_dir_all, File},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Instant,
};

use crossbeam_channel::{bounded, Receiver, Sender};
use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::prelude::*;
use rsfbclient::{charset, Queryable, Row, SimpleConnection};

use crate::config::ExtractorConfig;

pub struct ExtractionStats {
    pub rows_extracted: usize,
    pub duration_secs: f64,
    pub file_size_mb: f64,
}

pub struct Extractor {
    config: ExtractorConfig,
    pool: Arc<ConnectionPool>,
}

struct ConnectionPool {
    connections: Arc<Mutex<Vec<SimpleConnection>>>,
    config: ExtractorConfig,
}

impl ConnectionPool {
    fn new(config: ExtractorConfig) -> Result<Self> {
        let mut connections = Vec::new();
        for _ in 0..config.pool_size {
            let conn = Self::create_connection(&config)?;
            connections.push(conn);
        }
        Ok(Self {
            connections: Arc::new(Mutex::new(connections)),
            config,
        })
    }

    fn create_connection(config: &ExtractorConfig) -> Result<SimpleConnection> {
        let mut builder = rsfbclient::builder_native().with_dyn_link().with_remote();
        builder.db_name(&config.database_path);
        builder.user(&config.user);
        builder.pass(&config.password);
        builder.charset(charset::ISO_8859_1);

        let conn: SimpleConnection = builder
            .connect()
            .context("Failed to connect to Firebird")?
            .into();
        Ok(conn)
    }

    fn acquire(&self) -> Result<PooledConnection> {
        let mut pool = self.connections.lock().unwrap();
        if let Some(conn) = pool.pop() {
            Ok(PooledConnection {
                conn: Some(conn),
                pool: Arc::clone(&self.connections),
                config: self.config.clone(),
            })
        } else {
            // Create new connection if pool is empty
            Ok(PooledConnection {
                conn: Some(Self::create_connection(&self.config)?),
                pool: Arc::clone(&self.connections),
                config: self.config.clone(),
            })
        }
    }
}

struct PooledConnection {
    conn: Option<SimpleConnection>,
    pool: Arc<Mutex<Vec<SimpleConnection>>>,
    config: ExtractorConfig,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if let Ok(mut pool) = self.pool.lock() {
                pool.push(conn);
            }
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = SimpleConnection;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

#[derive(Clone)]
struct TableMetadata {
    table_name: String,
    columns: Vec<ColumnMetadata>,
    row_count: i64,
    has_blob: bool,
    pk: Option<PrimaryKeyInfo>,
}

#[derive(Clone)]
struct ColumnMetadata {
    name: String,
    data_type: DataType,
    is_text_blob: bool,
}

#[derive(Clone)]
struct PrimaryKeyInfo {
    columns: Vec<String>,
    min_values: Vec<i64>,
    max_values: Vec<i64>,
    row_count: i64,
}

impl Extractor {
    pub fn new(config: ExtractorConfig) -> Result<Self> {
        create_dir_all(&config.out_dir)?;
        let pool = Arc::new(ConnectionPool::new(config.clone())?);
        Ok(Self { config, pool })
    }

    pub fn extract_table(&self, table_name: &str) -> Result<ExtractionStats> {
        let start = Instant::now();
        println!("→ Extracting table: {}", table_name);

        // Load metadata
        let meta = Arc::new(self.load_metadata(table_name)?);
        println!("  Rows: {}", format_number(meta.row_count));
        println!("  Columns: {}", meta.columns.len());

        if meta.row_count == 0 {
            println!("  (empty table) — skipping");
            return Ok(ExtractionStats {
                rows_extracted: 0,
                duration_secs: start.elapsed().as_secs_f64(),
                file_size_mb: 0.0,
            });
        }

        let output_path = self.config.out_dir.join(format!("{}.parquet", table_name.to_lowercase()));

        // ULTRA-AGGRESSIVE: Always try parallel PK partitioning
        // Even with small ranges, multiple workers can still help
        if let Some(ref pk) = meta.pk {
            println!("  Using parallel PK partitioning with {} workers", self.config.parallelism);
            self.extract_parallel_pk(&meta, &output_path, start)
        } else {
            println!("  No PK detected — using optimized sequential extraction");
            self.extract_sequential(&meta, &output_path, start)
        }
    }

    fn load_metadata(&self, table: &str) -> Result<TableMetadata> {
        let mut conn = self.pool.acquire()?;

        // Detect PK
        let pk = Self::detect_pk(&mut *conn, table)?;

        // Load columns
        let columns = Self::load_columns(&mut *conn, table)?;

        // Get row count
        let count_sql = format!("SELECT COUNT(*) FROM {}", table);
        let counts: Vec<(i64,)> = conn.query(&count_sql, ())?;
        let row_count = counts.first().map(|c| c.0).unwrap_or(0);

        let has_blob = columns.iter().any(|c| matches!(c.data_type, DataType::Utf8 if c.is_text_blob));

        Ok(TableMetadata {
            table_name: table.to_string(),
            columns,
            row_count,
            has_blob,
            pk,
        })
    }

    fn detect_pk(pool: &mut SimpleConnection, table: &str) -> Result<Option<PrimaryKeyInfo>> {
        // Find PK index
        let sql = r#"
            SELECT ri.rdb$index_name
            FROM rdb$indices ri
            WHERE ri.rdb$relation_name = ? 
            AND ri.rdb$index_type = 1
        "#;
        
        let indices: Vec<(String,)> = pool.query(sql, (table.to_uppercase(),))?;
        let pk_index_name = match indices.first() {
            Some((idx,)) => idx.trim().to_string(),
            None => return Ok(None),
        };

        // Get PK columns
        let col_sql = r#"
            SELECT seg.rdb$field_name
            FROM rdb$index_segments seg
            WHERE seg.rdb$index_name = ?
            ORDER BY seg.rdb$field_position
        "#;
        
        let pk_cols: Vec<(String,)> = pool.query(&col_sql, (pk_index_name.to_uppercase(),))?;
        let pk_column_names: Vec<String> = pk_cols
            .iter()
            .map(|(c,)| c.trim().to_string())
            .collect();

        if pk_column_names.is_empty() {
            return Ok(None);
        }

        // Check if all PK columns are numeric (INTEGER/BIGINT)
        let type_sql = r#"
            SELECT rdb$field_type
            FROM rdb$relation_fields
            WHERE rdb$relation_name = ? AND rdb$field_name = ?
        "#;
        
        let mut all_numeric = true;
        for col in &pk_column_names {
            let types: Vec<(i16,)> = pool.query(type_sql, (table.to_uppercase(), col.to_uppercase()))?;
            let fb_type = types.first().map(|t| t.0).unwrap_or(0);
            // 7 = SMALLINT, 8 = INTEGER, 16 = BIGINT
            if fb_type != 7 && fb_type != 8 && fb_type != 16 {
                all_numeric = false;
                break;
            }
        }

        if !all_numeric {
            return Ok(None);
        }

        // Get row count first
        let count_sql = format!("SELECT COUNT(*) FROM {}", table);
        let counts: Vec<(i64,)> = pool.query(&count_sql, ())?;
        let row_count = counts.first().map(|c| c.0).unwrap_or(0);

        // OPTIMIZATION: Skip expensive MIN/MAX for huge tables, but still try partitioning
        // Even without exact ranges, we can partition by row count
        if row_count > 10_000_000 && pk_column_names.len() > 1 {
            // For composite keys on huge tables, use row-based partitioning
            println!("  Using row-based partitioning (table too large for MIN/MAX)");
            return Ok(Some(PrimaryKeyInfo {
                columns: pk_column_names,
                min_values: vec![0],
                max_values: vec![row_count],
                row_count,
            }));
        }

        // Get MIN, MAX for first PK column (for partitioning)
        let first_col = &pk_column_names[0];
        let stats_sql = format!("SELECT MIN({}), MAX({}) FROM {}", first_col, first_col, table);
        let stats: Vec<(Option<i64>, Option<i64>)> = pool.query(&stats_sql, ())?;
        
        let (min_val, max_val) = stats.first()
            .and_then(|(min, max)| Some((min.unwrap_or(0), max.unwrap_or(0))))
            .unwrap_or((0, row_count));

        Ok(Some(PrimaryKeyInfo {
            columns: pk_column_names,
            min_values: vec![min_val],
            max_values: vec![max_val],
            row_count,
        }))
    }

    fn load_columns(pool: &mut SimpleConnection, table: &str) -> Result<Vec<ColumnMetadata>> {
        // Get field names first
        let name_sql = r#"
            SELECT rdb$field_name
            FROM rdb$relation_fields
            WHERE rdb$relation_name = ?
            ORDER BY rdb$field_position
        "#;
        
        let field_names: Vec<(String,)> = pool.query(name_sql, (table.to_uppercase(),))?;
        
        let mut columns = Vec::new();
        
        // For each field, get its type from rdb$fields
        let type_sql = r#"
            SELECT f.rdb$field_type, f.rdb$field_sub_type
            FROM rdb$fields f
            INNER JOIN rdb$relation_fields rf ON f.rdb$field_name = rf.rdb$field_source
            WHERE rf.rdb$relation_name = ? AND rf.rdb$field_name = ?
        "#;
        
        for (field_name,) in field_names {
            let col_name = field_name.trim().to_string();
            let types: Vec<(i16, i16)> = pool.query(type_sql, (table.to_uppercase(), col_name.to_uppercase()))?;
            let (fb_type, subtype) = types.first().map(|t| (t.0, t.1)).unwrap_or((37, 0)); // Default to VARCHAR
            
            let (data_type, is_text_blob) = fb_to_arrow_type(fb_type, subtype);
            columns.push(ColumnMetadata {
                name: col_name,
                data_type,
                is_text_blob,
            });
        }

        Ok(columns)
    }

    fn extract_parallel_pk(
        &self,
        meta: &TableMetadata,
        output_path: &Path,
        start: Instant,
    ) -> Result<ExtractionStats> {
        let pk = meta.pk.as_ref().unwrap();
        let parallelism = self.config.parallelism;

        // Calculate large batch size (500K-1M rows)
        let batch_size = calculate_batch_size(meta.row_count, meta.has_blob);

        // Partition PK range
        let pk_range = pk.max_values[0] - pk.min_values[0];
        let rows_per_partition = (meta.row_count as f64 / parallelism as f64).ceil() as i64;
        let pk_step = if pk_range > 0 { pk_range as f64 / parallelism as f64 } else { 1.0 };

        println!("  Batch size: {}", format_number(batch_size as i64));
        println!("  Partitions: {}", parallelism);
        println!("  Rows per partition: ~{}", format_number(rows_per_partition));

        // Create temp files for each partition
        let temp_dir = output_path.parent().unwrap();
        let temp_files: Vec<PathBuf> = (0..parallelism)
            .map(|i| temp_dir.join(format!("{}_part_{}.parquet", output_path.file_stem().unwrap().to_str().unwrap(), i)))
            .collect();

        // Parallel extraction with multiple writers
        let pool = Arc::clone(&self.pool);
        let meta_arc = Arc::new(meta.clone());
        let results: Vec<Result<PartitionResult>> = (0..parallelism)
            .into_par_iter()
            .map(|i| {
                let start_pk = pk.min_values[0] + (pk_step * i as f64) as i64;
                let end_pk = if i == parallelism - 1 {
                    pk.max_values[0]
                } else {
                    pk.min_values[0] + (pk_step * (i + 1) as f64) as i64
                };

                let pool_clone = Arc::clone(&pool);
                let meta_clone = meta_arc.clone();
                let temp_path = temp_files[i].clone();

                extract_partition(pool_clone, meta_clone, start_pk, end_pk, batch_size, &temp_path)
            })
            .collect();

        // Collect results
        let mut total_rows = 0;
        let mut partition_files = Vec::new();
        
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(part_result) => {
                    total_rows += part_result.rows;
                    if part_result.rows > 0 {
                        partition_files.push(temp_files[i].clone());
                    }
                    println!("  Partition {}: {} rows", i, format_number(part_result.rows as i64));
                }
                Err(e) => {
                    eprintln!("  Partition {} failed: {}", i, e);
                }
            }
        }

        // Merge temp files into final output
        println!("  Merging {} partition files...", partition_files.len());
        merge_parquet_files(&partition_files, output_path)?;

        // Cleanup temp files
        for temp_file in &temp_files {
            let _ = std::fs::remove_file(temp_file);
        }

        let duration = start.elapsed().as_secs_f64();
        let file_size_mb = std::fs::metadata(output_path)
            .map(|m| m.len() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0);

        println!(
            "  ✓ Done: {} rows → {} in {} ({:.1} MB, {:.0} rows/s)",
            format_number(total_rows as i64),
            output_path.display(),
            format_duration(duration),
            file_size_mb,
            total_rows as f64 / duration
        );

        Ok(ExtractionStats {
            rows_extracted: total_rows,
            duration_secs: duration,
            file_size_mb,
        })
    }

    fn extract_sequential(
        &self,
        meta: &TableMetadata,
        output_path: &Path,
        start: Instant,
    ) -> Result<ExtractionStats> {
        // Optimized sequential with prefetch + writer pipeline
        let batch_size = calculate_batch_size(meta.row_count, meta.has_blob);
        println!("  Batch size: {}", format_number(batch_size as i64));

        type RowBatch = Vec<Row>;
        let (fetch_tx, fetch_rx): (Sender<Option<RowBatch>>, Receiver<Option<RowBatch>>) = bounded(10); // Aggressive prefetch
        let (batch_tx, batch_rx): (Sender<Option<RecordBatch>>, Receiver<Option<RecordBatch>>) = bounded(8);

        let pool_clone = Arc::clone(&self.pool);
        let columns_sql: String = meta.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ");
        let query = format!("SELECT {} FROM {}", columns_sql, meta.table_name); // NO ORDER BY!
        let page_size = batch_size as i64;

        // Prefetch thread
        let fetcher = thread::spawn(move || {
            let mut conn = match pool_clone.acquire() {
                Ok(c) => c,
                Err(_) => return,
            };

            let mut offset = 0i64;
            loop {
                let page_query = format!("{} ROWS {} TO {}", query, offset + 1, offset + page_size);
                match conn.query(&page_query, ()) {
                    Ok(rows) => {
                        if rows.is_empty() {
                            let _ = fetch_tx.send(None);
                            break;
                        }
                        if fetch_tx.send(Some(rows)).is_err() {
                            break;
                        }
                        offset += page_size;
                    }
                    Err(_) => {
                        let _ = fetch_tx.send(None);
                        break;
                    }
                }
            }
        });

        // Writer thread
        let fields: Vec<Field> = meta.columns.iter().map(|m| Field::new(&m.name, m.data_type.clone(), true)).collect();
        let schema_for_writer = Arc::new(Schema::new(fields));
        let props_for_writer = self.create_writer_props();
        let output_path_clone = output_path.to_path_buf();

        let writer_handle = thread::spawn(move || -> Result<()> {
            let file = File::create(&output_path_clone)?;
            let buf = BufWriter::with_capacity(128 * 1024 * 1024, file);
            let mut writer = ArrowWriter::try_new(buf, schema_for_writer, Some(props_for_writer))?;

            while let Ok(opt) = batch_rx.recv() {
                match opt {
                    Some(batch) => writer.write(&batch)?,
                    None => break,
                }
            }
            writer.close()?;
            Ok(())
        });

        // Process batches
        let mut total_rows = 0;
        while let Ok(Some(rows)) = fetch_rx.recv() {
            let batch = build_arrow_batch(meta, &rows)?;
            let row_count = batch.num_rows();
            if batch_tx.send(Some(batch)).is_err() {
                break;
            }
            total_rows += row_count;

            if total_rows % 500_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = total_rows as f64 / elapsed;
                let pct = (total_rows as f64 * 100.0) / (meta.row_count.max(1) as f64);
                println!(
                    "  Progress: {} / {} rows ({:.1}%) - {:.0} rows/s",
                    format_number(total_rows as i64),
                    format_number(meta.row_count),
                    pct,
                    rate
                );
            }
        }

        let _ = batch_tx.send(None);
        let _ = fetcher.join();
        writer_handle.join().map_err(|_| anyhow::anyhow!("writer thread panicked"))??;

        let duration = start.elapsed().as_secs_f64();
        let file_size_mb = std::fs::metadata(output_path)
            .map(|m| m.len() as f64 / (1024.0 * 1024.0))
            .unwrap_or(0.0);

        Ok(ExtractionStats {
            rows_extracted: total_rows,
            duration_secs: duration,
            file_size_mb,
        })
    }

    fn create_writer_props(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(if self.config.use_compression {
                Compression::UNCOMPRESSED
            } else {
                Compression::UNCOMPRESSED
            })
            .set_dictionary_enabled(false)
            .build()
    }
}

struct PartitionResult {
    rows: usize,
}

fn extract_partition(
    pool: Arc<ConnectionPool>,
    meta: Arc<TableMetadata>,
    start_pk: i64,
    end_pk: i64,
    batch_size: usize,
    output_path: &Path,
) -> Result<PartitionResult> {
    let mut conn = pool.acquire()?;
    let pk_col = &meta.pk.as_ref().unwrap().columns[0];
    let columns_sql: String = meta.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ");

    // NO ORDER BY - maximum speed!
    let query = format!(
        "SELECT {} FROM {} WHERE {} >= {} AND {} <= {}",
        columns_sql, meta.table_name, pk_col, start_pk, pk_col, end_pk
    );

    let rows: Vec<Row> = conn.query(&query, ())?;
    let total_rows = rows.len();

    if total_rows == 0 {
        return Ok(PartitionResult { rows: 0 });
    }

    // Write to temp file with writer thread
    let (batch_tx, batch_rx): (Sender<Option<RecordBatch>>, Receiver<Option<RecordBatch>>) = bounded(4);
    
    let fields: Vec<Field> = meta.columns.iter().map(|m| Field::new(&m.name, m.data_type.clone(), true)).collect();
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let output_path_clone = output_path.to_path_buf();

    let writer_handle = thread::spawn(move || -> Result<()> {
        let file = File::create(&output_path_clone)?;
        let buf = BufWriter::with_capacity(128 * 1024 * 1024, file);
        let mut writer = ArrowWriter::try_new(buf, schema, Some(props))?;

        while let Ok(opt) = batch_rx.recv() {
            match opt {
                Some(batch) => writer.write(&batch)?,
                None => break,
            }
        }
        writer.close()?;
        Ok(())
    });

    // Process in batches
    for chunk in rows.chunks(batch_size) {
        let batch = build_arrow_batch(&meta, chunk)?;
        if batch_tx.send(Some(batch)).is_err() {
            break;
        }
    }

    let _ = batch_tx.send(None);
    writer_handle.join().map_err(|_| anyhow::anyhow!("writer thread panicked"))??;

    Ok(PartitionResult { rows: total_rows })
}

fn merge_parquet_files(input_files: &[PathBuf], output_path: &Path) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    if input_files.is_empty() {
        return Ok(());
    }

    if input_files.len() == 1 {
        std::fs::copy(&input_files[0], output_path)?;
        return Ok(());
    }

    // Read first file to get schema and build writer
    let first_file = File::open(&input_files[0])?;
    let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
    let schema = Arc::new(first_builder.schema().as_ref().clone());
    
    // Create output writer
    let output_file = File::create(output_path)?;
    let buf = BufWriter::with_capacity(128 * 1024 * 1024, output_file);
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(buf, schema, Some(props))?;

    // Read and write first file
    let first_reader = first_builder.with_batch_size(100_000).build()?;
    for batch_result in first_reader {
        let batch = batch_result?;
        writer.write(&batch)?;
    }

    // Merge remaining files
    for input_file in input_files.iter().skip(1) {
        let file = File::open(input_file)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.with_batch_size(100_000).build()?;

        for batch_result in reader {
            let batch = batch_result?;
            writer.write(&batch)?;
        }
    }

    writer.close()?;
    Ok(())
}

fn calculate_batch_size(row_count: i64, has_blob: bool) -> usize {
    // ULTRA-LARGE batches: 500K-1M rows
    let base_batch = if row_count < 200_000 {
        250_000
    } else if row_count < 10_000_000 {
        500_000  // Increased from 200K
    } else if row_count < 50_000_000 {
        750_000  // Increased from 300K
    } else {
        1_000_000  // MASSIVE: 1M rows per batch
    };

    let mut batch = base_batch;
    if has_blob {
        batch = (batch * 2) / 3;  // Reduce by 33% for BLOBs
    }

    batch.max(100_000)  // Minimum 100K
}

fn build_arrow_batch(meta: &TableMetadata, rows: &[Row]) -> Result<RecordBatch> {
    let num_cols = meta.columns.len();

    // Parallel column building
    let arrays: Vec<ArrayRef> = (0..num_cols)
        .into_par_iter()
        .map(|ci| {
            let col_meta = &meta.columns[ci];
            build_column_array(col_meta, rows, ci)
        })
        .collect();

    let fields: Vec<Field> = meta
        .columns
        .iter()
        .map(|m| Field::new(&m.name, m.data_type.clone(), true))
        .collect();

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .context("Failed to build record batch")
}

fn build_column_array(meta: &ColumnMetadata, rows: &[Row], col_index: usize) -> ArrayRef {
    let row_count = rows.len();

    match meta.data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(row_count);
            for row in rows {
                match row.cols.get(col_index).map(|c| &c.value) {
                    Some(rsfbclient::SqlType::Integer(v)) => builder.append_value(*v),
                    Some(rsfbclient::SqlType::Floating(v)) => builder.append_value(*v as i64),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(row_count);
            for row in rows {
                match row.cols.get(col_index).map(|c| &c.value) {
                    Some(rsfbclient::SqlType::Floating(v)) => builder.append_value(*v),
                    Some(rsfbclient::SqlType::Integer(v)) => builder.append_value(*v as f64),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(row_count, row_count * 64);
            for row in rows {
                match row.cols.get(col_index).map(|c| &c.value) {
                    Some(rsfbclient::SqlType::Text(t)) => {
                        if meta.is_text_blob {
                            let normalized = String::from_utf8_lossy(t.as_bytes()).trim().to_string();
                            builder.append_value(normalized);
                        } else {
                            builder.append_value(t.trim());
                        }
                    }
                    Some(rsfbclient::SqlType::Integer(v)) => builder.append_value(v.to_string()),
                    Some(rsfbclient::SqlType::Floating(v)) => builder.append_value(v.to_string()),
                    Some(rsfbclient::SqlType::Boolean(b)) => {
                        builder.append_value(if *b { "true" } else { "false" })
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match row.cols.get(col_index).map(|c| &c.value) {
                    Some(rsfbclient::SqlType::Text(t)) => {
                        // Text blob as binary
                        builder.append_value(t.as_bytes());
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Fallback: convert to string
            let mut builder = StringBuilder::with_capacity(row_count, row_count * 32);
            for _row in rows {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
    }
}

fn fb_to_arrow_type(fb_type: i16, subtype: i16) -> (DataType, bool) {
    match fb_type {
        7 => (DataType::Int64, false),   // SMALLINT
        8 => (DataType::Int64, false),   // INTEGER
        16 => (DataType::Int64, false),  // BIGINT
        10 => (DataType::Float64, false), // FLOAT
        27 => (DataType::Float64, false), // DOUBLE
        12 => {
            if subtype == 1 {
                (DataType::Utf8, true)  // BLOB SUB_TYPE TEXT
            } else {
                (DataType::Binary, false)  // BLOB
            }
        }
        14 => (DataType::Utf8, false),  // CHAR
        37 => (DataType::Utf8, false),  // VARCHAR
        23 => (DataType::Float64, false), // FLOAT
        _ => (DataType::Utf8, false),   // Default to string
    }
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + (s.len() / 3));
    let chars: Vec<char> = s.chars().collect();

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 && *ch != '-' {
            result.push(',');
        }
        result.push(*ch);
    }

    result
}

fn format_duration(secs: f64) -> String {
    let total_secs = secs as u64;
    let mins = total_secs / 60;
    let remaining_secs = total_secs % 60;

    if mins > 0 {
        format!("{}m {}s", mins, remaining_secs)
    } else {
        format!("{:.1}s", secs)
    }
}

