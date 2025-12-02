use clap::Parser;
use firebird_peregrine_falcon::Extractor;
use firebird_peregrine_falcon::ExtractorConfig;

#[derive(Parser)]
#[command(name = "firebird_peregrine_falcon")]
#[command(about = "Ultra-fast Firebird to Parquet extractor with parallel partitioning")]
struct Args {
    /// Firebird host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Firebird database path
    #[arg(long)]
    database: String,

    /// Output directory for Parquet files
    #[arg(long)]
    out_dir: String,

    /// Table name to extract
    #[arg(long)]
    table: String,

    /// Number of parallel workers (default: 2x CPU cores)
    #[arg(long)]
    parallelism: Option<usize>,

    /// Connection pool size (default: parallelism * 2)
    #[arg(long)]
    pool_size: Option<usize>,

    /// Firebird username
    #[arg(long, default_value = "SYSDBA")]
    user: String,

    /// Firebird password
    #[arg(long, default_value = "masterkey")]
    password: String,

    /// Use compression (default: false for speed)
    #[arg(long, default_value_t = false)]
    use_compression: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Adaptive Parallelism Logic
    let parallelism = if let Some(p) = args.parallelism {
        p
    } else {
        let available_mem = get_memory_limit();
        let safe_parallelism = calculate_safe_parallelism(available_mem);
        println!("Detected memory limit: {:.2} GB", available_mem as f64 / 1024.0 / 1024.0 / 1024.0);
        println!("Calculated safe parallelism: {} workers", safe_parallelism);
        safe_parallelism
    };

    let pool_size = args.pool_size.unwrap_or_else(|| parallelism * 2);

    println!("=== FIREBIRD PEREGRINE FALCON (ULTRA-FAST EXTRACTOR) ===");
    println!("Host: {}", args.host);
    println!("Database: {}", args.database);
    println!("Output: {}", args.out_dir);
    println!("Table: {}", args.table);
    println!("Parallelism: {} workers", parallelism);
    println!("Pool size: {} connections", pool_size);
    println!("Optimizations: Parallel PK partitioning, Multiple writers, Large batches, No ORDER BY");
    println!();

    let config = ExtractorConfig {
        host: args.host,
        database_path: args.database,
        out_dir: std::path::PathBuf::from(&args.out_dir),
        parallelism,
        pool_size,
        user: args.user,
        password: args.password,
        use_compression: args.use_compression,
    };

    let extractor = Extractor::new(config)?;
    let stats = extractor.extract_table(&args.table)?;

    println!();
    println!("=== EXTRACTION COMPLETE ===");
    println!("Rows: {}", stats.rows_extracted);
    println!("Duration: {:.1}s", stats.duration_secs);
    println!("File size: {:.2} MB", stats.file_size_mb);
    println!("Speed: {:.0} rows/s", stats.rows_extracted as f64 / stats.duration_secs);

    Ok(())
}


fn get_memory_limit() -> u64 {
    // 1. Try Cgroup v2
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Ok(bytes) = contents.trim().parse::<u64>() {
            if bytes > 0 && bytes < u64::MAX {
                return bytes;
            }
        }
    }

    // 2. Try Cgroup v1
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(bytes) = contents.trim().parse::<u64>() {
            if bytes > 0 && bytes < u64::MAX {
                return bytes;
            }
        }
    }

    // 3. Fallback to System Memory (sys-info)
    if let Ok(mem) = sys_info::mem_info() {
        return mem.total * 1024; // mem_info returns kB
    }

    // 4. Last resort fallback (assume 8GB)
    8 * 1024 * 1024 * 1024
}

fn calculate_safe_parallelism(available_bytes: u64) -> usize {
    // Reserve 20% for OS/Overhead
    let usable_bytes = (available_bytes as f64 * 0.8) as u64;
    
    // Estimate per-worker usage
    // Batch size (500k) * Row size (est 2KB to be safe) + Buffer overhead
    let estimated_worker_memory = 500_000 * 2048; // ~1GB per worker
    
    let max_workers = (usable_bytes / estimated_worker_memory) as usize;
    
    // Clamp between 1 and 2x CPU cores (don't go too crazy even if RAM is huge)
    let cpu_cores = num_cpus::get();
    let max_cpu_workers = cpu_cores * 4; // Allow more IO bound workers
    
    max_workers.clamp(1, max_cpu_workers)
}
