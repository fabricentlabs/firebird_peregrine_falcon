use std::path::PathBuf;

#[derive(Clone)]
pub struct ExtractorConfig {
    pub database_path: String,
    pub out_dir: PathBuf,
    pub parallelism: usize,
    pub pool_size: usize,
    pub user: String,
    pub password: String,
    pub use_compression: bool,
}

