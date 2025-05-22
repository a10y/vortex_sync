mod lister;
mod rewrite;

use object_store::{ObjectStore, ObjectStoreScheme};
use serde::Deserialize;
use std::sync::Arc;
use url::Url;

use anyhow::Result;
use log::LevelFilter;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode};

/// Configuration for  the job.
#[derive(Debug, Deserialize)]
struct Config {
    /// URL of the source of the tree
    source_url: Url,
    /// URL to the destination node
    destination_url: Url,
    /// Number of files to process in parallel
    #[serde(default = "default_parallelism")]
    parallelism: usize,
}

/// Default parallelism is 4 concurrent files
fn default_parallelism() -> usize {
    4
}

#[tokio::main]
pub async fn main() {
    TermLogger::init(
        LevelFilter::Info,
        ConfigBuilder::new().build(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .expect("initialize logging");

    let config = envy::from_env::<Config>().expect("Failed to load config from environment");
    let src_store = make_store(&config.source_url).expect("Failed to create source store");
    let dst_store =
        make_store(&config.destination_url).expect("Failed to create destination store");

    // Create a state file path in the current directory
    let state_file = std::path::PathBuf::from(".vortex_sync_state.json");

    // Create a lister with the source store
    let mut lister = lister::Lister::new(
        src_store.clone(),
        None, // No prefix, list all files
        state_file,
    )
    .await
    .expect("Failed to create lister");

    log::info!(
        "Processing {} files ({} already processed, {} remaining) with parallelism {}",
        lister.total_files(),
        lister.processed_files(),
        lister.remaining_files(),
        config.parallelism
    );

    while let Some(file_path) = lister.next_file() {
        let dest_path = file_path.replace(".parquet", ".vortex");
        let src_store = src_store.clone();
        let dst_store = dst_store.clone();
        rewrite::rewrite_parquet_to_vortex(src_store, &file_path, dst_store, &dest_path)
            .await
            .expect(format!("file {}", file_path).as_str());
    }

    log::info!("All files processed. Exiting.");
}

// Make an object store from the provided URL.
fn make_store(url: &Url) -> Result<Arc<dyn ObjectStore>> {
    match ObjectStoreScheme::parse(url)? {
        (ObjectStoreScheme::AmazonS3, _) => Ok(Arc::new(
            AmazonS3Builder::from_env().with_url(url.clone()).build()?,
        )),
        (ObjectStoreScheme::MicrosoftAzure, _) => {
            // Build the azure connector
            Ok(Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_url(url.clone())
                    .build()?,
            ))
        }
        _ => {
            anyhow::bail!("Unsupported scheme for vortex_sync: {}", url.scheme());
        }
    }
}
