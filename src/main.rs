mod lister;
mod rewrite;

use futures_util::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreScheme};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use url::Url;

use anyhow::Result;
use arrow_array::RecordBatchReader;
use futures_util::Stream;
use log::LevelFilter;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode};
use vortex::dtype::arrow::FromArrowType;
use vortex::iter::ArrayIteratorExt;
use vortex::stream::ArrayStream;

/// Configuration for  the job.
#[derive(Debug, Deserialize)]
struct Config {
    /// URL of the source of the tree
    source_url: Url,
    /// URL to the destination node
    destination_url: Url,
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
        "Processing {} files ({} already processed, {} remaining)",
        lister.total_files(),
        lister.processed_files(),
        lister.remaining_files()
    );

    // Process files one by one
    while let Some(file_path) = lister.next_file() {
        log::info!("Processing file: {}", file_path);

        // Generate destination path by replacing .parquet with .vortex
        let dest_path = file_path.replace(".parquet", ".vortex");

        // Process the file
        match rewrite::rewrite_parquet_to_vortex(
            src_store.clone(),
            &file_path,
            dst_store.clone(),
            &dest_path,
        )
        .await
        {
            Ok(_) => {
                log::info!("Successfully processed file: {}", file_path);
            }
            Err(e) => {
                log::error!("Failed to process file {}: {}", file_path, e);
            }
        }
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
