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
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
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

    // Create a channel for sending files to worker tasks
    let (tx, mut rx) = mpsc::channel(config.parallelism * 2);

    // Create a semaphore to limit concurrent tasks
    let semaphore = Arc::new(Semaphore::new(config.parallelism));

    // Spawn a task to send files from the lister to the channel
    let sender_task = tokio::spawn(async move {
        while let Some(file_path) = lister.next_file() {
            // Generate destination path by replacing .parquet with .vortex
            let dest_path = file_path.replace(".parquet", ".vortex");

            if tx.send((file_path, dest_path)).await.is_err() {
                // Channel closed, receiver is gone
                break;
            }
        }
    });

    // Create a JoinSet to track all worker tasks
    let mut tasks = JoinSet::new();

    // Process files in parallel
    loop {
        tokio::select! {
            // Wait for a file to process
            Some((file_path, dest_path)) = rx.recv() => {
                // Clone the necessary resources for the task
                let src_store = src_store.clone();
                let dst_store = dst_store.clone();
                let semaphore = semaphore.clone();

                // Spawn a task to process the file
                tasks.spawn(async move {
                    // Acquire a permit from the semaphore
                    let _permit = semaphore.acquire().await.unwrap();

                    log::info!("Processing file: {}", file_path);

                    // Process the file
                    let result = rewrite::rewrite_parquet_to_vortex(
                        src_store,
                        &file_path,
                        dst_store,
                        &dest_path,
                    ).await;

                    match result {
                        Ok(_) => {
                            log::info!("Successfully processed file: {}", file_path);
                        }
                        Err(e) => {
                            log::error!("Failed to process file {}: {}", file_path, e);
                        }
                    }

                    // Return the file path for logging purposes
                    file_path
                });
            }
            // Wait for a task to complete
            Some(result) = tasks.join_next() => {
                match result {
                    Ok(file_path) => {
                        log::debug!("Task completed for file: {}", file_path);
                    }
                    Err(e) => {
                        log::error!("Task panicked: {}", e);
                    }
                }
            }
            // Exit when both the channel is closed and all tasks are done
            else => {
                break;
            }
        }
    }

    // Wait for any remaining tasks to complete
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(file_path) => {
                log::debug!("Task completed for file: {}", file_path);
            }
            Err(e) => {
                log::error!("Task panicked: {}", e);
            }
        }
    }

    // Wait for the sender task to complete
    if let Err(e) = sender_task.await {
        log::error!("Sender task panicked: {}", e);
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
