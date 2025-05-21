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
    
    // Restore state, enumerate all jobs, dispatch and wait for them all
}

struct Lister {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    state: ListerState,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct ListerState {
    last_completed: Option<String>,
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
