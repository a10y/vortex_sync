use object_store::{ObjectStore, ObjectStoreScheme};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use url::Url;

use anyhow::Result;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;

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
    let config = envy::from_env::<Config>().expect("Failed to load config from environment");
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

// Start offset in the token tree instead.
impl Lister {
    fn new(store: Arc<dyn ObjectStore>, prefix: Option<Path>, state: ListerState) -> Result<Self> {
        Ok(Self {
            store,
            prefix,
            state,
        })
    }

    async fn restore(store: Arc<dyn ObjectStore>, prefix: Option<Path>, state: ListerState) -> Result<Self> {
        // Advance our internal iterator to the previously completed state node.
    }

    fn checkpoint(&self) -> ListerState {
        self.state.clone()
    }

    async fn next_item(&mut self) -> Option<String> {}
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
