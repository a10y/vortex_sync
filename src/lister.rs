//! Stateful file lister using a JSON file.

use anyhow::Result;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use log::{info, warn};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectMeta};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// A stateful file lister that keeps track of files to process
/// and persists its state to a JSON file.
pub struct Lister {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    state_file: PathBuf,
    state: ListerState,
}

/// The state of the lister, which is serialized to/from JSON.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ListerState {
    /// All files to process
    files: Vec<String>,
    /// Index of the next file to process
    next_index: usize,
}

impl Lister {
    /// Create a new lister with the given object store and optional prefix.
    pub async fn new(store: Arc<dyn ObjectStore>, prefix: Option<Path>, state_file: PathBuf) -> Result<Self> {
        // Try to load existing state
        let state = if state_file.exists() {
            info!("Found state file at {:?}, loading state", state_file);
            let state_json = fs::read_to_string(&state_file)?;
            serde_json::from_str(&state_json)?
        } else {
            info!("No state file found at {:?}, initializing new state", state_file);
            ListerState::default()
        };

        let mut lister = Self {
            store,
            prefix,
            state_file,
            state,
        };

        // If state is empty, initialize it
        if lister.state.files.is_empty() {
            lister.initialize().await?;
        }

        Ok(lister)
    }

    /// Initialize the lister by listing all Parquet files and saving the state.
    async fn initialize(&mut self) -> Result<()> {
        info!("Initializing lister by listing all Parquet files");

        // List all objects with the given prefix
        let list_stream = self.store.list(self.prefix.as_ref().map(|p| p));

        // Collect all Parquet files
        let mut files = Vec::new();
        tokio::pin!(list_stream);

        while let Some(meta_result) = list_stream.next().await {
            match meta_result {
                Ok(meta) => {
                    let path = meta.location.to_string();
                    if path.ends_with(".parquet") {
                        files.push(path);
                    }
                }
                Err(e) => {
                    warn!("Error listing objects: {}", e);
                }
            }
        }

        // Sort the files
        files.sort();

        info!("Found {} Parquet files", files.len());

        // Update state
        self.state.files = files;
        self.state.next_index = 0;

        // Save state
        self.save_state()?;

        Ok(())
    }

    /// Save the current state to the state file.
    fn save_state(&self) -> Result<()> {
        let state_json = serde_json::to_string(&self.state)?;
        fs::write(&self.state_file, state_json)?;
        info!("Saved state to {:?}", self.state_file);
        Ok(())
    }

    /// Get the next file to process, if any.
    pub fn next_file(&mut self) -> Option<String> {
        if self.state.next_index >= self.state.files.len() {
            return None;
        }

        let file = self.state.files[self.state.next_index].clone();
        self.state.next_index += 1;

        // Save state after advancing
        if let Err(e) = self.save_state() {
            warn!("Failed to save state: {}", e);
        }

        Some(file)
    }

    /// Get the total number of files.
    pub fn total_files(&self) -> usize {
        self.state.files.len()
    }

    /// Get the number of files processed so far.
    pub fn processed_files(&self) -> usize {
        self.state.next_index
    }

    /// Get the number of files remaining to process.
    pub fn remaining_files(&self) -> usize {
        self.state.files.len().saturating_sub(self.state.next_index)
    }
}
