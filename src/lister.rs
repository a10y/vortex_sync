//! List all files.

use object_store::ObjectStore;
use std::sync::Arc;

pub struct Lister {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}
