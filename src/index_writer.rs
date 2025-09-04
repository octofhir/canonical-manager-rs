use std::sync::Arc;
use tantivy::{Index, IndexWriter};

/// Manages a single Tantivy IndexWriter to avoid repeated writer creation.
pub struct IndexWriterManager {
    index: Arc<Index>,
    writer: tokio::sync::Mutex<IndexWriter>,
}

impl IndexWriterManager {
    pub fn new(index: Arc<Index>, heap_size_bytes: usize) -> Self {
        let writer = index
            .writer(heap_size_bytes)
            .expect("failed to create tantivy IndexWriter");
        Self {
            index,
            writer: tokio::sync::Mutex::new(writer),
        }
    }

    pub fn index(&self) -> &Arc<Index> {
        &self.index
    }

    pub async fn commit(&self) -> Result<(), crate::error::FcmError> {
        let mut writer = self.writer.lock().await;
        writer
            .commit()
            .map(|_| ())
            .map_err(|e| crate::error::FcmError::Generic(format!("Tantivy error: {e}")))
    }
}
