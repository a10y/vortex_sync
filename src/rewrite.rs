use futures_util::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;
use std::time::Instant;
use vortex::dtype::arrow::FromArrowType;
use vortex::dtype::DType;
use vortex::error::VortexError;
use vortex::file::VortexWriteOptions;
use vortex::stream::{ArrayStream, ArrayStreamAdapter};
use vortex::TryIntoArray;
use vortex_io::ObjectStoreWriter;

pub async fn rewrite_parquet_to_vortex(
    src_store: Arc<dyn ObjectStore>,
    parquet_source: &str,
    dst_store: Arc<dyn ObjectStore>,
    vortex_dest: &str,
) -> anyhow::Result<()> {
    // Read batches from the entire thing here.
    let reader = ParquetObjectReader::new(src_store, Path::parse(parquet_source)?);
    let parquet = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;

    let writer = ObjectStoreWriter::new(dst_store, Path::parse(vortex_dest)?).await?;
    let dtype = DType::from_arrow(parquet.schema().as_ref());
    let vortex_stream = parquet
        .map(|record_batch| {
            record_batch
                .map_err(VortexError::from)
                .and_then(|rb| rb.try_into_array())
        })
        .boxed();

    log::info!("begin writing {} to {}", parquet_source, vortex_dest);
    let duration = Instant::now();
    VortexWriteOptions::default()
        .write(writer, ArrayStreamAdapter::new(dtype, vortex_stream))
        .await?;
    let duration = duration.elapsed();
    log::info!("finished writing {} in {:?}", parquet_source, duration);

    Ok(())
}
