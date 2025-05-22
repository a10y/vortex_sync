use anyhow::Context;
use futures_util::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use std::sync::Arc;
use std::time::Instant;
use vortex::TryIntoArray;
use vortex::dtype::DType;
use vortex::dtype::arrow::FromArrowType;
use vortex::error::VortexError;
use vortex::file::VortexWriteOptions;
use vortex::stream::ArrayStreamAdapter;
use vortex_io::ObjectStoreWriter;

pub async fn rewrite_parquet_to_vortex(
    src_store: Arc<dyn ObjectStore>,
    parquet_source: &str,
    dst_store: Arc<dyn ObjectStore>,
    vortex_dest: &str,
) -> anyhow::Result<()> {
    // Read batches from the entire thing here.
    let src_path = Path::parse(parquet_source)?;
    let object_meta = src_store.head(&src_path).await?;
    let reader = ParquetObjectReader::new(src_store, src_path).with_file_size(object_meta.size);
    let parquet = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context("new stream builder")?
        .build()
        .context("ParquetRecordBatchStreamBuilder::build")?;

    let writer = ObjectStoreWriter::new(dst_store, Path::parse(vortex_dest)?).await?;
    let dtype = DType::from_arrow(parquet.schema().as_ref());
    log::info!("arrow schema: {}", parquet.schema());
    log::info!("vortex schema: {dtype}");
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
