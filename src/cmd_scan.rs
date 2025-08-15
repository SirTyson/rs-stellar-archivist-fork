//! Scan command implementation using the simplified pipeline

use crate::pipeline::{Pipeline, PipelineConfig};
use crate::scan_operation::ScanOperation;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

#[derive(Debug)]
pub struct ScanConfig {
    pub archive: String,
    pub concurrency: usize,
    pub skip_optional: bool,
    pub http_connections: usize,
    pub low: Option<u32>,
    pub high: Option<u32>,
}

pub async fn run(config: ScanConfig) -> Result<()> {
    info!("Starting scan of {}", config.archive);

    if let Some(low) = config.low {
        info!("Scanning from ledger {} onwards", low);
    }
    if let Some(high) = config.high {
        info!("Scanning up to checkpoint {}", high);
    }

    // Create the scan operation with custom HTTP concurrency
    let operation =
        ScanOperation::new_with_concurrency(&config.archive, config.http_connections).await?;

    // Get the storage operator from the operation to ensure consistent policy
    let source_op = operation.storage().clone();

    // Configure the pipeline with low/high bounds
    let pipeline_config = PipelineConfig {
        source: config.archive.clone(),
        concurrency: config.concurrency,
        skip_optional: config.skip_optional,
        low: config.low,
        high: config.high,
    };

    // Create and run the pipeline with the same storage operator
    let pipeline =
        Arc::new(Pipeline::new_with_storage(operation, pipeline_config, source_op).await?);
    pipeline.run().await?;

    Ok(())
}
