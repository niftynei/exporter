use anyhow::{Context, Result, anyhow};
use cln_rpc::ClnRpc;
use serde_json::Value;
use std::{path::Path, time::Duration};

pub async fn call(path: &Path, method: &str, params: Value, timeout: Duration) -> Result<Value> {
    tokio::time::timeout(timeout, async {
        let mut rpc = ClnRpc::new(path)
            .await
            .with_context(|| format!("connecting to CLN RPC at {}", path.display()))?;
        rpc.call_raw(method, &params)
            .await
            .map_err(|error| anyhow!("CLN RPC {method} failed: {error:?}"))
    })
    .await
    .with_context(|| format!("CLN RPC {method} timed out after {timeout:?}"))?
}
