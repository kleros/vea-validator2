pub mod contracts;
pub mod config;
pub mod startup;
pub mod tasks;
pub mod epoch_watcher;
pub mod indexer;
pub mod finality;

use alloy::providers::{Provider, DynProvider};
use alloy::network::Ethereum;
use alloy::rpc::types::Block;
use tokio::time::{sleep, Duration};
use tracing::warn;

const RPC_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(45),
];

pub(crate) async fn retry_rpc<T, E, F, Fut>(op_name: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for (attempt, delay) in RPC_RETRY_DELAYS.iter().enumerate() {
        match f().await {
            Ok(v) => return v,
            Err(e) => {
                if attempt == RPC_RETRY_DELAYS.len() - 1 {
                    panic!("Failed to {op_name} after {} attempts: {e}", RPC_RETRY_DELAYS.len());
                }
                warn!(logger = "RPC", op = op_name, attempt = attempt + 1, "{op_name} failed: {e}, retrying...");
                sleep(*delay).await;
            }
        }
    }
    unreachable!()
}

async fn get_block_with_retry(provider: &DynProvider<Ethereum>, block_num: alloy::eips::BlockNumberOrTag) -> Block {
    retry_rpc(&format!("get block {block_num}"), || async { provider.get_block_by_number(block_num).await }).await
        .unwrap_or_else(|| panic!("Block {block_num} not found"))
}

pub async fn find_block_by_timestamp(provider: &DynProvider<Ethereum>, target_ts: u64) -> u64 {
    let latest = get_block_with_retry(provider, Default::default()).await;
    let latest_num = latest.header.number;

    if target_ts >= latest.header.timestamp {
        return latest_num;
    }

    let mut lo = 0u64;
    let mut hi = latest_num;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let block = get_block_with_retry(provider, mid.into()).await;
        if block.header.timestamp < target_ts {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}
