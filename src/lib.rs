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

/// Backoff applied *before* each retry; the number of attempts is one more than
/// the number of delays (initial call + one retry per delay).
const RPC_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(45),
];

const RPC_MAX_ATTEMPTS: usize = RPC_RETRY_DELAYS.len() + 1;

pub(crate) const RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

/// Percentage a replacement transaction must reach relative to the one it replaces.
///
/// Geth's default `--txpool.pricebump` is 10%, applied to `maxFeePerGas` and
/// `maxPriorityFeePerGas` independently - bumping only one is rejected. 112 leaves
/// headroom for integer truncation and for nodes configured above the default.
const FEE_BUMP_PERCENT: u128 = 112;

/// Ceiling on a replacement fee, as a multiple of the current market estimate.
/// Resending every dispatcher cycle compounds, so escalation has to stop somewhere.
const FEE_CEILING_MULTIPLE: u128 = 4;

/// Fee for a replacement transaction: at least `FEE_BUMP_PERCENT` of what the stuck
/// transaction paid (or the node rejects it as underpriced), and at least the current
/// market rate (or it clears the mempool's rule but still will not be mined).
///
/// Returns `None` once the result would exceed the ceiling, meaning: stop bumping and
/// keep waiting. The last replacement stays in the mempool and remains mineable, so
/// the deposit is never at risk of being spent twice however long it takes.
pub(crate) fn bumped_fee(previous: u128, market_estimate: u128) -> Option<u128> {
    let bumped = (previous.saturating_mul(FEE_BUMP_PERCENT) / 100).max(market_estimate);
    let ceiling = market_estimate.saturating_mul(FEE_CEILING_MULTIPLE);
    (bumped <= ceiling).then_some(bumped)
}

/// Retries `f` with [`RPC_RETRY_DELAYS`] backoff, returning the last error if every
/// attempt fails. Callers decide what a failure means: startup paths that cannot
/// continue should use [`retry_rpc_or_panic`], while long-running loops should
/// handle the error and keep going.
pub(crate) async fn retry_rpc<T, E, F, Fut>(op_name: impl std::fmt::Display, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for attempt in 0..RPC_MAX_ATTEMPTS {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => match RPC_RETRY_DELAYS.get(attempt) {
                Some(delay) => {
                    warn!(logger = "RPC", op = %op_name, attempt = attempt + 1, "{op_name} failed: {e}, retrying...");
                    sleep(*delay).await;
                }
                None => return Err(e),
            },
        }
    }
    unreachable!()
}

/// Like [`retry_rpc`], but also retries when the node answers `null`.
///
/// A node that is behind or mid-sync can return `null` for a block that does exist,
/// and with several transports racing it is often the fastest to answer — there is
/// no block to fetch. Treating that as success hands `None` straight to callers that
/// panic on it, when a single retry would usually have cleared it.
pub(crate) async fn retry_rpc_opt<T, E, F, Fut>(op_name: impl std::fmt::Display, mut f: F) -> Result<Option<T>, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, E>>,
    E: std::fmt::Display,
{
    for attempt in 0..RPC_MAX_ATTEMPTS {
        let result = f().await;
        if let Ok(Some(_)) = &result {
            return result;
        }
        match RPC_RETRY_DELAYS.get(attempt) {
            Some(delay) => {
                match &result {
                    Ok(_) => warn!(logger = "RPC", op = %op_name, attempt = attempt + 1, "{op_name} returned null, retrying..."),
                    Err(e) => warn!(logger = "RPC", op = %op_name, attempt = attempt + 1, "{op_name} failed: {e}, retrying..."),
                }
                sleep(*delay).await;
            }
            None => return result,
        }
    }
    unreachable!()
}

/// Fail-fast wrapper around [`retry_rpc`] for paths where there is no meaningful
/// way to continue (startup checks, config loading).
pub(crate) async fn retry_rpc_or_panic<T, E, F, Fut>(op_name: impl std::fmt::Display, f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    retry_rpc(&op_name, f)
        .await
        .unwrap_or_else(|e| panic!("Failed to {op_name} after {RPC_MAX_ATTEMPTS} attempts: {e}"))
}

async fn get_block_with_retry(provider: &DynProvider<Ethereum>, block_num: alloy::eips::BlockNumberOrTag) -> Block {
    retry_rpc_opt(std::fmt::from_fn(|f| write!(f, "get block {block_num}")), || async { provider.get_block_by_number(block_num).await }).await
        .unwrap_or_else(|e| panic!("Failed to get block {block_num}: {e}"))
        .unwrap_or_else(|| panic!("Block {block_num} not found"))
}

pub async fn find_block_by_timestamp(provider: &DynProvider<Ethereum>, target_ts: u64) -> u64 {
    find_block_by_timestamp_from(provider, target_ts, 0).await
}

/// Like [`find_block_by_timestamp`], but seeds the search at `lo` rather than block 0.
///
/// The indexer's target only moves forward, so its previous answer is a valid lower
/// bound. Searching the whole chain each time costs ~28 block fetches on Arbitrum,
/// multiplied by the active transport count, every idle cycle, per chain, per route —
/// to rediscover a block already known. Seeding cuts that to the log of one cycle's
/// worth of blocks.
///
/// A stale `lo` above the true answer is safe: the search then returns `lo` itself,
/// which the caller has already scanned past, so no blocks are skipped.
pub async fn find_block_by_timestamp_from(
    provider: &DynProvider<Ethereum>,
    target_ts: u64,
    lo: u64,
) -> u64 {
    let latest = get_block_with_retry(provider, Default::default()).await;
    let latest_num = latest.header.number;

    if target_ts >= latest.header.timestamp {
        return latest_num;
    }

    let mut lo = lo.min(latest_num);
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

#[cfg(test)]
mod tests {
    use super::*;

    const GWEI: u128 = 1_000_000_000;

    #[test]
    fn bump_clears_geth_ten_percent_threshold() {
        // Market roughly level with what the stuck tx paid - the realistic stall.
        let out = bumped_fee(30 * GWEI, 30 * GWEI).unwrap();
        assert!(out >= 30 * GWEI * 110 / 100, "{out} does not clear a 10% pricebump over 30 gwei");
    }

    #[test]
    fn no_bump_when_already_far_above_market() {
        // Paying 30 gwei into a 1 gwei market: the tx will mine on its own merits, and
        // bumping past the ceiling would only overpay. Holding is the right answer.
        assert_eq!(bumped_fee(30 * GWEI, GWEI), None);
    }

    #[test]
    fn bump_floors_at_market_when_market_has_risen() {
        // 112% of 10 gwei is still far under a 50 gwei market: it would be accepted as a
        // replacement yet never mined, so the market rate has to win.
        assert_eq!(bumped_fee(10 * GWEI, 50 * GWEI), Some(50 * GWEI));
    }

    #[test]
    fn bump_stops_at_ceiling() {
        // Already at 4x market; another 12% would exceed it.
        assert_eq!(bumped_fee(40 * GWEI, 10 * GWEI), None);
    }

    #[test]
    fn bump_allowed_below_ceiling() {
        assert!(bumped_fee(20 * GWEI, 10 * GWEI).is_some(), "2x market is inside the 4x ceiling");
    }

    #[test]
    fn does_not_overflow_on_extreme_inputs() {
        // Degenerate values cannot occur in practice; the requirement is simply that
        // the arithmetic saturates instead of panicking in debug builds.
        let _ = bumped_fee(u128::MAX / 2, u128::MAX);
        let _ = bumped_fee(u128::MAX, GWEI);
        let _ = bumped_fee(0, 0);
    }
}
