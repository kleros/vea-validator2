use alloy::primitives::{Address, FixedBytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, Filter};
use alloy::network::Ethereum;
use alloy::providers::DynProvider;

use crate::contracts::{INodeInterface, IVeaInbox};
use crate::find_block_by_timestamp;

const NODE_INTERFACE: Address = Address::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xC8]);
const SEARCH_RANGE_BLOCKS: u64 = 2000;
const MAX_SEARCH_ITERATIONS: u64 = 10;

pub async fn is_epoch_finalized(
    epoch: u64,
    epoch_period: u64,
    arb_provider: &DynProvider<Ethereum>,
    l1_provider: &DynProvider<Ethereum>,
    sequencer_inbox: Option<Address>,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let sequencer_inbox = match sequencer_inbox {
        Some(addr) => addr,
        None => return Ok(true),
    };

    let epoch_end_ts = (epoch + 1) * epoch_period;
    let l2_block = find_block_by_timestamp(arb_provider, epoch_end_ts).await;

    let batch = get_batch_for_block(arb_provider, l2_block).await?;

    let l1_block = get_l1_block_for_batch(l1_provider, sequencer_inbox, batch, epoch_end_ts).await?;
    if l1_block.is_none() {
        return Ok(false);
    }
    let l1_block = l1_block.unwrap();

    let finalized = l1_provider.get_block_by_number(BlockNumberOrTag::Finalized).await?;
    match finalized {
        Some(block) => Ok(block.header.number >= l1_block),
        None => Ok(false),
    }
}

async fn get_batch_for_block(
    arb_provider: &DynProvider<Ethereum>,
    l2_block: u64,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let node_interface = INodeInterface::new(NODE_INTERFACE, arb_provider.clone());
    let result = node_interface.findBatchContainingBlock(l2_block).call().await?;
    Ok(result)
}

async fn get_l1_block_for_batch(
    l1_provider: &DynProvider<Ethereum>,
    sequencer_inbox: Address,
    batch: u64,
    epoch_end_ts: u64,
) -> Result<Option<u64>, Box<dyn std::error::Error + Send + Sync>> {
    let event_sig = alloy::primitives::keccak256("SequencerBatchDelivered(uint256,bytes32,bytes32,bytes32,uint256,(uint64,uint64,uint64,uint64),uint8)");

    let latest = l1_provider.get_block_number().await?;
    let start_block = find_block_by_timestamp(l1_provider, epoch_end_ts).await;
    let mut from_block = start_block;
    let mut iterations = 0;

    while from_block < latest && iterations < MAX_SEARCH_ITERATIONS {
        let to_block = (from_block + SEARCH_RANGE_BLOCKS).min(latest);

        let filter = Filter::new()
            .address(sequencer_inbox)
            .event_signature(event_sig)
            .topic1(U256::from(batch))
            .from_block(from_block)
            .to_block(to_block);

        let logs = l1_provider.get_logs(&filter).await?;

        if !logs.is_empty() {
            return Ok(logs[0].block_number);
        }

        from_block = to_block + 1;
        iterations += 1;
    }

    Ok(None)
}

/// How far behind the inbox head [`final_snapshot`] reads, in blocks (~8 s on Arbitrum).
/// Far enough back that a transport a few blocks behind the head still has the block,
/// close enough that a full node still has its state: public Arbitrum full nodes keep it
/// for only about the last 100-120 blocks (arbitrum-sepolia publicnode, 2026-10-15).
const SNAPSHOT_READ_LAG_BLOCKS: u64 = 32;

/// The inbox snapshot for `epoch`, read at an inbox block past the epoch's end, or
/// `None` when this RPC has not reached the epoch's end yet (S8).
///
/// Read at "latest", a node lagging behind the epoch end answers with an older root
/// saved earlier in the same epoch (or zero), which would make an honest claim look
/// invalid, or make us claim a stale root. A head before the epoch end is caught here,
/// and pinning the read to a post-epoch block makes any other node that lacks that block
/// fail the call rather than answer with stale data.
///
/// `saveSnapshot` only writes the current epoch's slot, so every block from the epoch end
/// on holds the same root. The read is pinned to a recent one, `SNAPSHOT_READ_LAG_BLOCKS`
/// behind the head, or, while the head is fewer blocks than that past the end, to the
/// first block past the end. Pinned to the first block past the end always, the read
/// needed an archive node for every claim, validation and verification (all run minutes
/// to days after the end), and on a full node it failed on every poll.
pub async fn final_snapshot(
    inbox_provider: &DynProvider<Ethereum>,
    inbox_address: Address,
    epoch: u64,
    epoch_period: u64,
) -> Result<Option<FixedBytes<32>>, Box<dyn std::error::Error + Send + Sync>> {
    let epoch_end_ts = (epoch + 1) * epoch_period;
    let head = inbox_provider.get_block_by_number(BlockNumberOrTag::Latest).await?
        .ok_or("inbox RPC returned no latest block")?;
    if head.header.timestamp < epoch_end_ts {
        return Ok(None);
    }
    let recent = head.header.number.saturating_sub(SNAPSHOT_READ_LAG_BLOCKS);
    let block_num = match inbox_provider.get_block_by_number(recent.into()).await? {
        Some(block) if block.header.timestamp >= epoch_end_ts => recent,
        _ => {
            let first = find_block_by_timestamp(inbox_provider, epoch_end_ts).await;
            match inbox_provider.get_block_by_number(first.into()).await? {
                Some(block) if block.header.timestamp >= epoch_end_ts => first,
                _ => return Ok(None),
            }
        }
    };
    let inbox = IVeaInbox::new(inbox_address, inbox_provider.clone());
    Ok(Some(inbox.snapshots(U256::from(epoch)).block(block_num.into()).call().await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use alloy::providers::ProviderBuilder;
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
    use alloy::transports::{TransportError, TransportFut};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    const PERIOD: u64 = 3600;
    const EPOCH: u64 = 10;
    const EPOCH_END: u64 = (EPOCH + 1) * PERIOD;
    /// State kept by a full (non-archive) node, in blocks.
    const KEPT_STATE: u64 = 128;

    fn stale_root() -> FixedBytes<32> { FixedBytes::repeat_byte(0xAA) }
    fn final_root() -> FixedBytes<32> { FixedBytes::repeat_byte(0xBB) }

    /// A full node with one block per second (block `n` at timestamp `n`) that keeps state
    /// for its last `KEPT_STATE` blocks only and answers older `eth_call`s the way Nitro
    /// does. `snapshots(EPOCH)` holds `stale_root` before the epoch end and `final_root`
    /// from it on. Records the block every `eth_call` was pinned to.
    #[derive(Clone)]
    struct PrunedNode {
        head: u64,
        calls: Arc<Mutex<Vec<u64>>>,
    }

    impl PrunedNode {
        fn new(head: u64) -> Self {
            Self { head, calls: Arc::new(Mutex::new(Vec::new())) }
        }

        fn provider(&self) -> DynProvider<Ethereum> {
            DynProvider::new(ProviderBuilder::new().connect_client(RpcClient::builder().transport(self.clone(), true)))
        }

        fn block_number(&self, tag: &Value) -> u64 {
            match tag.as_str().unwrap() {
                "latest" => self.head,
                hex => u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap(),
            }
        }

        fn answer(&self, method: &str, params: &Value) -> Result<Value, (i64, String)> {
            match method {
                "eth_getBlockByNumber" => {
                    let n = self.block_number(&params[0]);
                    if n > self.head {
                        return Ok(Value::Null);
                    }
                    let header = alloy::rpc::types::Header {
                        hash: B256::from(U256::from(n)),
                        inner: alloy::consensus::Header { number: n, timestamp: n, ..Default::default() },
                        total_difficulty: None,
                        size: None,
                    };
                    Ok(serde_json::to_value(alloy::rpc::types::Block::<alloy::rpc::types::Transaction>::empty(header)).unwrap())
                }
                "eth_call" => {
                    let n = self.block_number(&params[1]);
                    self.calls.lock().unwrap().push(n);
                    if n > self.head {
                        return Err((-32000, "header not found".into()));
                    }
                    if n + KEPT_STATE < self.head {
                        return Err((-32000, format!("historical state {} is not available", B256::from(U256::from(n)))));
                    }
                    let root = if n >= EPOCH_END { final_root() } else { stale_root() };
                    Ok(json!(alloy::hex::encode_prefixed(root)))
                }
                m => Err((-32601, format!("{m} is not served by the fake node"))),
            }
        }
    }

    impl tower::Service<RequestPacket> for PrunedNode {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RequestPacket) -> Self::Future {
            let node = self.clone();
            Box::pin(async move {
                let respond = |r: &Value| match node.answer(r["method"].as_str().unwrap(), &r["params"]) {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": r["id"], "result": result }),
                    Err((code, message)) => json!({ "jsonrpc": "2.0", "id": r["id"], "error": { "code": code, "message": message } }),
                };
                let response = match serde_json::to_value(&req).unwrap() {
                    Value::Array(batch) => Value::Array(batch.iter().map(respond).collect()),
                    single => respond(&single),
                };
                Ok(serde_json::from_str(&response.to_string()).unwrap())
            })
        }
    }

    /// Long after the epoch end, as for every claim, validation and verification, a full
    /// node no longer has the state of the first block past it. The read must still
    /// succeed there: pinned to that first block, it failed forever without an archive node.
    #[tokio::test]
    async fn pruned_node_serves_the_final_snapshot_long_after_the_epoch_end() {
        let node = PrunedNode::new(EPOCH_END + 27 * 3600);
        let root = final_snapshot(&node.provider(), Address::ZERO, EPOCH, PERIOD).await
            .expect("a full node must be able to serve the read");
        assert_eq!(root, Some(final_root()));
        let at = *node.calls.lock().unwrap().last().unwrap();
        assert!(at + KEPT_STATE >= node.head, "read pinned to block {at}, outside the node's state window");
    }

    /// Within the lag of the epoch end the recent block precedes it, so the read falls back
    /// to the first block past the end; it is never at a block before the end (S8).
    #[tokio::test]
    async fn just_after_the_epoch_end_reads_the_first_block_past_it() {
        let node = PrunedNode::new(EPOCH_END + 5);
        assert_eq!(final_snapshot(&node.provider(), Address::ZERO, EPOCH, PERIOD).await.unwrap(), Some(final_root()));
        assert_eq!(*node.calls.lock().unwrap(), vec![EPOCH_END]);
    }

    /// A head before the epoch end is not ready, and nothing is read (S8).
    #[tokio::test]
    async fn head_before_the_epoch_end_is_not_ready() {
        let node = PrunedNode::new(EPOCH_END - 1);
        assert_eq!(final_snapshot(&node.provider(), Address::ZERO, EPOCH, PERIOD).await.unwrap(), None);
        assert!(node.calls.lock().unwrap().is_empty());
    }
}
