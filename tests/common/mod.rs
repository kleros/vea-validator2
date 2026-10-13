pub use alloy::providers::Provider;
use alloy::providers::ProviderBuilder;
use alloy::primitives::Address;
use vea_validator::config::Route;
use vea_validator::contracts::IVeaInboxArbToEth;
use std::str::FromStr;

const SNAPSHOT_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/.devnet-snapshot");

pub async fn send_messages(route: &Route) {
    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    for i in 0..3u8 {
        let msg = alloy::primitives::Bytes::from(vec![0xDE, 0xAD, i]);
        inbox.sendMessage(Address::from_str("0x0000000000000000000000000000000000000001").unwrap(), msg)
            .send().await.unwrap().get_receipt().await.unwrap();
    }
}

pub async fn restore_pristine() {
    let snapshot_id = std::fs::read_to_string(SNAPSHOT_FILE)
        .unwrap_or_else(|_| panic!("Missing {}. Run full-devnet.sh first.", SNAPSHOT_FILE));
    let snapshot_id = snapshot_id.trim();

    let arb = ProviderBuilder::new().connect_http("http://localhost:8545".parse().unwrap());
    let eth = ProviderBuilder::new().connect_http("http://localhost:8546".parse().unwrap());
    let gnosis = ProviderBuilder::new().connect_http("http://localhost:8547".parse().unwrap());

    let _: serde_json::Value = arb.raw_request("evm_revert".into(), vec![serde_json::json!(snapshot_id)]).await.unwrap();
    let _: serde_json::Value = eth.raw_request("evm_revert".into(), vec![serde_json::json!(snapshot_id)]).await.unwrap();
    let _: serde_json::Value = gnosis.raw_request("evm_revert".into(), vec![serde_json::json!(snapshot_id)]).await.unwrap();

    // `evm_revert` does not restore the mining mode; a test that switched automine off
    // and then failed must not leave it off for the next one.
    for p in [&arb, &eth, &gnosis] {
        let _: serde_json::Value = p.raw_request("evm_setAutomine".into(), vec![serde_json::json!(true)]).await.unwrap();
    }

    let new_id: serde_json::Value = arb.raw_request("evm_snapshot".into(), Vec::<serde_json::Value>::new()).await.unwrap();
    let _: serde_json::Value = eth.raw_request("evm_snapshot".into(), Vec::<serde_json::Value>::new()).await.unwrap();
    let _: serde_json::Value = gnosis.raw_request("evm_snapshot".into(), Vec::<serde_json::Value>::new()).await.unwrap();

    std::fs::write(SNAPSHOT_FILE, new_id.as_str().unwrap()).unwrap();
}

/// Move every chain's block clock to exactly 10 s past the end of `epoch`.
///
/// Set as an absolute timestamp per chain (`evm_setNextBlockTimestamp` + `evm_mine`),
/// which anvil also adopts as its clock for the blocks that follow. Mining with a
/// relative interval does not work here: anvil adds the interval to its clock offset
/// relative to wall time, not to the latest block, so a chain whose latest block is old
/// (Ethereum's pristine deploy block) ended up ahead by the devnet's uptime and the
/// epoch watcher claimed the wrong epoch (`test_claim` failed with "No snapshot" or
/// "Invalid epoch." once the devnet had been up for ~40 minutes).
pub async fn advance_past_epoch(_outbox_provider: &impl Provider, epoch: u64, epoch_period: u64) {
    let target = (epoch + 1) * epoch_period + 10;
    for url in ["http://localhost:8545", "http://localhost:8546", "http://localhost:8547"] {
        let p = ProviderBuilder::new().connect_http(url.parse().unwrap());
        let latest = p.get_block_by_number(Default::default()).await.unwrap().unwrap().header.timestamp;
        assert!(target > latest, "chain {url} is already past the target epoch end ({latest} >= {target})");
        let _: serde_json::Value = p
            .raw_request("evm_setNextBlockTimestamp".into(), vec![serde_json::json!(target)])
            .await
            .unwrap();
        let _: serde_json::Value = p.raw_request("evm_mine".into(), Vec::<serde_json::Value>::new()).await.unwrap();
    }
}

pub async fn advance_time(seconds: u64) {
    let arb = ProviderBuilder::new().connect_http("http://localhost:8545".parse().unwrap());
    let eth = ProviderBuilder::new().connect_http("http://localhost:8546".parse().unwrap());
    let gnosis = ProviderBuilder::new().connect_http("http://localhost:8547".parse().unwrap());

    for p in [&arb, &eth, &gnosis] {
        let _: serde_json::Value = p
            .raw_request("anvil_mine".into(), vec![serde_json::json!(1), serde_json::json!(seconds)])
            .await
            .unwrap();
    }
}
