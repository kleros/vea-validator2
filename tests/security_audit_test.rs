mod common;

use alloy::primitives::{Address, FixedBytes, U256};
use futures_util::FutureExt;
use serial_test::serial;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use vea_validator::{
    contracts::{Claim, IVeaInboxArbToEth, IVeaOutboxArbToEth, Party},
    config::ValidatorConfig,
    indexer::EventIndexer,
    tasks::{dispatcher::TaskDispatcher, TaskKind, TaskStore, ClaimStore},
};
use common::{restore_pristine, advance_time, advance_past_epoch, send_messages};
use alloy::providers::Provider;

const WRONG_ROOT: [u8; 32] = [0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

fn stores(dir: &tempfile::TempDir) -> (Arc<Mutex<TaskStore>>, Arc<Mutex<ClaimStore>>) {
    (
        Arc::new(Mutex::new(TaskStore::new(dir.path().join("schedule.json")))),
        Arc::new(Mutex::new(ClaimStore::new(dir.path().join("claims.json")))),
    )
}

async fn challenged_epochs(provider: &impl Provider, outbox: Address) -> Vec<u64> {
    let sig = alloy::primitives::keccak256("Challenged(uint256,address)");
    let filter = alloy::rpc::types::Filter::new().address(outbox).event_signature(sig).from_block(0u64);
    provider.get_logs(&filter).await.unwrap().iter()
        .map(|l| U256::from_be_bytes(l.topics()[1].0).to::<u64>())
        .collect()
}

/// S1: an outbox event for a claim made before the validator's sync window must not
/// crash the route, and a dishonest claim whose verification starts after a fresh
/// start must still be challenged.
///
/// `startVerification` has no deadline, so anyone can call it on an old claim at any
/// time; before the fix the indexer panicked ("this is a bug") on the event, the
/// cursor never advanced past it, and the route crash-looped forever.
#[tokio::test]
#[serial]
async fn test_s1_old_claim_verification_started_after_fresh_start() {
    let c = ValidatorConfig::from_env().unwrap();
    let mut route = c.build_routes()[0].clone();
    // A short window, so a claim 25 hours old lies outside it.
    route.settings.sync_lookback_secs = 2 * 3600;
    let outbox_provider = route.outbox_provider.clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    let deposit = outbox.deposit().call().await.unwrap();

    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;

    // The attacker's dishonest claim.
    let wrong_root = FixedBytes::<32>::from(WRONG_ROOT);
    let receipt = outbox.claim(U256::from(epoch), wrong_root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    let claim_ts = outbox_provider.get_block_by_number(receipt.block_number.unwrap().into()).await.unwrap().unwrap().header.timestamp;
    let claimer = receipt.from;

    // Past sequencerDelayLimit + epochPeriod, so startVerification is allowed.
    advance_time(25 * 3600 + 60).await;

    // The validator starts fresh now: its window begins well after the claim.
    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), c.wallet.default_signer().address(), task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    indexer.scan_once().await;
    task_store.lock().unwrap().set_on_sync(true);
    assert!(!claim_store.lock().unwrap().exists(epoch), "precondition: claim is outside the sync window");

    // The attacker starts verification after the validator started.
    outbox.startVerification(U256::from(epoch), Claim {
        stateRoot: wrong_root,
        claimer,
        timestampClaimed: claim_ts as u32,
        timestampVerification: 0,
        blocknumberVerification: 0,
        honest: Party::None,
        challenger: Address::ZERO,
    }).send().await.unwrap().get_receipt().await.unwrap();

    advance_time(20 * 60 + 10).await;

    let scanned = AssertUnwindSafe(indexer.scan_once()).catch_unwind().await;
    assert!(scanned.is_ok(), "indexer panicked on VerificationStarted for a claim older than the sync window");

    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    dispatcher.process_pending().await;
    dispatcher.process_pending().await;

    assert!(
        challenged_epochs(&outbox_provider, route.outbox_address).await.contains(&epoch),
        "dishonest claim older than the sync window was not challenged after its verification started"
    );
    let state = task_store.lock().unwrap().load();
    assert!(!state.tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::Challenge)), "Challenge task should be done");
}

/// S2: a claim that fails for a non-RPC reason (here: no WETH for the Gnosis deposit)
/// must not panic the route. The epoch watcher shares the route's `select!` with the
/// indexer and dispatcher, so a panic there restarts the whole route (5 minutes plus a
/// resync) and, failing again on every restart, starves the challenge path.
#[tokio::test]
#[serial]
async fn test_s2_failed_claim_does_not_panic_route() {
    use vea_validator::contracts::IVeaInboxArbToGnosis;
    use vea_validator::epoch_watcher::EpochWatcher;

    let c = ValidatorConfig::from_env().unwrap();
    assert!(c.make_claims, "devnet .env.local sets MAKE_CLAIMS=true");
    let route = c.build_routes()[1].clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToGnosis::new(route.inbox_address, route.inbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    for i in 0..3u8 {
        inbox.sendMessage(Address::with_last_byte(1), alloy::primitives::Bytes::from(vec![0xAA, i]))
            .send().await.unwrap().get_receipt().await.unwrap();
    }
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&route.outbox_provider, epoch, epoch_period).await;
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    task_store.lock().unwrap().set_on_sync(true);
    let watcher = EpochWatcher::new(c.clone(), route.clone(), true, claim_store, task_store);

    let run = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        AssertUnwindSafe(watcher.watch_epochs(epoch_period)).catch_unwind(),
    ).await;
    match run {
        Err(_) => {} // still running after the failed claim: the route survives
        Ok(Err(_)) => panic!("epoch watcher panicked on a failed claim, crashing the route"),
        Ok(Ok(r)) => panic!("epoch watcher returned unexpectedly: {r:?}"),
    }
}
