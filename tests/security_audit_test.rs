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

/// S3: a pending record whose transaction is in no mempool (dropped, evicted, or never
/// broadcast because the send errored after the record was written) must not block the
/// action forever once the market fee has fallen below what the record paid.
///
/// Before the fix the bump would exceed the 4x-market ceiling, so `send_or_replace`
/// held, and the hold only clears when the saved nonce is mined - which never happens
/// when nothing is pending at that nonce. A challenge stuck like this never goes out.
#[tokio::test]
#[serial]
async fn test_s3_stale_pending_record_above_ceiling_is_resent() {
    use vea_validator::contracts::IVeaOutbox;
    use vea_validator::tasks::{pending_key, send_or_replace};

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let provider = route.outbox_provider.clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, _) = stores(&dir);
    let market = provider.estimate_eip1559_fees().await.unwrap();
    let nonce = provider.get_transaction_count(wallet).pending().await.unwrap();
    // Recorded when fees were 100x today's market; nothing was ever broadcast.
    task_store.lock().unwrap().set_pending_tx(
        &pending_key("challenge", 1), nonce, market.max_fee_per_gas * 100, market.max_priority_fee_per_gas.max(1) * 100,
    );

    let outbox = IVeaOutbox::new(route.outbox_address, provider.clone());
    let mut outcome = String::new();
    for _ in 0..3 {
        match send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "challenge", route.name).await {
            Ok(()) => { outcome = "sent".into(); break; }
            Err(e) => outcome = e.to_string(),
        }
    }
    assert_eq!(outcome, "sent", "stale pending record blocked the send");
    assert_eq!(provider.get_transaction_count(wallet).latest().await.unwrap(), nonce + 1, "tx should be mined at the recorded nonce");
    assert!(task_store.lock().unwrap().get_pending_tx(&pending_key("challenge", 1)).is_none(), "record should be cleared");
}

async fn set_automine(provider: &impl Provider, on: bool) {
    let _: serde_json::Value = provider.raw_request("evm_setAutomine".into(), vec![serde_json::json!(on)]).await.unwrap();
}

async fn mine(provider: &impl Provider) {
    let _: serde_json::Value = provider.raw_request("evm_mine".into(), Vec::<serde_json::Value>::new()).await.unwrap();
}

/// Hold path 1 ("queued behind an earlier nonce"): a record above the blocking nonce
/// is held while the earlier tx is pending, and cleared as `NonceConsumed` once the
/// confirmed nonce has passed it.
#[tokio::test]
#[serial]
async fn test_hold_path_queued_behind_earlier_nonce() {
    use vea_validator::contracts::IVeaOutbox;
    use vea_validator::tasks::{pending_key, send_or_replace};

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let provider = route.outbox_provider.clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    let dir = tempfile::tempdir().unwrap();
    let (task_store, _) = stores(&dir);
    let outbox = IVeaOutbox::new(route.outbox_address, provider.clone());
    let key = pending_key("challenge", 1);
    let market = provider.estimate_eip1559_fees().await.unwrap();

    set_automine(&provider, false).await;
    let n = provider.get_transaction_count(wallet).latest().await.unwrap();
    let _earlier = outbox.deposit().nonce(n).send().await.unwrap();

    // Our record sits at n + 1, behind the pending tx at n: hold, keep the record.
    task_store.lock().unwrap().set_pending_tx(&key, n + 1, market.max_fee_per_gas, market.max_priority_fee_per_gas);
    let r = send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "challenge", route.name).await;
    assert_eq!(r.unwrap_err().to_string(), "PendingReplacement");
    assert_eq!(task_store.lock().unwrap().get_pending_tx(&key).unwrap().nonce, n + 1);
    assert_eq!(provider.get_transaction_count(wallet).pending().await.unwrap(), n + 1, "nothing new broadcast while held");

    // n and n + 1 mine (someone else used n + 1); a new tx is pending at n + 2. Our
    // record at n + 1 is now below the confirmed nonce: cleared, NonceConsumed.
    let _other = outbox.deposit().nonce(n + 1).send().await.unwrap();
    mine(&provider).await;
    let _later = outbox.deposit().nonce(n + 2).send().await.unwrap();
    let r = send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "challenge", route.name).await;
    assert_eq!(r.unwrap_err().to_string(), "NonceConsumed");
    assert!(task_store.lock().unwrap().get_pending_tx(&key).is_none(), "consumed record should be cleared");
    set_automine(&provider, true).await;
}

/// Hold path 2 ("fee ceiling reached"): our own tx is pending at the recorded nonce at
/// a fee far above market, so it is held (no further bump). Once it is mined the next
/// attempt learns the nonce is consumed and clears the record.
#[tokio::test]
#[serial]
async fn test_hold_path_fee_ceiling_then_consumed() {
    use vea_validator::contracts::IVeaOutbox;
    use vea_validator::tasks::{pending_key, send_or_replace};

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let provider = route.outbox_provider.clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    let dir = tempfile::tempdir().unwrap();
    let (task_store, _) = stores(&dir);
    let outbox = IVeaOutbox::new(route.outbox_address, provider.clone());
    let key = pending_key("challenge", 1);
    let market = provider.estimate_eip1559_fees().await.unwrap();
    let (high_fee, high_tip) = (market.max_fee_per_gas * 100, market.max_priority_fee_per_gas.max(1) * 100);

    set_automine(&provider, false).await;
    let n = provider.get_transaction_count(wallet).latest().await.unwrap();
    task_store.lock().unwrap().set_pending_tx(&key, n, high_fee, high_tip);
    let _ours = outbox.deposit().nonce(n).max_fee_per_gas(high_fee).max_priority_fee_per_gas(high_tip).send().await.unwrap();

    let r = send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "challenge", route.name).await;
    assert_eq!(r.unwrap_err().to_string(), "PendingReplacement", "pending at 100x market: hold, do not bump");
    assert_eq!(task_store.lock().unwrap().get_pending_tx(&key).unwrap().max_fee, high_fee, "record unchanged while held");

    mine(&provider).await;
    let r = send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "challenge", route.name).await;
    assert_eq!(r.unwrap_err().to_string(), "NonceConsumed");
    assert!(task_store.lock().unwrap().get_pending_tx(&key).is_none());
    assert_eq!(provider.get_transaction_count(wallet).latest().await.unwrap(), n + 1, "exactly one tx at the pinned nonce");
    set_automine(&provider, true).await;
}

/// F4 re-add path: a replayed `Claimed` log (crash between storing the claim and
/// scheduling its task) re-adds `ValidateClaim`, unless the claim has moved on.
#[tokio::test]
#[serial]
async fn test_claimed_replay_readds_validate_claim_unless_moved_on() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let outbox_provider = route.outbox_provider.clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    let root = inbox.snapshots(U256::from(epoch)).call().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), c.wallet.default_signer().address(), task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    let start = task_store.lock().unwrap().load();
    indexer.scan_once().await;

    let has_validate = |ts: &Arc<Mutex<TaskStore>>| ts.lock().unwrap().load().tasks.iter()
        .any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::ValidateClaim));
    let replay = |ts: &Arc<Mutex<TaskStore>>| {
        let s = ts.lock().unwrap();
        let t = s.load().tasks.into_iter().find(|t| t.epoch == epoch);
        if let Some(t) = t { s.remove_task(&t); }
        s.initialize_sync(start.indexing_since.unwrap(), start.inbox_last_block.unwrap(), start.outbox_last_block.unwrap());
    };
    assert!(has_validate(&task_store));

    // Crash after store(), before add_task(): the claim exists, the task does not.
    replay(&task_store);
    assert!(!has_validate(&task_store));
    indexer.scan_once().await;
    assert!(has_validate(&task_store), "replayed Claimed should re-add ValidateClaim");
    assert_eq!(claim_store.lock().unwrap().get(epoch).state_root, root, "stored claim unchanged");

    // A claim that has moved on (here: challenged) must not get ValidateClaim back.
    claim_store.lock().unwrap().update(epoch, |c| c.challenger = Address::with_last_byte(9));
    replay(&task_store);
    indexer.scan_once().await;
    assert!(!has_validate(&task_store), "moved-on claim must not get ValidateClaim re-added");
}

/// F3 wiring: the VerifySnapshot task, run by the dispatcher, refuses to verify a
/// claim whose root differs from the inbox snapshot, and drops the task.
#[tokio::test]
#[serial]
async fn test_verify_snapshot_task_refuses_dishonest_claim() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let outbox_provider = route.outbox_provider.clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), c.wallet.default_signer().address(), task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    task_store.lock().unwrap().set_on_sync(true);

    let wrong_root = FixedBytes::<32>::from(WRONG_ROOT);
    let deposit = outbox.deposit().call().await.unwrap();
    let receipt = outbox.claim(U256::from(epoch), wrong_root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    let claim_ts = outbox_provider.get_block_by_number(receipt.block_number.unwrap().into()).await.unwrap().unwrap().header.timestamp;
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;
    // Isolate the verify path: pretend validation never ran.
    let t = task_store.lock().unwrap().load().tasks.into_iter().find(|t| t.epoch == epoch).unwrap();
    task_store.lock().unwrap().remove_task(&t);

    advance_time(25 * 3600 + 60).await;
    outbox.startVerification(U256::from(epoch), Claim {
        stateRoot: wrong_root, claimer: receipt.from, timestampClaimed: claim_ts as u32,
        timestampVerification: 0, blocknumberVerification: 0, honest: Party::None, challenger: Address::ZERO,
    }).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;
    assert!(task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::VerifySnapshot)));

    advance_time(3600).await;
    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    dispatcher.process_pending().await;

    let sig = alloy::primitives::keccak256("Verified(uint256)");
    let filter = alloy::rpc::types::Filter::new().address(route.outbox_address).event_signature(sig).from_block(0u64);
    assert!(outbox_provider.get_logs(&filter).await.unwrap().is_empty(), "dishonest claim was verified");
    assert!(!task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::VerifySnapshot)), "VerifySnapshot should be dropped");
}

/// S6: a send that reverts in gas estimation was never broadcast, so it must not leave
/// a pending record. A stale record pins a nonce another action then uses; the next
/// attempt "bumps" it and silently replaces that other action's in-flight tx.
#[tokio::test]
#[serial]
async fn test_s6_estimate_revert_leaves_no_record_to_hijack_another_tx() {
    use vea_validator::contracts::IVeaOutbox;
    use vea_validator::tasks::{pending_key, send_or_replace};

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let provider = route.outbox_provider.clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    let dir = tempfile::tempdir().unwrap();
    let (task_store, _) = stores(&dir);
    let eth_outbox = IVeaOutboxArbToEth::new(route.outbox_address, provider.clone());
    let outbox = IVeaOutbox::new(route.outbox_address, provider.clone());
    let key = pending_key("claim", 1);

    // A claim without its deposit reverts in estimation: nothing is broadcast.
    let n = provider.get_transaction_count(wallet).pending().await.unwrap();
    let r = send_or_replace(eth_outbox.claim(U256::from(1), FixedBytes::<32>::from(WRONG_ROOT)), &provider, wallet, &task_store, 1, "claim", route.name).await;
    assert!(r.unwrap_err().to_string().contains("reverted"));
    assert_eq!(provider.get_transaction_count(wallet).pending().await.unwrap(), n, "nothing was broadcast");

    // Another action (another task or route) now has its tx in flight at nonce n.
    set_automine(&provider, false).await;
    let other = outbox.epochPeriod().nonce(n).send().await.unwrap();
    let other_hash = *other.tx_hash();

    let miner = {
        let p = provider.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            mine(&p).await;
        })
    };
    send_or_replace(outbox.deposit(), &provider, wallet, &task_store, 1, "claim", route.name).await.unwrap();
    miner.await.unwrap();
    set_automine(&provider, true).await;

    assert!(
        provider.get_transaction_receipt(other_hash).await.unwrap().is_some(),
        "the other action's in-flight tx was replaced through a stale pending record"
    );
    assert!(task_store.lock().unwrap().get_pending_tx(&key).is_none());
}

/// S7: a validator that (re)syncs over a dispute that is already resolved must not
/// send the snapshot again. `Verified` did not invalidate `SendSnapshot`, so every
/// challenged-and-resolved epoch in the sync window cost a fresh `sendSnapshot` on L2
/// and, through the relay it schedules, an L1 `executeTransaction` that resolves nothing.
#[tokio::test]
#[serial]
async fn test_s7_resync_does_not_resend_snapshot_for_resolved_dispute() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let outbox_provider = route.outbox_provider.clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), FixedBytes::<32>::from(WRONG_ROOT)).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    // Validator A runs the whole dispute: challenge, sendSnapshot, relay -> Verified.
    let dir_a = tempfile::tempdir().unwrap();
    let (ts_a, cs_a) = stores(&dir_a);
    let indexer_a = EventIndexer::new(route.clone(), wallet, ts_a.clone(), cs_a.clone());
    indexer_a.initialize().await;
    ts_a.lock().unwrap().set_on_sync(true);
    let dispatcher_a = TaskDispatcher::new(c.clone(), route.clone(), ts_a.clone(), cs_a.clone());
    indexer_a.scan_once().await;
    dispatcher_a.process_pending().await;
    dispatcher_a.process_pending().await;
    advance_time(20 * 60 + 10).await;
    indexer_a.scan_once().await;
    dispatcher_a.process_pending().await;
    advance_time(20 * 60 + 10).await;
    indexer_a.scan_once().await;
    advance_time(7 * 24 * 3600 + 3600 + 10).await;
    dispatcher_a.process_pending().await;
    let sig = alloy::primitives::keccak256("Verified(uint256)");
    let filter = alloy::rpc::types::Filter::new().address(route.outbox_address).event_signature(sig).from_block(0u64);
    assert!(!outbox_provider.get_logs(&filter).await.unwrap().is_empty(), "precondition: dispute resolved");
    advance_time(20 * 60 + 10).await;

    // Validator B starts fresh; its window covers the whole, finished dispute.
    let dir_b = tempfile::tempdir().unwrap();
    let (ts_b, cs_b) = stores(&dir_b);
    let indexer_b = EventIndexer::new(route.clone(), wallet, ts_b.clone(), cs_b.clone());
    indexer_b.initialize().await;
    while !indexer_b.scan_once().await {}
    let left: Vec<_> = ts_b.lock().unwrap().load().tasks.into_iter().filter(|t| t.epoch == epoch).map(|t| t.kind.name()).collect();
    assert!(!left.contains(&"SendSnapshot"), "resolved dispute still schedules SendSnapshot after resync: {left:?}");
}

/// A second Arbitrum node that is stuck at an older block, as a lagging or stuck RPC in
/// a fallback set would be. Killed on drop.
struct LaggingNode(std::process::Child);

impl LaggingNode {
    async fn fork_at(block: u64) -> (Self, alloy::providers::DynProvider) {
        let child = std::process::Command::new("anvil")
            .args(["--fork-url", "http://localhost:8545", "--fork-block-number", &block.to_string(), "-p", "8548", "--chain-id", "31337"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("anvil must be on PATH");
        let node = LaggingNode(child);
        let provider = alloy::providers::DynProvider::new(
            alloy::providers::ProviderBuilder::new().connect_http("http://localhost:8548".parse().unwrap()),
        );
        for _ in 0..50 {
            if provider.get_block_number().await.is_ok() { return (node, provider); }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        panic!("lagging anvil did not start");
    }
}

impl Drop for LaggingNode {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// S8: an Arbitrum RPC that lags behind the end of the claimed epoch answers
/// `snapshots(epoch)` with an older root. `validate_claim` then judged an honest claim
/// invalid and scheduled a challenge (losing the challenge deposit), and `claim` would
/// file that stale root (losing the claim deposit). The finality check does not catch
/// it: on a lagging node the block search returns its head, whose batch is final.
#[tokio::test]
#[serial]
async fn test_s8_lagging_inbox_rpc_does_not_challenge_honest_claim() {
    use vea_validator::tasks::validate_claim;

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let outbox_provider = route.outbox_provider.clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    // Two snapshots in one epoch: the claim must match the last one.
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    let first = inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    send_messages(&route).await;
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    let root = inbox.snapshots(U256::from(epoch)).call().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), c.wallet.default_signer().address(), task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    indexer.scan_once().await;
    assert!(claim_store.lock().unwrap().exists(epoch));

    // The Arbitrum RPC that answers is stuck right after the first snapshot.
    let (_node, lagging) = LaggingNode::fork_at(first.block_number.unwrap()).await;
    let mut lagging_route = route.clone();
    lagging_route.inbox_provider = lagging;
    let now = outbox_provider.get_block_by_number(Default::default()).await.unwrap().unwrap().header.timestamp;

    let r = validate_claim::execute(&c, &lagging_route, epoch, &claim_store, now, &task_store).await;
    let tasks = task_store.lock().unwrap().load().tasks;
    assert!(
        !tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::Challenge)),
        "honest claim scheduled for challenge from a lagging RPC's stale snapshot (result: {r:?})"
    );
    assert_eq!(r.unwrap_err().to_string(), "EpochNotFinalized", "a node behind the epoch end is not ready yet");
}

/// S11: two senders sharing the wallet on one chain (both routes' dispatchers, the
/// epoch watcher, the relay on its own provider instance) must not pick the same nonce.
/// Without the per-chain broadcast lock both read the same `pending` count: the second
/// is rejected as underpriced, and its next attempt "bumps", replacing the first one's
/// transaction. The two keys and routes differ on purpose - only the chain is shared.
#[tokio::test]
#[serial]
async fn test_s11_concurrent_senders_get_distinct_nonces() {
    use vea_validator::contracts::IVeaOutbox;
    use vea_validator::tasks::send_or_replace;

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    let dir = tempfile::tempdir().unwrap();
    let (task_store, _) = stores(&dir);
    // Two provider instances on one chain, as the ETH route's outbox and the relay are.
    let p1 = route.outbox_provider.clone();
    let p2 = c.ethereum_provider.clone();
    let n = p1.get_transaction_count(wallet).latest().await.unwrap();

    set_automine(&p1, false).await;
    let miner = {
        let p = p1.clone();
        tokio::spawn(async move {
            for _ in 0..40 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                mine(&p).await;
            }
        })
    };
    let o1 = IVeaOutbox::new(route.outbox_address, p1.clone());
    let o2 = IVeaOutbox::new(route.outbox_address, p2.clone());
    let (a, b) = tokio::join!(
        send_or_replace(o1.deposit(), &p1, wallet, &task_store, 1, "challenge", route.name),
        send_or_replace(o2.epochPeriod(), &p2, wallet, &task_store, 2, "executeTransaction", "ARB_TO_GNOSIS"),
    );
    miner.abort();
    set_automine(&p1, true).await;

    assert!(a.is_ok() && b.is_ok(), "both concurrent sends should land: {a:?} / {b:?}");
    assert_eq!(p1.get_transaction_count(wallet).latest().await.unwrap(), n + 2, "one tx at n and one at n + 1");
}

/// S18/S12: an Arbitrum send whose gas estimate reverts must not leave a nonce gap.
/// alloy's `CachedNonceManager` hands out the nonce in the same `try_join!` as the gas
/// estimate and never takes it back, so after one failed `sendSnapshot` the next plain
/// `.send()` went out one above the chain's pending nonce and never mined; route
/// restarts reuse the provider, so only a process restart cleared it.
///
/// Driven through the dispatcher, whose signature is the same before and after the fix,
/// so the test compiles at both commits. Before the fix `process_pending` sits on the
/// queued SaveSnapshot tx until `RECEIPT_TIMEOUT` (120 s), longer than this test waits;
/// if the node rejected the gapped tx instead, the snapshot stays zero. (At the old
/// commit the gapped tx stays in anvil's pool across `evm_revert`: restart the devnet
/// after running it there.)
#[tokio::test]
#[serial]
async fn test_s18_failed_arbitrum_send_leaves_no_nonce_gap() {
    use vea_validator::tasks::{ClaimData, Task};

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    // Plain sends on the route's own provider: primes its cached nonce manager, as the
    // validator's earlier sends would have. (Unprimed, the first fetch races the
    // estimate and may be dropped with it, hiding the gap.)
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    let n = route.inbox_provider.get_transaction_count(wallet).pending().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    task_store.lock().unwrap().set_on_sync(true);
    // A claim for the current epoch: the inbox refuses its snapshot until the epoch is
    // over ("Can only send past epoch snapshot."), so the estimate reverts.
    claim_store.lock().unwrap().store(ClaimData {
        epoch, state_root: FixedBytes::<32>::from(WRONG_ROOT), claimer: wallet, timestamp_claimed: 1,
        timestamp_verification: 0, blocknumber_verification: 0, honest: "None".into(), challenger: wallet,
    });
    // Store order is execution order: the failing send first, then the one that must land.
    task_store.lock().unwrap().add_task(Task { epoch, execute_after: 0, kind: TaskKind::SendSnapshot });
    task_store.lock().unwrap().add_task(Task { epoch, execute_after: 0, kind: TaskKind::SaveSnapshot });

    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    let run = tokio::time::timeout(std::time::Duration::from_secs(60), dispatcher.process_pending()).await;
    assert!(run.is_ok(), "saveSnapshot after a failed sendSnapshot did not land: queued behind a nonce gap");

    assert_ne!(inbox.snapshots(U256::from(epoch)).call().await.unwrap(), FixedBytes::<32>::ZERO, "snapshot not saved");
    let latest = route.inbox_provider.get_transaction_count(wallet).latest().await.unwrap();
    let pending = route.inbox_provider.get_transaction_count(wallet).pending().await.unwrap();
    assert_eq!(latest, n + 1, "exactly the saveSnapshot was mined; the reverted sendSnapshot was never broadcast");
    assert_eq!(pending, latest, "nothing left queued behind a gap");
    let left: Vec<_> = task_store.lock().unwrap().load().tasks.into_iter().map(|t| t.kind.name()).collect();
    assert_eq!(left, vec!["SendSnapshot"], "SaveSnapshot done, SendSnapshot retried next poll");
}

/// S10: the stored `honest` must be what the outbox wrote, not a guess from the inbox
/// root. `verifySnapshot` writes Claimer whatever the root, so a dishonest claim that
/// nobody challenged in time (this validator was offline) and that passed the
/// censorship test is Claimer on-chain. The indexer stored Challenger (root differs
/// from the snapshot), and `withdrawChallengeDeposit` then reverted with "Invalid
/// claim." on every poll, forever.
#[tokio::test]
#[serial]
async fn test_s10_verified_unchallenged_dishonest_claim_is_stored_as_claimer() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let outbox_provider = route.outbox_provider.clone();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&outbox_provider, epoch, epoch_period).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), c.wallet.default_signer().address(), task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    task_store.lock().unwrap().set_on_sync(true);

    // The attacker's claim; the validator indexes it but never runs its tasks.
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), FixedBytes::<32>::from(WRONG_ROOT)).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;

    // Past sequencerDelayLimit + epochPeriod: the attacker starts verification...
    advance_time(25 * 3600 + 60).await;
    let claim = claim_store.lock().unwrap().get_claim(epoch);
    outbox.startVerification(U256::from(epoch), claim).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;

    // ...and, the censorship test passed, verifies the wrong root unchallenged.
    let min_challenge = outbox.minChallengePeriod().call().await.unwrap().to::<u64>();
    advance_time(min_challenge + 10).await;
    let claim = claim_store.lock().unwrap().get_claim(epoch);
    outbox.verifySnapshot(U256::from(epoch), claim).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;

    assert_eq!(claim_store.lock().unwrap().get(epoch).honest, "Claimer", "verifySnapshot writes Claimer whatever the root");
    let stored = claim_store.lock().unwrap().get_claim(epoch);
    assert_eq!(
        outbox.hashClaim(stored).call().await.unwrap(),
        outbox.claimHashes(U256::from(epoch)).call().await.unwrap(),
        "stored claim must hash to the on-chain claim"
    );

    // The withdrawal goes through instead of reverting on every poll.
    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    dispatcher.process_pending().await;
    assert_eq!(outbox.claimHashes(U256::from(epoch)).call().await.unwrap(), FixedBytes::<32>::ZERO, "deposit was not withdrawn");
    assert!(
        !task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::WithdrawDeposit)),
        "WithdrawDeposit should be done"
    );
}

/// S15: `Verified(uint256 _epoch)` does not index its epoch in either outbox, so the
/// frontrun check for `verifySnapshot` (which keyed on `topics[1]`) could never match.
/// A claim verified by someone else between our pre-check and our tx reverted "Invalid
/// claim." and was rescheduled every 30 minutes instead of dropped.
#[tokio::test]
#[serial]
async fn test_s15_verify_frontrun_is_recognised() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;
    // The other party's sends use fresh provider instances: a provider's cached nonce
    // filler is stale once a pinned send of ours went out on it.
    let fresh = || c.build_routes()[0].outbox_provider.clone();

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, route.outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    let root = inbox.snapshots(U256::from(epoch)).call().await.unwrap();
    advance_past_epoch(&route.outbox_provider, epoch, epoch_period).await;
    let deposit = outbox.deposit().call().await.unwrap();
    IVeaOutboxArbToEth::new(route.outbox_address, fresh()).claim(U256::from(epoch), root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), wallet, task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    task_store.lock().unwrap().set_on_sync(true);
    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    indexer.scan_once().await;
    dispatcher.process_pending().await;
    advance_time(25 * 3600 + 10).await;
    dispatcher.process_pending().await;
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;
    assert!(task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::VerifySnapshot)), "precondition: VerifySnapshot scheduled");

    // Someone else verifies first, with the struct the validator holds.
    let min_challenge = outbox.minChallengePeriod().call().await.unwrap().to::<u64>();
    advance_time(min_challenge + 10).await;
    let claim = claim_store.lock().unwrap().get_claim(epoch);
    IVeaOutboxArbToEth::new(route.outbox_address, fresh()).verifySnapshot(U256::from(epoch), claim).send().await.unwrap().get_receipt().await.unwrap();

    // Our attempt reverts ("Invalid claim.": honest changed); the Verified log must be
    // recognised and the task dropped, not rescheduled.
    dispatcher.process_pending().await;
    assert!(
        !task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::VerifySnapshot)),
        "VerifySnapshot should be dropped after another party's Verified"
    );
}

/// S16: the "state root already in a pending claim" skip could be griefed. Someone who
/// claims a past epoch with the next epoch's root (dishonest, that deposit is lost to
/// our challenge) made this validator skip claiming the next epoch. The skip now counts
/// only an honest pending claim: one whose epoch's final snapshot is that root.
#[tokio::test]
#[serial]
async fn test_s16_dishonest_claim_with_our_root_does_not_block_our_claim() {
    use vea_validator::tasks::claim;

    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, route.outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&route.outbox_provider, epoch, epoch_period).await;

    // In epoch + 1: more messages, a snapshot for epoch + 1 saved early (its root is
    // public), and the griefer claims `epoch` with that root.
    send_messages(&route).await;
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    let next_root = inbox.snapshots(U256::from(epoch + 1)).call().await.unwrap();
    assert_ne!(next_root, inbox.snapshots(U256::from(epoch)).call().await.unwrap(), "precondition: distinct roots");
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), next_root).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), wallet, task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    task_store.lock().unwrap().set_on_sync(true);
    indexer.scan_once().await;
    assert!(claim_store.lock().unwrap().exists(epoch), "precondition: the griefer's claim is known");

    // Epoch + 1 is over; this validator wants to claim it with `next_root`.
    advance_past_epoch(&route.outbox_provider, epoch + 1, epoch_period).await;
    advance_time(20 * 60).await;
    let now = route.outbox_provider.get_block_by_number(Default::default()).await.unwrap().unwrap().header.timestamp;
    claim::execute(&c, &route, epoch + 1, &claim_store, now, &task_store).await.unwrap();
    assert_ne!(outbox.claimHashes(U256::from(epoch + 1)).call().await.unwrap(), FixedBytes::<32>::ZERO, "our claim for epoch + 1 was skipped");
}
/// S13: an RPC failure inside an indexer event handler must not panic the route. Before
/// the fix every state read in the handlers was an `expect`: one transient error or
/// `null` from a fallback transport cost a route restart (5 minutes) plus a resync. Now
/// the handler returns `Err`, `scan_chain` leaves the cursor where it was and rescans the
/// chunk on the next cycle.
///
/// Driven through `dispatch_outbox_event` with a hand-built `Verified` log and an outbox
/// provider nothing listens on (connection refused at once), so the first state read,
/// `claimHashes(epoch)`, fails. Needs neither the devnet nor `.env.local`. At b797b88 the
/// method is private: make it `pub` there to watch the `expect` panic.
#[tokio::test]
#[serial]
async fn test_s13_rpc_failure_in_event_handler_returns_err_instead_of_panicking() {
    use alloy::primitives::{keccak256, Bytes};
    use alloy::providers::{DynProvider, ProviderBuilder};
    use vea_validator::config::{Route, RouteSettings};
    use vea_validator::tasks::ClaimData;

    let dead = || DynProvider::new(ProviderBuilder::new().connect_http("http://127.0.0.1:9".parse().unwrap()));
    let route = Route {
        name: "ARB_TO_ETH",
        inbox_chain_id: 42161,
        inbox_address: Address::with_last_byte(0x1A),
        inbox_provider: dead(),
        inbox_avg_block_millis: 250,
        outbox_chain_id: 1,
        outbox_address: Address::with_last_byte(0x0B),
        outbox_provider: dead(),
        weth_address: None,
        settings: RouteSettings::test_defaults(),
    };

    let epoch = 42u64;
    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    // A known, unchallenged claim: the handler gets past `ensure_claim_known` and makes
    // its first RPC call, `claimHashes(epoch)`.
    claim_store.lock().unwrap().store(ClaimData {
        epoch, state_root: FixedBytes::<32>::from(WRONG_ROOT), claimer: Address::with_last_byte(0xC1), timestamp_claimed: 1,
        timestamp_verification: 0, blocknumber_verification: 0, honest: "None".into(), challenger: Address::ZERO,
    });
    let indexer = EventIndexer::new(route.clone(), Address::ZERO, task_store.clone(), claim_store.clone());

    let verified = alloy::rpc::types::Log {
        inner: alloy::primitives::Log::new_unchecked(
            route.outbox_address,
            vec![keccak256("Verified(uint256)"), FixedBytes::<32>::from(U256::from(epoch).to_be_bytes::<32>())],
            Bytes::new(),
        ),
        block_hash: None,
        block_number: Some(1),
        // Set, so the handler needs no block fetch for the timestamp.
        block_timestamp: Some(1_700_000_000),
        transaction_hash: None,
        transaction_index: None,
        log_index: None,
        removed: false,
    };

    let outcome = AssertUnwindSafe(indexer.dispatch_outbox_event(&verified)).catch_unwind().await;
    let result = match outcome {
        Ok(r) => r,
        Err(_) => panic!("indexer panicked on an RPC failure inside an event handler"),
    };
    assert!(result.is_err(), "an unreachable outbox RPC must surface as Err so the chunk is rescanned");

    // Nothing was decided on the failed read: no withdrawal scheduled, claim untouched.
    assert!(task_store.lock().unwrap().load().tasks.is_empty(), "no task may be scheduled from a failed state read");
    let claim = claim_store.lock().unwrap().get(epoch);
    assert_eq!(claim.honest, "None", "stored claim must not change on a failed read");
}

/// Panel F1: `resolveDisputedClaim` for an unchallenged claim with a wrong root emits
/// `Verified` but leaves `honest = None`; the outbox still accepts `challenge` and the
/// claimer can still start and pass verification. The handler used to invalidate
/// ValidateClaim/Challenge on every Verified, so a validator catching up on such a claim
/// (offline while the week-long bridge resolution ran) never challenged it.
#[tokio::test]
#[serial]
async fn test_panel_verified_without_honest_party_keeps_claim_challengeable() {
    let c = ValidatorConfig::from_env().unwrap();
    let route = c.build_routes()[0].clone();
    let wallet = c.wallet.default_signer().address();
    restore_pristine().await;

    let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
    let outbox = IVeaOutboxArbToEth::new(route.outbox_address, route.outbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await.unwrap().try_into().unwrap();
    send_messages(&route).await;
    let epoch: u64 = inbox.epochNow().call().await.unwrap().try_into().unwrap();
    inbox.saveSnapshot().send().await.unwrap().get_receipt().await.unwrap();
    advance_past_epoch(&route.outbox_provider, epoch, epoch_period).await;
    let deposit = outbox.deposit().call().await.unwrap();
    outbox.claim(U256::from(epoch), FixedBytes::<32>::from(WRONG_ROOT)).value(deposit).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;

    let dir = tempfile::tempdir().unwrap();
    let (task_store, claim_store) = stores(&dir);
    let indexer = EventIndexer::new(route.clone(), wallet, task_store.clone(), claim_store.clone());
    indexer.initialize().await;
    task_store.lock().unwrap().set_on_sync(true);
    indexer.scan_once().await;
    // The validator knows the claim but never got to validate it (offline).
    task_store.lock().unwrap().invalidate_tasks(epoch, &["ValidateClaim"]);

    // Anyone sends the unchallenged snapshot through the bridge; the validator relays
    // its own SnapshotSent a week later and the outbox resolves with honest = None.
    let claim = claim_store.lock().unwrap().get_claim(epoch);
    inbox.sendSnapshot(U256::from(epoch), claim).send().await.unwrap().get_receipt().await.unwrap();
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;
    let dispatcher = TaskDispatcher::new(c.clone(), route.clone(), task_store.clone(), claim_store.clone());
    advance_time(7 * 24 * 3600 + 3600 + 10).await;
    dispatcher.process_pending().await;
    let sig = alloy::primitives::keccak256("Verified(uint256)");
    let filter = alloy::rpc::types::Filter::new().address(route.outbox_address).event_signature(sig).from_block(0u64);
    assert!(!route.outbox_provider.get_logs(&filter).await.unwrap().is_empty(), "precondition: Verified emitted by the bridge resolution");
    advance_time(20 * 60 + 10).await;
    indexer.scan_once().await;

    let stored = claim_store.lock().unwrap().get(epoch);
    assert_eq!(stored.honest, "None", "the contract left no honest party");
    assert!(
        task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::ValidateClaim | TaskKind::Challenge)),
        "a still-challengeable claim must keep (or get back) its ValidateClaim/Challenge task"
    );

    // And the claim does get challenged.
    dispatcher.process_pending().await;
    dispatcher.process_pending().await;
    assert!(challenged_epochs(&route.outbox_provider, route.outbox_address).await.contains(&epoch), "dishonest claim left unchallenged after a None resolution");
}
