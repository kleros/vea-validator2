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
