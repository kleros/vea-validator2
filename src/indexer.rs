use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{sleep, Duration};
use tracing::{info, warn, error};

use crate::config::Route;
use crate::contracts::{IVeaOutbox, IArbSys};
use crate::tasks::{Task, TaskKind, TaskStore, ClaimStore, ClaimData};
use crate::{find_block_by_timestamp, find_block_by_timestamp_from};

use alloy::network::Ethereum;
use alloy::providers::DynProvider;

enum ScanTarget { Inbox, Outbox }

const CHUNK_SIZE: u64 = 2000;
const FINALITY_BUFFER_SECS: u64 = 20 * 60;
const CATCHUP_SLEEP: Duration = Duration::from_secs(1);
const IDLE_SLEEP: Duration = Duration::from_secs(5 * 60);
const ARB_SYS: Address = Address::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x64]);

/// Why an event handler could not finish: an RPC error, a `null` answer for
/// something the log proves exists (a lagging node), a state read that contradicts
/// the log (also a lagging node), or a malformed log. `scan_chain` stops the chunk on
/// the first one and rescans it next cycle (S13); nothing a handler does before the
/// failing read is lost, and replaying the chunk's earlier events is idempotent.
pub type IndexError = Box<dyn std::error::Error + Send + Sync>;

async fn get_log_timestamp(log: &alloy::rpc::types::Log, provider: &DynProvider<Ethereum>) -> Result<u64, IndexError> {
    if let Some(ts) = log.block_timestamp {
        return Ok(ts);
    }
    let block_num = log.block_number.ok_or("log missing block_number")?;
    let block = provider.get_block_by_number(block_num.into()).await?
        .ok_or_else(|| format!("block {block_num} not found (lagging node?)"))?;
    Ok(block.header.timestamp)
}

pub struct EventIndexer {
    route: Route,
    wallet_address: Address,
    task_store: Arc<Mutex<TaskStore>>,
    claim_store: Arc<Mutex<ClaimStore>>,
    inbox_catchup: (AtomicU64, AtomicU64, AtomicU64),
    outbox_catchup: (AtomicU64, AtomicU64, AtomicU64),
}

impl EventIndexer {
    pub fn new(
        route: Route,
        wallet_address: Address,
        task_store: Arc<Mutex<TaskStore>>,
        claim_store: Arc<Mutex<ClaimStore>>,
    ) -> Self {
        Self {
            route,
            wallet_address,
            task_store,
            claim_store,
            inbox_catchup: (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)),
            outbox_catchup: (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)),
        }
    }

    pub async fn initialize(&self) {
        self.task_store.lock().unwrap().set_on_sync(false);
        let state = self.task_store.lock().unwrap().load();

        let inbox_now = self.route.inbox_provider.get_block_by_number(Default::default()).await
            .expect("Failed to get inbox block")
            .expect("Inbox block not found")
            .header.timestamp;

        let needs_init = match (state.inbox_last_block, state.outbox_last_block) {
            (None, _) | (_, None) => true,
            (Some(inbox_b), Some(outbox_b)) => {
                let inbox_ts = self.route.inbox_provider.get_block_by_number(inbox_b.into()).await
                    .expect("Failed to get inbox last block")
                    .expect("Inbox last block not found")
                    .header.timestamp;
                let outbox_ts = self.route.outbox_provider.get_block_by_number(outbox_b.into()).await
                    .expect("Failed to get outbox last block")
                    .expect("Outbox last block not found")
                    .header.timestamp;
                inbox_ts < inbox_now.saturating_sub(self.route.settings.sync_lookback_secs)
                    || outbox_ts < inbox_now.saturating_sub(self.route.settings.sync_lookback_secs)
            }
        };

        if needs_init {
            let indexing_since = inbox_now.saturating_sub(self.route.settings.sync_lookback_secs);
            let inbox_start = find_block_by_timestamp(&self.route.inbox_provider, indexing_since).await;
            let outbox_start = find_block_by_timestamp(&self.route.outbox_provider, indexing_since).await;
            self.task_store.lock().unwrap().initialize_sync(indexing_since, inbox_start, outbox_start);
            info!(logger = "Indexer", route = self.route.name, indexing_since, inbox_start, outbox_start, "Initialized sync");
        }
    }

    pub async fn run(&self) {
        loop {
            let done = self.scan_once().await;
            if done {
                if !self.task_store.lock().unwrap().is_on_sync() {
                    info!(logger = "Indexer", route = self.route.name, "Sync complete");
                    self.task_store.lock().unwrap().set_on_sync(true);
                }
                sleep(IDLE_SLEEP).await;
            } else {
                sleep(CATCHUP_SLEEP).await;
            }
        }
    }

    pub async fn scan_once(&self) -> bool {
        let inbox_done = self.scan_chain(ScanTarget::Inbox).await;
        let outbox_done = self.scan_chain(ScanTarget::Outbox).await;
        inbox_done && outbox_done
    }

    async fn scan_chain(&self, target: ScanTarget) -> bool {
        use ScanTarget::*;

        let (provider, address, label, catchup) = match target {
            Inbox => (&self.route.inbox_provider, self.route.inbox_address, "Inbox", &self.inbox_catchup),
            Outbox => (&self.route.outbox_provider, self.route.outbox_address, "Outbox", &self.outbox_catchup),
        };

        let current_block_data = match provider.get_block_by_number(Default::default()).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                warn!(logger = "Indexer", route = self.route.name, chain = label, "Latest block not found");
                return false;
            }
            Err(e) => {
                warn!(logger = "Indexer", route = self.route.name, chain = label, "Failed to get block data: {e}");
                return false;
            }
        };
        let now = current_block_data.header.timestamp;

        let state = self.task_store.lock().unwrap().load();
        let from_block = match target {
            Inbox => state.inbox_last_block.expect("inbox_last_block not set"),
            Outbox => state.outbox_last_block.expect("outbox_last_block not set"),
        };

        let (catchup_start, catchup_target, last_logged_pct) = catchup;
        // The last target doubles as the lower bound for the next search: the target
        // timestamp only moves forward, so the answer can never fall below it.
        let previous_target = catchup_target.load(Ordering::Relaxed);
        let mut target_block = previous_target;

        if target_block == 0 || from_block >= target_block {
            let target_ts = now.saturating_sub(FINALITY_BUFFER_SECS);
            target_block = find_block_by_timestamp_from(provider, target_ts, previous_target).await;
            catchup_target.store(target_block, Ordering::Relaxed);
        }

        if from_block >= target_block {
            catchup_start.store(0, Ordering::Relaxed);
            // `catchup_target` is deliberately kept: zeroing it here is what forced the
            // next idle cycle to re-search the chain from block 0.
            last_logged_pct.store(0, Ordering::Relaxed);
            return true;
        }

        if catchup_start.load(Ordering::Relaxed) == 0 {
            catchup_start.store(from_block, Ordering::Relaxed);
            last_logged_pct.store(0, Ordering::Relaxed);
        }

        let to_block = min(from_block + CHUNK_SIZE, target_block);

        let event_sigs: Vec<FixedBytes<32>> = match target {
            Inbox => vec![
                alloy::primitives::keccak256("SnapshotSent(uint256,bytes32)"),
                alloy::primitives::keccak256("SnapshotSaved(bytes32,uint256,uint64)"),
            ],
            Outbox => vec![
                alloy::primitives::keccak256("Claimed(address,uint256,bytes32)"),
                alloy::primitives::keccak256("VerificationStarted(uint256)"),
                alloy::primitives::keccak256("Challenged(uint256,address)"),
                alloy::primitives::keccak256("Verified(uint256)"),
            ],
        };

        let filter = Filter::new()
            .address(address)
            .event_signature(event_sigs)
            .from_block(from_block)
            .to_block(to_block);

        match provider.get_logs(&filter).await {
            Ok(logs) => {
                for log in logs {
                    let handled = match target {
                        Inbox => self.dispatch_inbox_event(&log).await,
                        Outbox => self.dispatch_outbox_event(&log).await,
                    };
                    if let Err(e) = handled {
                        // The cursor stays put, so the whole chunk is rescanned next
                        // cycle; the events before this one replay idempotently (S13).
                        warn!(logger = "Indexer", route = self.route.name, chain = label, block = log.block_number.unwrap_or(0), from_block, to_block, "Failed to process event, chunk will be rescanned: {e}");
                        return false;
                    }
                }

                match target {
                    Inbox => self.task_store.lock().unwrap().update_inbox_block(to_block),
                    Outbox => self.task_store.lock().unwrap().update_outbox_block(to_block),
                }

                let is_done = to_block >= target_block;

                let start = catchup_start.load(Ordering::Relaxed);
                if start != 0 && !is_done {
                    let total = target_block.saturating_sub(start);
                    let done = to_block.saturating_sub(start);
                    let pct = if total > 0 { (done * 100) / total } else { 100 };
                    let last_pct = last_logged_pct.load(Ordering::Relaxed);
                    if pct / 10 > last_pct / 10 {
                        info!(logger = "Indexer", route = self.route.name, chain = label, progress = pct, "Syncing");
                        last_logged_pct.store(pct, Ordering::Relaxed);
                    }
                }
                if is_done {
                    catchup_start.store(0, Ordering::Relaxed);
                    catchup_target.store(0, Ordering::Relaxed);
                    last_logged_pct.store(0, Ordering::Relaxed);
                }
                is_done
            }
            Err(e) => {
                warn!(logger = "Indexer", route = self.route.name, chain = label, from_block, to_block, "Failed to query logs: {e}");
                false
            }
        }
    }

    async fn dispatch_inbox_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        let topic0 = match log.topics().first() {
            Some(t) => *t,
            None => return Ok(()),
        };

        if topic0 == alloy::primitives::keccak256("SnapshotSent(uint256,bytes32)") {
            self.handle_snapshot_sent(log).await?;
        } else if topic0 == alloy::primitives::keccak256("SnapshotSaved(bytes32,uint256,uint64)") {
            self.handle_snapshot_saved(log);
        }
        Ok(())
    }

    /// Public only so a test can drive one outbox log through the handlers with a
    /// provider of its choosing; `scan_once` is the real entry point.
    #[doc(hidden)]
    pub async fn dispatch_outbox_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        let topic0 = match log.topics().first() {
            Some(t) => *t,
            None => return Ok(()),
        };

        if topic0 == alloy::primitives::keccak256("Claimed(address,uint256,bytes32)") {
            self.handle_claimed_event(log).await?;
        } else if topic0 == alloy::primitives::keccak256("VerificationStarted(uint256)") {
            self.handle_verification_started_event(log).await?;
        } else if topic0 == alloy::primitives::keccak256("Challenged(uint256,address)") {
            self.handle_challenged_event(log).await?;
        } else if topic0 == alloy::primitives::keccak256("Verified(uint256)") {
            self.handle_verified_event(log).await?;
        }
        Ok(())
    }

    fn handle_snapshot_saved(&self, log: &alloy::rpc::types::Log) {
        if log.data().data.len() < 96 {
            return;
        }
        let count = U256::from_be_slice(&log.data().data[64..96]).to::<u64>();
        self.task_store.lock().unwrap().set_last_saved_count(count);
        info!(logger = "Indexer", route = self.route.name, count, "SnapshotSaved");
    }

    async fn handle_snapshot_sent(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        let epoch = match self.parse_epoch_from_snapshot_sent(log) {
            Some(e) => e,
            None => return Ok(()),
        };

        let state = self.task_store.lock().unwrap().load();
        if state.tasks.iter().any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::ExecuteRelay { .. })) {
            return Ok(());
        }

        let tx_hash = match log.transaction_hash {
            Some(h) => h,
            None => return Ok(()),
        };

        // The log proves the tx exists: `null` here is a node that has not got it yet.
        let tx = self.route.inbox_provider.get_transaction_by_hash(tx_hash).await?
            .ok_or_else(|| format!("transaction {tx_hash:#x} not found (lagging node?)"))?;
        if tx.inner.signer() != self.wallet_address {
            info!(logger = "Indexer", route = self.route.name, epoch, "SnapshotSent not from validator, skipping");
            return Ok(());
        }

        match self.fetch_l2_to_l1_from_tx(tx_hash, epoch).await? {
            Some(task) => {
                info!(logger = "Indexer", route = self.route.name, epoch, position = %format!("{:#x}", task.2), "Found SnapshotSent");
                self.task_store.lock().unwrap().add_task(Task {
                    epoch: task.0,
                    execute_after: task.1,
                    kind: TaskKind::ExecuteRelay {
                        position: task.2,
                        l2_sender: task.3,
                        dest_addr: task.4,
                        l2_block: task.5,
                        l1_block: task.6,
                        l2_timestamp: task.7,
                        amount: task.8,
                        data: task.9,
                    },
                });
            }
            None => {
                error!(logger = "Indexer", route = self.route.name, tx = ?tx_hash, "No L2ToL1Tx found in tx");
            }
        }
        Ok(())
    }

    async fn handle_claimed_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        if log.topics().len() < 3 {
            return Ok(());
        }

        let claimer = Address::from_slice(&log.topics()[1].0[12..]);
        let epoch = U256::from_be_bytes(log.topics()[2].0).to::<u64>();
        info!(logger = "Indexer", route = self.route.name, epoch, block = log.block_number.unwrap_or(0), "Claimed event");

        if log.data().data.len() < 32 {
            return Ok(());
        }
        let state_root = FixedBytes::<32>::from_slice(&log.data().data[0..32]);

        let block_ts = get_log_timestamp(log, &self.route.outbox_provider).await?;
        let timestamp_claimed = block_ts as u32;

        // Dedup on the claim: a crash between `store()` and `add_task()` replays this log.
        if self.claim_store.lock().unwrap().exists(epoch) {
            let has_task = self.task_store.lock().unwrap().load().tasks.iter().any(|t| t.epoch == epoch);
            let claim = self.claim_store.lock().unwrap().get(epoch);
            let moved_on = claim.timestamp_verification != 0
                || claim.challenger != Address::ZERO
                || claim.honest != "None";
            if !has_task && !moved_on {
                info!(logger = "Indexer", route = self.route.name, epoch, "Claim exists without a task, re-adding ValidateClaim");
                self.task_store.lock().unwrap().add_task(Task {
                    epoch,
                    execute_after: block_ts,
                    kind: TaskKind::ValidateClaim,
                });
            }
            return Ok(());
        }

        self.claim_store.lock().unwrap().store(ClaimData {
            epoch,
            state_root,
            claimer,
            timestamp_claimed,
            timestamp_verification: 0,
            blocknumber_verification: 0,
            honest: "None".to_string(),
            challenger: Address::ZERO,
        });

        self.task_store.lock().unwrap().add_task(Task {
            epoch,
            execute_after: block_ts,
            kind: TaskKind::ValidateClaim,
        });
        Ok(())
    }

    async fn handle_verification_started_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        if log.topics().len() < 2 {
            return Ok(());
        }

        let epoch = U256::from_be_bytes(log.topics()[1].0).to::<u64>();
        info!(logger = "Indexer", route = self.route.name, epoch, block = log.block_number.unwrap_or(0), "VerificationStarted event");

        if !self.ensure_claim_known(epoch, log).await? {
            return Ok(());
        }

        let block_ts = get_log_timestamp(log, &self.route.outbox_provider).await? as u32;
        let block_num = log.block_number.ok_or("log missing block_number")? as u32;

        self.claim_store.lock().unwrap().update(epoch, |c| {
            c.timestamp_verification = block_ts;
            c.blocknumber_verification = block_num;
        });

        self.task_store.lock().unwrap().invalidate_tasks(epoch, &["StartVerification"]);

        let execute_after = (block_ts as u64) + self.route.settings.min_challenge_period;

        self.task_store.lock().unwrap().add_task(Task {
            epoch,
            execute_after,
            kind: TaskKind::VerifySnapshot,
        });
        Ok(())
    }

    async fn handle_challenged_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        if log.topics().len() < 3 {
            return Ok(());
        }

        let epoch = U256::from_be_bytes(log.topics()[1].0).to::<u64>();
        let challenger = Address::from_slice(&log.topics()[2].0[12..]);
        info!(logger = "Indexer", route = self.route.name, epoch, block = log.block_number.unwrap_or(0), "Challenged event");

        if !self.ensure_claim_known(epoch, log).await? {
            return Ok(());
        }

        // Already handled: a chunk rescan (S13) replays this log, and the SendSnapshot it
        // scheduled may have run in between; task dedup alone would re-add it and send
        // the snapshot twice. The challenger is written last, so a crash between the
        // two writes still replays into the scheduling below.
        if self.claim_store.lock().unwrap().get(epoch).challenger == challenger {
            return Ok(());
        }

        self.task_store.lock().unwrap().invalidate_tasks(epoch, &["ValidateClaim", "Challenge", "StartVerification", "VerifySnapshot"]);

        let block_ts = get_log_timestamp(log, &self.route.outbox_provider).await?;
        self.task_store.lock().unwrap().add_task(Task {
            epoch,
            execute_after: block_ts,
            kind: TaskKind::SendSnapshot,
        });

        self.claim_store.lock().unwrap().update(epoch, |c| {
            c.challenger = challenger;
        });
        Ok(())
    }

    async fn handle_verified_event(&self, log: &alloy::rpc::types::Log) -> Result<(), IndexError> {
        let epoch = if log.topics().len() >= 2 {
            U256::from_be_bytes(log.topics()[1].0).to::<u64>()
        } else if log.data().data.len() >= 32 {
            U256::from_be_slice(&log.data().data[0..32]).to::<u64>()
        } else {
            return Ok(());
        };
        info!(logger = "Indexer", route = self.route.name, epoch, block = log.block_number.unwrap_or(0), "Verified event");

        let block_ts = get_log_timestamp(log, &self.route.outbox_provider).await?;

        if !self.ensure_claim_known(epoch, log).await? {
            return Ok(());
        }

        // The resolution is in, one way or another: the dispute path is over.
        self.task_store.lock().unwrap().invalidate_tasks(epoch, &["SendSnapshot", "ExecuteRelay"]);

        // The state read below is at `latest`; a node whose head is still before this
        // log would answer with the pre-resolution hash, which for an unchallenged claim
        // is indistinguishable from the real `None` outcome. Refuse such a node here so
        // the chunk is rescanned instead (partial: the two requests may hit different
        // transports of a fallback set).
        let head = self.route.outbox_provider.get_block_number().await?;
        let log_block = log.block_number.ok_or("log missing block_number")?;
        if head < log_block {
            return Err(format!("outbox RPC head {head} is before the Verified log's block {log_block} (lagging node?)").into());
        }

        // `honest` is read back from the chain, not guessed from the inbox root (S10).
        // `verifySnapshot` writes Claimer whatever the root; `resolveDisputedClaim`
        // writes Claimer when the bridged root matches the claim, Challenger when it
        // does not and the claim was challenged, and leaves None for an unchallenged
        // claim with a wrong root. The log does not tell the paths apart, but the hash
        // the outbox stores does: the stored struct must hash to it, or every later
        // call with it reverts "Invalid claim.".
        let claim = self.claim_store.lock().unwrap().get(epoch);
        let outbox = IVeaOutbox::new(self.route.outbox_address, self.route.outbox_provider.clone());
        let claim_hash = outbox.claimHashes(U256::from(epoch)).call().await?;
        let honest = match claim.honest_party(claim_hash) {
            Some(h) => h,
            None if claim_hash == FixedBytes::<32>::ZERO => {
                info!(logger = "Indexer", route = self.route.name, epoch, "Claim already withdrawn");
                self.claim_store.lock().unwrap().remove(epoch);
                return Ok(());
            }
            None => {
                error!(logger = "Indexer", route = self.route.name, epoch, claim_hash = ?claim_hash, "Stored claim hashes to the on-chain claim for no honest party, not withdrawing");
                return Ok(());
            }
        };
        if honest == "None" && claim.challenger != Address::ZERO {
            // Only an unchallenged claim can be resolved and still have no honest party
            // (a challenged one is Claimer or Challenger after Verified), so for a
            // challenged claim this is a stale state read from a lagging node: fail the
            // chunk and rescan, as for any other indexer RPC failure, instead of losing
            // the challenger's withdrawal. Checked before the store is touched: on a
            // rescan of an already handled event the stale hash would otherwise
            // overwrite the stored Challenger with None, and the dispatcher drops a
            // WithdrawDeposit task that finds honest == None.
            return Err(format!("claimHashes[{epoch}] still hashes with honest = None after Verified of a challenged claim: stale RPC state").into());
        }
        self.claim_store.lock().unwrap().update(epoch, |c| {
            c.honest = honest.to_string();
        });

        if honest == "None" {
            // Neither withdrawal applies, and the claim is still live: the outbox still
            // accepts `challenge` (challenger == 0, honest == None) and the claimer can
            // still start and pass verification. Its root is wrong (a matching root
            // would have made it Claimer), so it must still be validated and challenged:
            // keep those tasks, and re-add ValidateClaim if neither is pending.
            warn!(logger = "Indexer", route = self.route.name, epoch, "Verified without an honest party (unchallenged claim with a wrong root resolved through the bridge), nothing to withdraw; claim stays challengeable");
            let pending = self.task_store.lock().unwrap().load().tasks.iter()
                .any(|t| t.epoch == epoch && matches!(t.kind, TaskKind::ValidateClaim | TaskKind::Challenge));
            if !pending {
                self.task_store.lock().unwrap().add_task(Task {
                    epoch,
                    execute_after: block_ts,
                    kind: TaskKind::ValidateClaim,
                });
            }
            return Ok(());
        }
        self.task_store.lock().unwrap().invalidate_tasks(epoch, &["ValidateClaim", "Challenge", "StartVerification", "VerifySnapshot"]);
        self.task_store.lock().unwrap().add_task(Task {
            epoch,
            execute_after: block_ts,
            kind: TaskKind::WithdrawDeposit,
        });
        Ok(())
    }

    /// Make sure the claim behind an outbox event for `epoch` is in the `ClaimStore`.
    ///
    /// A claim made before the sync window was never seen, yet its later events can
    /// arrive at any time: `startVerification` and `verifySnapshot` have no deadline.
    /// Dropping such an event would leave a dishonest claim unvalidated, and treating it
    /// as impossible (a panic) stalls the route for good, since the cursor never moves
    /// past the log. Instead the claim is rebuilt from the chain (see
    /// [`Self::backfill_claim`]). Returns false, after logging, only when no `Claimed`
    /// log exists for the epoch, in which case the event is dropped.
    async fn ensure_claim_known(&self, epoch: u64, log: &alloy::rpc::types::Log) -> Result<bool, IndexError> {
        if self.claim_store.lock().unwrap().exists(epoch) {
            return Ok(true);
        }
        let before_block = log.block_number.ok_or("log missing block_number")?;
        if self.backfill_claim(epoch, before_block).await? {
            return Ok(true);
        }
        warn!(logger = "Indexer", route = self.route.name, epoch, block = before_block, "No Claimed event found for epoch, dropping event");
        Ok(false)
    }

    /// Rebuild a claim from its `Claimed` log and replay the epoch's later
    /// `VerificationStarted`/`Challenged` logs that precede `before_block`, so the
    /// stored claim hashes to what the outbox holds.
    ///
    /// The outbox only accepts a claim for `epoch` while its own clock is in
    /// `epoch + 1`, so the `Claimed` log lies in a one-epoch window of outbox blocks.
    /// All logs are fetched before anything is stored, so an RPC failure (an error,
    /// like the indexer's other RPC failures; the chunk is rescanned next cycle)
    /// cannot leave a half-built claim behind.
    async fn backfill_claim(&self, epoch: u64, before_block: u64) -> Result<bool, IndexError> {
        let provider = &self.route.outbox_provider;
        let outbox = IVeaOutbox::new(self.route.outbox_address, provider.clone());
        let epoch_period = outbox.epochPeriod().call().await?.to::<u64>();

        let window_start = find_block_by_timestamp(provider, (epoch + 1) * epoch_period).await;
        let window_end = find_block_by_timestamp(provider, (epoch + 2) * epoch_period).await.min(before_block);
        let claimed = Filter::new()
            .address(self.route.outbox_address)
            .event_signature(alloy::primitives::keccak256("Claimed(address,uint256,bytes32)"))
            .topic2(U256::from(epoch))
            .from_block(window_start)
            .to_block(window_end);
        let claim_log = match provider.get_logs(&claimed).await?.into_iter().next() {
            Some(l) => l,
            None => return Ok(false),
        };

        let claim_block = claim_log.block_number.ok_or("log missing block_number")?;
        let mut later = Vec::new();
        let mut from = claim_block;
        while from < before_block {
            let to = min(from + CHUNK_SIZE, before_block - 1);
            let filter = Filter::new()
                .address(self.route.outbox_address)
                .event_signature(vec![
                    alloy::primitives::keccak256("VerificationStarted(uint256)"),
                    alloy::primitives::keccak256("Challenged(uint256,address)"),
                ])
                .topic1(U256::from(epoch))
                .from_block(from)
                .to_block(to);
            later.extend(provider.get_logs(&filter).await?);
            from = to + 1;
        }

        info!(logger = "Indexer", route = self.route.name, epoch, claim_block, replayed = later.len(), "Backfilled claim from before the sync window");
        let replayed: Result<(), IndexError> = async {
            self.handle_claimed_event(&claim_log).await?;
            for l in &later {
                Box::pin(self.dispatch_outbox_event(l)).await?;
            }
            Ok(())
        }.await;
        if let Err(e) = replayed {
            // The claim must not survive half-built: `ensure_claim_known` would find it
            // on the rescan and skip the backfill, leaving the verification fields
            // unset, so every later call with it reverts "Invalid claim.". Roll back and
            // let the rescan rebuild it from scratch.
            warn!(logger = "Indexer", route = self.route.name, epoch, "Backfill replay failed, rolling the claim back: {e}");
            self.claim_store.lock().unwrap().remove(epoch);
            self.task_store.lock().unwrap().invalidate_tasks(epoch, &["ValidateClaim", "Challenge", "SendSnapshot", "StartVerification", "VerifySnapshot", "WithdrawDeposit"]);
            return Err(e);
        }
        Ok(true)
    }

    fn parse_epoch_from_snapshot_sent(&self, log: &alloy::rpc::types::Log) -> Option<u64> {
        if log.topics().len() < 2 {
            return None;
        }
        Some(U256::from_be_bytes(log.topics()[1].0).to::<u64>())
    }

    async fn fetch_l2_to_l1_from_tx(
        &self,
        tx_hash: FixedBytes<32>,
        epoch: u64,
    ) -> Result<Option<(u64, u64, U256, Address, Address, u64, u64, u64, U256, Bytes)>, IndexError> {
        // The log proves the tx is mined: `null` here is a node that has not got it yet.
        let receipt = self.route.inbox_provider.get_transaction_receipt(tx_hash).await?
            .ok_or_else(|| format!("receipt of {tx_hash:#x} not found (lagging node?)"))?;

        for log in receipt.inner.logs() {
            if log.address() != ARB_SYS {
                continue;
            }

            let decoded = match log.log_decode::<IArbSys::L2ToL1Tx>() {
                Ok(d) => d,
                Err(_) => continue,
            };

            let block_number = receipt.block_number.ok_or("receipt missing block_number")?;
            let block_timestamp = self.route.inbox_provider.get_block_by_number(block_number.into()).await?
                .ok_or_else(|| format!("block {block_number} not found (lagging node?)"))?
                .header.timestamp;

            return Ok(Some((
                epoch,
                block_timestamp + self.route.settings.relay_delay_secs,
                decoded.inner.position,
                decoded.inner.data.caller,
                decoded.inner.destination,
                decoded.inner.data.arbBlockNum.to::<u64>(),
                decoded.inner.data.ethBlockNum.to::<u64>(),
                decoded.inner.data.timestamp.to::<u64>(),
                decoded.inner.data.callvalue,
                decoded.inner.data.data.clone(),
            )));
        }
        Ok(None)
    }
}
