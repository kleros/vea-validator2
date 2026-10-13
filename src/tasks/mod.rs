pub mod dispatcher;
pub mod save_snapshot;
pub mod claim;
pub mod validate_claim;
pub mod challenge;
pub mod send_snapshot;
pub mod start_verification;
pub mod verify_snapshot;
pub mod execute_relay;
pub mod withdraw_deposit;

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::contract::Error as ContractError;
use alloy::network::Ethereum;
use alloy::providers::{DynProvider, PendingTransactionBuilder, Provider};
use alloy::rpc::types::Filter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn, error};

use crate::contracts::{Claim, Party};
use crate::RECEIPT_TIMEOUT;

fn decode_revert_reason(err_msg: &str) -> Option<String> {
    let data_prefix = "data: \"0x";
    let start = err_msg.find(data_prefix)? + data_prefix.len();
    let end = err_msg[start..].find('"')? + start;
    let hex_data = &err_msg[start..end];

    if hex_data.is_empty() {
        return Some("(empty)".to_string());
    }

    if hex_data.len() < 8 {
        return Some(format!("0x{}", hex_data));
    }

    let bytes = alloy::hex::decode(hex_data).ok()?;

    if bytes.len() >= 4 && bytes[0..4] == [0x08, 0xc3, 0x79, 0xa0] {
        if bytes.len() >= 68 {
            let offset = u32::from_be_bytes([bytes[4 + 28], bytes[4 + 29], bytes[4 + 30], bytes[4 + 31]]) as usize;
            if bytes.len() >= 36 + offset {
                let len_start = 4 + offset;
                let len = u32::from_be_bytes([bytes[len_start + 28], bytes[len_start + 29], bytes[len_start + 30], bytes[len_start + 31]]) as usize;
                let str_start = len_start + 32;
                if bytes.len() >= str_start + len {
                    return String::from_utf8(bytes[str_start..str_start + len].to_vec()).ok();
                }
            }
        }
    }

    if bytes.len() >= 4 && bytes[0..4] == [0x4e, 0x48, 0x7b, 0x71] {
        if bytes.len() >= 36 {
            let code = U256::from_be_slice(&bytes[4..36]);
            return Some(format!("Panic(0x{:02x})", code));
        }
    }

    if bytes.len() >= 4 {
        let selector: [u8; 4] = bytes[0..4].try_into().unwrap();
        let known_errors: &[(&str, usize)] = &[
            ("UnknownRoot(bytes32)", 32),
            ("AlreadySpent()", 0),
            ("ProofTooLong(uint256)", 32),
            ("NotOutbox(address)", 32),
        ];
        for (sig, param_len) in known_errors {
            let expected = &alloy::primitives::keccak256(sig)[..4];
            if selector == expected {
                if *param_len == 0 {
                    return Some(sig.to_string());
                } else if bytes.len() >= 4 + param_len {
                    return Some(format!("{}(0x{})", sig.split('(').next().unwrap(), alloy::hex::encode(&bytes[4..4 + param_len])));
                }
            }
        }
    }

    Some(format!("0x{}", hex_data))
}

pub async fn was_event_emitted(
    provider: &DynProvider<Ethereum>,
    address: Address,
    event_sig: &str,
    epoch: u64,
) -> bool {
    let current_block = match provider.get_block_number().await {
        Ok(b) => b,
        Err(_) => return false,
    };
    let from_block = current_block.saturating_sub(500);

    // Bounded at the head: a `toBlock` past it is rejected by some nodes (anvil answers
    // `BlockOutOfRangeError`), which made this check always answer "no", so a task whose
    // job another party did was rescheduled forever instead of dropped.
    let sig = alloy::primitives::keccak256(event_sig);
    let filter = Filter::new()
        .address(address)
        .event_signature(sig)
        .from_block(from_block)
        .to_block(current_block);

    match provider.get_logs(&filter).await {
        Ok(logs) => logs.iter().any(|log| {
            log.topics().get(1)
                .map(|t| U256::from_be_bytes(t.0).to::<u64>() == epoch)
                .unwrap_or(false)
        }),
        Err(_) => false,
    }
}

/// Write `contents` to `path` so a crash leaves either the old or the new complete file.
///
/// Writes `<path>.tmp`, fsyncs it, renames it over `path`, then fsyncs the parent
/// directory on a best-effort basis. A stale `.tmp` from a crash is simply overwritten
/// by the next save and is never read by the loaders.
fn atomic_write(path: &Path, contents: &str) -> std::io::Result<()> {
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp = PathBuf::from(tmp_name);
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// What a held pending record's saved nonce means given the confirmed (`latest`) nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NonceStatus {
    /// The confirmed nonce is above the saved one: the slot was mined.
    Consumed,
    /// Not above the saved nonce, or the confirmed nonce is unknown: keep holding.
    Open,
}

fn reconcile(saved: u64, confirmed: Option<u64>) -> NonceStatus {
    match confirmed {
        Some(c) if c > saved => NonceStatus::Consumed,
        _ => NonceStatus::Open,
    }
}

/// Shared by both hold paths: clear the record and return `NonceConsumed` when the
/// saved nonce was already mined, otherwise `PendingReplacement`.
fn hold_or_clear(
    task_store: &Arc<Mutex<TaskStore>>,
    key: &str,
    saved: u64,
    confirmed: Option<u64>,
    action: &str,
    epoch: u64,
    route_name: &str,
) -> Box<dyn std::error::Error + Send + Sync> {
    match reconcile(saved, confirmed) {
        NonceStatus::Consumed => {
            warn!(logger = "Task", route = route_name, action, epoch, saved_nonce = saved, confirmed_nonce = confirmed, "Pending nonce already mined, clearing record");
            task_store.lock().unwrap().clear_pending_tx(key);
            "NonceConsumed".into()
        }
        NonceStatus::Open => "PendingReplacement".into(),
    }
}

/// The lowest nonce this account has broadcast but not had confirmed, if any.
///
/// A fee bump only helps the transaction sitting at this nonce; anything numbered
/// above it cannot be mined until that one clears, so bumping those overpays without
/// unblocking anything.
///
/// Approximate by nature - mempool contents are node-local and `FallbackService` races
/// several transports - so this is used only to avoid pointless bumps, never as the
/// safety mechanism. Preventing a duplicate send is the pinned nonce's job, and that
/// never consults the mempool.
pub async fn blocking_nonce(provider: &DynProvider<Ethereum>, address: Address) -> Option<u64> {
    let confirmed = provider.get_transaction_count(address).latest().await.ok()?;
    let pending = provider.get_transaction_count(address).pending().await.ok()?;
    (pending > confirmed).then_some(confirmed)
}

/// One lock per chain for [`send_or_replace`]'s nonce selection and broadcast (S11).
///
/// Both routes' dispatchers, both epoch watchers and the relay sign with one wallet, and
/// on Ethereum the relay even uses a second provider instance. Two of them could read
/// the same `pending` nonce between each other's selection and broadcast: the second
/// broadcast is rejected as underpriced, and its next "bump" then replaces the first
/// one's transaction.
///
/// Keyed by the chain id the node reports, which is exactly the nonce space the senders
/// share (anvil's ids on the devnet differ from the canonical ones, but every provider
/// for one chain talks to the same node). The `std` mutex only guards the map and is
/// released within this expression, before anything is awaited; the `tokio` mutex is the
/// one held across awaits.
fn send_lock(chain_id: u64) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: std::sync::OnceLock<Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>> = std::sync::OnceLock::new();
    LOCKS.get_or_init(Default::default).lock().unwrap().entry(chain_id).or_default().clone()
}

/// Send `call`, or replace this task's in-flight transaction with a fee-bumped one at
/// the same nonce.
///
/// The nonce is pinned on every send, not just replacements. Pinning bypasses alloy's
/// `NonceFiller` (its `status()` returns `Finished` once a nonce is set), which is
/// correct for a replacement - it reuses a slot rather than claiming a new one - and
/// on a first send it means the nonce is known *before* broadcasting. Reading it back
/// afterwards would be unreliable: a node that has not yet seen the transaction answers
/// null, and losing the nonce would let the next cycle send a duplicate, which is the
/// exact failure this function exists to prevent.
pub async fn send_or_replace<P, D>(
    call: alloy::contract::CallBuilder<P, D, Ethereum>,
    provider: &DynProvider<Ethereum>,
    wallet_address: Address,
    task_store: &Arc<Mutex<TaskStore>>,
    epoch: u64,
    action: &str,
    route_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    P: Provider<Ethereum>,
    D: alloy::contract::CallDecoder,
{
    let key = pending_key(action, epoch);
    let market = provider.estimate_eip1559_fees().await?;

    // Held from nonce selection until the broadcast has returned, and released before
    // the receipt wait (S11): no other sender on this chain may pick a nonce while this
    // one is between selection and broadcast. Every early return below releases it
    // through the guard's drop. Nothing under it calls `send_or_replace`, so it cannot
    // deadlock, and the loops sharing a `select!` make progress while one of them waits.
    let chain_lock = send_lock(provider.get_chain_id().await?);
    let broadcasting = chain_lock.lock().await;

    // Bound to a local so the `MutexGuard` is dropped here rather than being held
    // across the awaits below, which would make this future non-`Send`.
    let existing = task_store.lock().unwrap().get_pending_tx(&key);

    let (nonce, max_fee, priority_fee) = match &existing {
        Some(previous) => {
            let nonce = previous.nonce;
            let blocking = blocking_nonce(provider, wallet_address).await;
            if let Some(blocking) = blocking {
                if blocking != nonce {
                    info!(logger = "Task", route = route_name, action, nonce, blocking, "Queued behind an earlier nonce, waiting");
                    // `blocking_nonce` returns the confirmed nonce it just read.
                    return Err(hold_or_clear(task_store, &key, nonce, Some(blocking), action, epoch, route_name));
                }
            }
            match (
                crate::bumped_fee(previous.max_fee, market.max_fee_per_gas),
                crate::bumped_fee(previous.priority_fee, market.max_priority_fee_per_gas),
            ) {
                (Some(max_fee), Some(priority_fee)) => {
                    info!(logger = "Task", route = route_name, action, nonce, max_fee, "Replacing pending tx with a higher fee");
                    (nonce, max_fee, priority_fee)
                }
                // Nothing of ours is pending, so the recorded tx was dropped or never
                // broadcast: there is nothing to replace, and holding would wait for a
                // nonce that is never mined (S3). Resend at the same nonce at market fee;
                // the pinned nonce still rules out a duplicate ("nonce too low" if it was
                // mined after all, "underpriced" if it is pending somewhere after all).
                _ if blocking.is_none() => {
                    warn!(logger = "Task", route = route_name, action, nonce, "Recorded tx not pending, resending at the same nonce at market fee");
                    (nonce, market.max_fee_per_gas, market.max_priority_fee_per_gas)
                }
                _ => {
                    warn!(logger = "Task", route = route_name, action, nonce, "Fee ceiling reached, holding rather than bumping further");
                    // One `latest` read, deliberately without retry backoff.
                    let confirmed = provider.get_transaction_count(wallet_address).latest().await.ok();
                    return Err(hold_or_clear(task_store, &key, nonce, confirmed, action, epoch, route_name));
                }
            }
        }
        None => {
            let nonce = provider.get_transaction_count(wallet_address).pending().await?;
            (nonce, market.max_fee_per_gas, market.max_priority_fee_per_gas)
        }
    };

    task_store.lock().unwrap().set_pending_tx(&key, nonce, max_fee, priority_fee);

    let sent = call
        .nonce(nonce)
        .max_fee_per_gas(max_fee)
        .max_priority_fee_per_gas(priority_fee)
        .send()
        .await;
    drop(broadcasting);

    let pending = match sent {
        Ok(pending) => pending,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("nonce too low") {
                info!(logger = "Task", route = route_name, action, nonce, "Nonce already consumed, deferring to on-chain checks");
                task_store.lock().unwrap().clear_pending_tx(&key);
                return Err("NonceConsumed".into());
            }
            if msg.contains("replacement transaction underpriced") {
                warn!(logger = "Task", route = route_name, action, nonce, "Replacement underpriced, bumping further next cycle");
                return Err("PendingReplacement".into());
            }
            // Only gas estimation executes the call, so a revert means this attempt was
            // never broadcast (S6). Leaving its record would pin a nonce that another
            // action may take next, and a later "bump" would replace that action's tx.
            // Put back what was there before: nothing, or the earlier broadcast's record.
            if msg.contains("execution reverted") {
                match &existing {
                    None => task_store.lock().unwrap().clear_pending_tx(&key),
                    Some(previous) => task_store.lock().unwrap().set_pending_tx(&key, previous.nonce, previous.max_fee, previous.priority_fee),
                }
                return send_tx(Err(e), action, route_name).await;
            }
            // Anything else may or may not have reached a mempool, so the record stays:
            // a stale one is harmless, a lost one risks a duplicate.
            return send_tx(Err(e), action, route_name).await;
        }
    };

    match pending.with_timeout(Some(RECEIPT_TIMEOUT)).get_receipt().await {
        Ok(receipt) => {
            task_store.lock().unwrap().clear_pending_tx(&key);
            if !receipt.status() {
                return Err(format!("[{}] {} reverted", route_name, action).into());
            }
            info!(logger = "Task", route = route_name, action, "Transaction succeeded");
            Ok(())
        }
        Err(e) => {
            warn!(logger = "Task", route = route_name, action, nonce, "Receipt not seen ({e}), will replace next cycle");
            Err("PendingReplacement".into())
        }
    }
}

pub async fn send_tx(
    result: Result<PendingTransactionBuilder<Ethereum>, ContractError>,
    action: &str,
    route_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match result {
        Ok(pending) => {
            let receipt = pending.with_timeout(Some(RECEIPT_TIMEOUT)).get_receipt().await?;
            if !receipt.status() {
                return Err(format!("[{}] {} reverted", route_name, action).into());
            }
            info!(logger = "Task", route = route_name, action, "Transaction succeeded");
            Ok(())
        }
        Err(e) => {
            let err_msg = e.to_string();
            match decode_revert_reason(&err_msg) {
                Some(reason) => {
                    error!(logger = "Task", route = route_name, action, reason = reason.as_str(), "Transaction reverted");
                    Err(format!("[{}] {} reverted: {}", route_name, action, reason).into())
                }
                None => Err(e.into()),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub epoch: u64,
    pub execute_after: u64,
    pub kind: TaskKind,
}

/// A transaction that has been broadcast but not yet seen confirmed.
///
/// Tracked per route in [`RouteState`] rather than on a [`Task`], because the claim
/// that `EpochWatcher` files has no `Task` behind it, and because a task can be
/// removed by `invalidate_tasks` while its transaction is still in flight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTx {
    pub nonce: u64,
    pub max_fee: u128,
    pub priority_fee: u128,
}

/// Key for [`RouteState::pending_txs`] - one in-flight transaction per action per epoch.
pub fn pending_key(action: &str, epoch: u64) -> String {
    format!("{action}:{epoch}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TaskKind {
    SaveSnapshot,
    Claim { state_root: FixedBytes<32> },
    ValidateClaim,
    Challenge,
    SendSnapshot,
    StartVerification,
    VerifySnapshot,
    ExecuteRelay {
        #[serde(with = "u256_hex")]
        position: U256,
        l2_sender: Address,
        dest_addr: Address,
        l2_block: u64,
        l1_block: u64,
        l2_timestamp: u64,
        #[serde(with = "u256_hex")]
        amount: U256,
        #[serde(with = "bytes_hex")]
        data: Bytes,
    },
    WithdrawDeposit,
}

impl TaskKind {
    pub fn name(&self) -> &'static str {
        match self {
            TaskKind::SaveSnapshot => "SaveSnapshot",
            TaskKind::Claim { .. } => "Claim",
            TaskKind::ValidateClaim => "ValidateClaim",
            TaskKind::Challenge => "Challenge",
            TaskKind::SendSnapshot => "SendSnapshot",
            TaskKind::StartVerification => "StartVerification",
            TaskKind::VerifySnapshot => "VerifySnapshot",
            TaskKind::ExecuteRelay { .. } => "ExecuteRelay",
            TaskKind::WithdrawDeposit => "WithdrawDeposit",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RouteState {
    pub inbox_last_block: Option<u64>,
    pub outbox_last_block: Option<u64>,
    pub tasks: Vec<Task>,
    pub indexing_since: Option<u64>,
    #[serde(default)]
    pub on_sync: bool,
    pub last_saved_count: Option<u64>,
    /// In-flight transactions, keyed by [`pending_key`]. Survives task removal, and
    /// covers actions that have no `Task` at all.
    #[serde(default)]
    pub pending_txs: HashMap<String, PendingTx>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimData {
    pub epoch: u64,
    pub state_root: FixedBytes<32>,
    pub claimer: Address,
    pub timestamp_claimed: u32,
    pub timestamp_verification: u32,
    pub blocknumber_verification: u32,
    pub honest: String,
    pub challenger: Address,
}

impl ClaimData {
    /// The outbox's `hashClaim`: `keccak256(abi.encodePacked(stateRoot, claimer,
    /// timestampClaimed, timestampVerification, blocknumberVerification, honest,
    /// challenger))`, computed with `honest` in place of the stored value.
    pub fn hash_with_honest(&self, honest: Party) -> FixedBytes<32> {
        let mut packed = Vec::with_capacity(85);
        packed.extend_from_slice(self.state_root.as_slice());
        packed.extend_from_slice(self.claimer.as_slice());
        packed.extend_from_slice(&self.timestamp_claimed.to_be_bytes());
        packed.extend_from_slice(&self.timestamp_verification.to_be_bytes());
        packed.extend_from_slice(&self.blocknumber_verification.to_be_bytes());
        packed.push(honest as u8);
        packed.extend_from_slice(self.challenger.as_slice());
        alloy::primitives::keccak256(packed)
    }

    /// The `honest` the outbox wrote for this claim: the candidate whose hash equals
    /// `claim_hash` (`claimHashes[epoch]`). `None` when no candidate matches, i.e. the
    /// hash was deleted by a withdrawal or the stored claim is wrong in another field.
    pub fn honest_party(&self, claim_hash: FixedBytes<32>) -> Option<&'static str> {
        [(Party::None, "None"), (Party::Claimer, "Claimer"), (Party::Challenger, "Challenger")]
            .into_iter()
            .find(|(party, _)| self.hash_with_honest(*party) == claim_hash)
            .map(|(_, name)| name)
    }
}

pub struct ClaimStore {
    path: PathBuf,
}

impl ClaimStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn load_all(&self) -> Vec<ClaimData> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => serde_json::from_str(&contents).expect("Failed to parse claims file - data corrupted"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => panic!("Failed to read claims file: {}", e),
        }
    }

    fn save_all(&self, claims: &[ClaimData]) {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).expect("Failed to create claims directory");
        }
        let contents = serde_json::to_string_pretty(claims).expect("Failed to serialize claims");
        atomic_write(&self.path, &contents).expect("Failed to write claims file");
    }

    pub fn store(&self, claim: ClaimData) {
        let mut claims = self.load_all();
        if let Some(stored) = claims.iter().find(|c| c.epoch == claim.epoch) {
            // A replayed event: keep the stored claim, which may carry later updates.
            if stored.claimer != claim.claimer
                || stored.state_root != claim.state_root
                || stored.timestamp_claimed != claim.timestamp_claimed
            {
                error!(
                    logger = "ClaimStore", epoch = claim.epoch,
                    stored_claimer = ?stored.claimer, new_claimer = ?claim.claimer,
                    stored_state_root = ?stored.state_root, new_state_root = ?claim.state_root,
                    stored_timestamp = stored.timestamp_claimed, new_timestamp = claim.timestamp_claimed,
                    "Conflicting claim for an existing epoch, keeping the stored claim"
                );
            }
            return;
        }
        claims.push(claim);
        self.save_all(&claims);
    }

    pub fn update<F>(&self, epoch: u64, f: F)
    where
        F: FnOnce(&mut ClaimData),
    {
        let mut claims = self.load_all();
        let claim = claims.iter_mut().find(|c| c.epoch == epoch);
        match claim {
            Some(c) => {
                f(c);
                self.save_all(&claims);
            }
            None => panic!("Cannot update claim for epoch {} - not found", epoch),
        }
    }

    pub fn get(&self, epoch: u64) -> ClaimData {
        let claims = self.load_all();
        let matches: Vec<_> = claims.into_iter().filter(|c| c.epoch == epoch).collect();
        match matches.len() {
            0 => panic!("No claim found for epoch {}", epoch),
            1 => matches.into_iter().next().unwrap(),
            n => panic!("Multiple claims ({}) for epoch {} - impossible state", n, epoch),
        }
    }

    pub fn get_claim(&self, epoch: u64) -> Claim {
        let c = self.get(epoch);
        Claim {
            stateRoot: c.state_root,
            claimer: c.claimer,
            timestampClaimed: c.timestamp_claimed,
            timestampVerification: c.timestamp_verification,
            blocknumberVerification: c.blocknumber_verification,
            honest: match c.honest.as_str() {
                "Claimer" => Party::Claimer,
                "Challenger" => Party::Challenger,
                _ => Party::None,
            },
            challenger: c.challenger,
        }
    }

    pub fn remove(&self, epoch: u64) {
        let mut claims = self.load_all();
        claims.retain(|c| c.epoch != epoch);
        self.save_all(&claims);
    }

    pub fn has_state_root_in_recent_claims(&self, state_root: FixedBytes<32>, since_timestamp: u32) -> bool {
        let claims = self.load_all();
        claims.iter().any(|c| c.state_root == state_root && c.timestamp_claimed >= since_timestamp)
    }

    pub fn exists(&self, epoch: u64) -> bool {
        self.load_all().iter().any(|c| c.epoch == epoch)
    }
}

pub struct TaskStore {
    path: PathBuf,
}

impl TaskStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn label(&self) -> String {
        self.path.file_stem()
            .map(|s| s.to_string_lossy().to_uppercase().replace("-", "_"))
            .unwrap_or_default()
    }

    pub fn load(&self) -> RouteState {
        match fs::read_to_string(&self.path) {
            Ok(contents) => serde_json::from_str(&contents).expect("Failed to parse schedule file - data corrupted"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => RouteState::default(),
            Err(e) => panic!("Failed to read schedule file: {}", e),
        }
    }

    pub fn save(&self, state: &RouteState) {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).expect("Failed to create schedules directory");
        }
        let contents = serde_json::to_string_pretty(state).expect("Failed to serialize state");
        atomic_write(&self.path, &contents).expect("Failed to write state file");
    }

    pub fn add_task(&self, task: Task) {
        let mut state = self.load();
        if state.tasks.iter().any(|t| t.epoch == task.epoch && t.kind.name() == task.kind.name()) {
            debug!(logger = "TaskStore", route = self.label().as_str(), task = task.kind.name(), epoch = task.epoch, "Task already scheduled, skipping");
            return;
        }
        info!(logger = "TaskStore", route = self.label().as_str(), task = task.kind.name(), epoch = task.epoch, execute_after = task.execute_after, "Scheduling task");
        state.tasks.push(task);
        self.save(&state);
    }

    pub fn remove_task(&self, task: &Task) {
        let mut state = self.load();
        state.tasks.retain(|t| !(t.epoch == task.epoch && t.kind.name() == task.kind.name()));
        self.save(&state);
    }

    pub fn reschedule_task(&self, task: &Task, execute_after: u64) {
        info!(logger = "TaskStore", route = self.label().as_str(), task = task.kind.name(), epoch = task.epoch, execute_after, "Rescheduling task");
        let mut state = self.load();
        for t in &mut state.tasks {
            if t.epoch == task.epoch && t.kind.name() == task.kind.name() {
                t.execute_after = execute_after;
                break;
            }
        }
        self.save(&state);
    }

    /// Record the nonce and fees a transaction went out with.
    ///
    /// Written before the transaction is broadcast, so the record cannot be lost by a
    /// crash between broadcasting and persisting. A send that reverts in gas estimation
    /// gets its record cleared or restored by `send_or_replace` (S6); any other rejected
    /// send leaves a stale record, and the next attempt simply sends at that nonce.
    pub fn set_pending_tx(&self, key: &str, nonce: u64, max_fee: u128, priority_fee: u128) {
        info!(logger = "TaskStore", route = self.label().as_str(), key, nonce, "Recording pending tx");
        let mut state = self.load();
        state.pending_txs.insert(key.to_string(), PendingTx { nonce, max_fee, priority_fee });
        self.save(&state);
    }

    /// Forget a pending transaction, once its outcome is known.
    pub fn clear_pending_tx(&self, key: &str) {
        let mut state = self.load();
        state.pending_txs.remove(key);
        self.save(&state);
    }

    pub fn get_pending_tx(&self, key: &str) -> Option<PendingTx> {
        self.load().pending_txs.get(key).cloned()
    }

    pub fn update_inbox_block(&self, block: u64) {
        let mut state = self.load();
        state.inbox_last_block = Some(block);
        self.save(&state);
    }

    pub fn update_outbox_block(&self, block: u64) {
        let mut state = self.load();
        state.outbox_last_block = Some(block);
        self.save(&state);
    }

    pub fn set_indexing_since(&self, ts: u64) {
        let mut state = self.load();
        state.indexing_since = Some(ts);
        self.save(&state);
    }

    pub fn initialize_sync(&self, indexing_since: u64, inbox_block: u64, outbox_block: u64) {
        let mut state = self.load();
        state.indexing_since = Some(indexing_since);
        state.inbox_last_block = Some(inbox_block);
        state.outbox_last_block = Some(outbox_block);
        self.save(&state);
    }

    pub fn set_on_sync(&self, value: bool) {
        let mut state = self.load();
        state.on_sync = value;
        self.save(&state);
    }

    pub fn is_on_sync(&self) -> bool {
        self.load().on_sync
    }

    pub fn invalidate_tasks(&self, epoch: u64, kinds: &[&str]) {
        let mut state = self.load();
        let before = state.tasks.len();
        state.tasks.retain(|t| !(t.epoch == epoch && kinds.contains(&t.kind.name())));
        let removed = before - state.tasks.len();
        if removed > 0 {
            info!(logger = "TaskStore", route = self.label().as_str(), epoch, removed, "Invalidated tasks");
        }
        self.save(&state);
    }

    pub fn set_last_saved_count(&self, count: u64) {
        let mut state = self.load();
        state.last_saved_count = Some(count);
        self.save(&state);
    }

    pub fn get_last_saved_count(&self) -> Option<u64> {
        self.load().last_saved_count
    }
}

mod u256_hex {
    use alloy::primitives::U256;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{:#x}", value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        U256::from_str_radix(s.trim_start_matches("0x"), 16).map_err(serde::de::Error::custom)
    }
}

mod bytes_hex {
    use alloy::hex;
    use alloy::primitives::Bytes;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{}", hex::encode(value)))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = hex::decode(s.trim_start_matches("0x")).map_err(serde::de::Error::custom)?;
        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_consumed_when_confirmed_above_saved() {
        assert_eq!(reconcile(5, Some(6)), NonceStatus::Consumed);
        assert_eq!(reconcile(5, Some(100)), NonceStatus::Consumed);
    }

    #[test]
    fn reconcile_open_when_confirmed_not_above_saved() {
        assert_eq!(reconcile(5, Some(5)), NonceStatus::Open);
        assert_eq!(reconcile(5, Some(3)), NonceStatus::Open);
    }

    #[test]
    fn reconcile_open_when_confirmed_unknown() {
        assert_eq!(reconcile(5, None), NonceStatus::Open);
    }

    #[test]
    fn hold_or_clear_clears_record_only_when_consumed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Mutex::new(TaskStore::new(dir.path().join("s.json"))));
        let key = pending_key("claim", 7);
        store.lock().unwrap().set_pending_tx(&key, 5, 1, 1);

        let e = hold_or_clear(&store, &key, 5, Some(5), "claim", 7, "R");
        assert_eq!(e.to_string(), "PendingReplacement");
        let e = hold_or_clear(&store, &key, 5, None, "claim", 7, "R");
        assert_eq!(e.to_string(), "PendingReplacement");
        assert!(store.lock().unwrap().get_pending_tx(&key).is_some());

        let e = hold_or_clear(&store, &key, 5, Some(6), "claim", 7, "R");
        assert_eq!(e.to_string(), "NonceConsumed");
        assert!(store.lock().unwrap().get_pending_tx(&key).is_none());
    }

    fn claim() -> ClaimData {
        ClaimData {
            epoch: 1,
            state_root: FixedBytes::repeat_byte(0x11),
            claimer: Address::repeat_byte(0x22),
            timestamp_claimed: 0x1234_5678,
            timestamp_verification: 0x8765_4321,
            blocknumber_verification: 0xdead_beef,
            honest: "None".to_string(),
            challenger: Address::repeat_byte(0x33),
        }
    }

    // Reference hashes from `cast abi-encode --packed` + `cast keccak` over the same
    // 85 bytes the outbox's `hashClaim` packs, with honest = 0, 1, 2.
    const HASH_NONE: FixedBytes<32> = alloy::primitives::b256!("ff368b4940e750f11025a640534138cfff9f4fd80af19fcaab697b0a336f9b81");
    const HASH_CLAIMER: FixedBytes<32> = alloy::primitives::b256!("694c56409884736f7f1627ac67f47ba94b791e1b32062052acba7d51c29f2aa1");
    const HASH_CHALLENGER: FixedBytes<32> = alloy::primitives::b256!("42c2a82c6f0b252253a14fdef270b67348b4a9497fd5f9aa0a8bfa77b6fa7f64");

    #[test]
    fn hash_with_honest_matches_the_outbox_hash_claim() {
        let c = claim();
        assert_eq!(c.hash_with_honest(Party::None), HASH_NONE);
        assert_eq!(c.hash_with_honest(Party::Claimer), HASH_CLAIMER);
        assert_eq!(c.hash_with_honest(Party::Challenger), HASH_CHALLENGER);
    }

    #[test]
    fn honest_party_is_whatever_the_chain_hash_says() {
        // S10: the stored `honest` is not guessed from the inbox root but read back from
        // the on-chain hash, so every contract path (verifySnapshot: Claimer whatever the
        // root; resolveDisputedClaim: Claimer / Challenger / None) is mirrored.
        let c = claim();
        assert_eq!(c.honest_party(HASH_NONE), Some("None"));
        assert_eq!(c.honest_party(HASH_CLAIMER), Some("Claimer"));
        assert_eq!(c.honest_party(HASH_CHALLENGER), Some("Challenger"));
        assert_eq!(c.honest_party(FixedBytes::ZERO), None, "deleted hash: nothing matches");
        let mut other = claim();
        other.timestamp_verification = 0;
        assert_eq!(other.honest_party(HASH_CLAIMER), None, "a claim wrong in another field matches nothing");
    }
}
