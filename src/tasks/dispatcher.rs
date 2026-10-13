use alloy::providers::Provider;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::{Route, ValidatorConfig};
use crate::tasks;
use crate::tasks::{Task, TaskKind, TaskStore, ClaimStore};

const POLL_INTERVAL: Duration = Duration::from_secs(15);

type TaskResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const MIN: u64 = 60;

/// What happens to a task after one execution attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Finished (or judged obsolete): removed from the store.
    Done,
    /// Left exactly as it is, so the next poll runs it again.
    Retry,
    /// Kept, with `execute_after` pushed this many seconds past now.
    RescheduleIn(u64),
}

/// The error-to-decision table. Task modules signal outcomes by returning `Err` with
/// these exact strings, so a new case needs a row here and a test below.
fn outcome(kind: &TaskKind, result: &TaskResult) -> Outcome {
    let e = match result {
        Ok(()) => return Outcome::Done,
        Err(e) => e.to_string(),
    };
    match kind {
        TaskKind::ValidateClaim if e == "EpochNotFinalized" => Outcome::RescheduleIn(30 * MIN),
        // In flight: keep the task as-is so the next cycle replaces it. Must precede the
        // catch-alls below, or an in-flight tx gets pushed into the future.
        TaskKind::Challenge | TaskKind::StartVerification | TaskKind::VerifySnapshot | TaskKind::ExecuteRelay { .. }
        | TaskKind::SaveSnapshot | TaskKind::SendSnapshot
            if e == "PendingReplacement" => Outcome::Retry,
        TaskKind::Challenge if e == "Insufficient funds" => Outcome::RescheduleIn(30 * MIN),
        TaskKind::Challenge if e == "VerificationStarted" => Outcome::RescheduleIn(20 * MIN),
        TaskKind::Challenge | TaskKind::StartVerification | TaskKind::VerifySnapshot | TaskKind::WithdrawDeposit
            if e.contains("Invalid claim") => Outcome::RescheduleIn(30 * MIN),
        TaskKind::ExecuteRelay { .. } if e == "RootNotConfirmed" => Outcome::RescheduleIn(60 * MIN),
        // The pinned nonce was consumed elsewhere and the record cleared: the next poll
        // sends fresh, as for every other kind (F16); not a failed relay to push 30 min out.
        TaskKind::ExecuteRelay { .. } if e == "NonceConsumed" => Outcome::Retry,
        TaskKind::ExecuteRelay { .. } => Outcome::RescheduleIn(30 * MIN),
        _ => Outcome::Retry,
    }
}

/// Apply an [`Outcome`] to the store. Only `Done` removes the task; a rescheduled task
/// keeps its new `execute_after`.
fn apply(store: &Mutex<TaskStore>, task: &Task, outcome: Outcome, now: u64) {
    match outcome {
        Outcome::Done => store.lock().unwrap().remove_task(task),
        Outcome::Retry => {}
        Outcome::RescheduleIn(secs) => store.lock().unwrap().reschedule_task(task, now + secs),
    }
}

pub struct TaskDispatcher {
    config: ValidatorConfig,
    route: Route,
    task_store: Arc<Mutex<TaskStore>>,
    claim_store: Arc<Mutex<ClaimStore>>,
}

impl TaskDispatcher {
    pub fn new(
        config: ValidatorConfig,
        route: Route,
        task_store: Arc<Mutex<TaskStore>>,
        claim_store: Arc<Mutex<ClaimStore>>,
    ) -> Self {
        Self {
            config,
            route,
            task_store,
            claim_store,
        }
    }

    pub async fn run(&self) {
        loop {
            self.process_pending().await;
            sleep(POLL_INTERVAL).await;
        }
    }

    pub async fn process_pending(&self) {
        if !self.task_store.lock().unwrap().is_on_sync() {
            return;
        }

        let state = self.task_store.lock().unwrap().load();

        let now = match self.route.outbox_provider.get_block_by_number(Default::default()).await {
            Ok(Some(block)) => block.header.timestamp,
            _ => {
                warn!(logger = "Dispatcher", route = self.route.name, "Failed to get latest block, retrying next cycle");
                return;
            }
        };

        let ready: Vec<Task> = state
            .tasks
            .iter()
            .filter(|t| now >= t.execute_after)
            .cloned()
            .collect();

        if ready.is_empty() {
            return;
        }

        info!(logger = "Dispatcher", route = self.route.name, count = ready.len(), "Processing ready tasks");

        for task in ready {
            info!(logger = "Dispatcher", route = self.route.name, task = task.kind.name(), epoch = task.epoch, "Executing task");
            let result = self.execute_task(&task, now).await;
            let decision = outcome(&task.kind, &result);
            match &result {
                Ok(()) => info!(logger = "Dispatcher", route = self.route.name, task = task.kind.name(), epoch = task.epoch, "Completed task"),
                Err(e) => warn!(logger = "Dispatcher", route = self.route.name, task = task.kind.name(), epoch = task.epoch, error = %e, decision = ?decision, "Task not completed"),
            }
            apply(&self.task_store, &task, decision, now);
        }
    }

    async fn execute_task(&self, task: &Task, current_timestamp: u64) -> TaskResult {
        let epoch = task.epoch;
        let wallet_address = self.config.wallet.default_signer().address();
        match &task.kind {
            TaskKind::SaveSnapshot => {
                tasks::save_snapshot::execute(&self.route, &self.task_store, epoch, wallet_address).await
            }
            TaskKind::Claim { .. } => {
                tasks::claim::execute(&self.config, &self.route, epoch, &self.claim_store, current_timestamp, &self.task_store).await
            }
            TaskKind::ValidateClaim => {
                tasks::validate_claim::execute(&self.config, &self.route, epoch, &self.claim_store, current_timestamp, &self.task_store).await
            }
            TaskKind::Challenge => {
                tasks::challenge::execute(&self.config, &self.route, epoch, &self.claim_store, &self.task_store).await
            }
            TaskKind::SendSnapshot => {
                tasks::send_snapshot::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await
            }
            TaskKind::StartVerification => {
                tasks::start_verification::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await
            }
            TaskKind::VerifySnapshot => {
                tasks::verify_snapshot::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await
            }
            TaskKind::ExecuteRelay { position, l2_sender, dest_addr, l2_block, l1_block, l2_timestamp, amount, data } => {
                tasks::execute_relay::execute(
                    &self.config,
                    &self.route,
                    *position,
                    *l2_sender,
                    *dest_addr,
                    *l2_block,
                    *l1_block,
                    *l2_timestamp,
                    *amount,
                    data.clone(),
                    &self.task_store,
                    epoch,
                ).await
            }
            TaskKind::WithdrawDeposit => {
                tasks::withdraw_deposit::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, Bytes, U256};

    fn store() -> (tempfile::TempDir, Mutex<TaskStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = TaskStore::new(dir.path().join("route.json"));
        (dir, Mutex::new(store))
    }

    fn task(epoch: u64, kind: TaskKind, execute_after: u64) -> Task {
        Task { epoch, execute_after, kind }
    }

    fn err(msg: &str) -> TaskResult {
        Err(msg.into())
    }

    fn relay() -> TaskKind {
        TaskKind::ExecuteRelay {
            position: U256::ZERO, l2_sender: Address::ZERO, dest_addr: Address::ZERO,
            l2_block: 0, l1_block: 0, l2_timestamp: 0, amount: U256::ZERO, data: Bytes::new(),
        }
    }

    fn all_kinds() -> Vec<TaskKind> {
        vec![
            TaskKind::SaveSnapshot, TaskKind::Claim { state_root: Default::default() }, TaskKind::ValidateClaim,
            TaskKind::Challenge, TaskKind::SendSnapshot, TaskKind::StartVerification, TaskKind::VerifySnapshot,
            relay(), TaskKind::WithdrawDeposit,
        ]
    }

    #[test]
    fn ok_is_done_for_every_kind() {
        for kind in all_kinds() {
            assert_eq!(outcome(&kind, &Ok(())), Outcome::Done, "{}", kind.name());
        }
    }

    #[test]
    fn pending_replacement_is_left_untouched_for_every_kind() {
        // Never rescheduled: the next poll must replace the in-flight tx.
        for kind in all_kinds() {
            assert_eq!(outcome(&kind, &err("PendingReplacement")), Outcome::Retry, "{}", kind.name());
        }
    }

    #[test]
    fn validate_claim_rows() {
        assert_eq!(outcome(&TaskKind::ValidateClaim, &err("EpochNotFinalized")), Outcome::RescheduleIn(30 * MIN));
        assert_eq!(outcome(&TaskKind::ValidateClaim, &err("rpc down")), Outcome::Retry);
    }

    #[test]
    fn challenge_rows() {
        let k = TaskKind::Challenge;
        assert_eq!(outcome(&k, &err("Insufficient funds")), Outcome::RescheduleIn(30 * MIN));
        assert_eq!(outcome(&k, &err("VerificationStarted")), Outcome::RescheduleIn(20 * MIN));
        assert_eq!(outcome(&k, &err("[ARB_TO_ETH] challenge reverted: Invalid claim.")), Outcome::RescheduleIn(30 * MIN));
        assert_eq!(outcome(&k, &err("EpochNotFinalized")), Outcome::Retry);
        assert_eq!(outcome(&k, &err("NonceConsumed")), Outcome::Retry);
    }

    #[test]
    fn start_and_verify_rows() {
        for k in [TaskKind::StartVerification, TaskKind::VerifySnapshot, TaskKind::WithdrawDeposit] {
            assert_eq!(outcome(&k, &err("[R] verifySnapshot reverted: Invalid claim.")), Outcome::RescheduleIn(30 * MIN), "{}", k.name());
            assert_eq!(outcome(&k, &err("NonceConsumed")), Outcome::Retry, "{}", k.name());
            // Strings owned by other tasks do not leak into these rows.
            assert_eq!(outcome(&k, &err("Insufficient funds")), Outcome::Retry, "{}", k.name());
        }
    }

    /// The Arbitrum sends go through `send_or_replace` too (S18/S12): in flight stays
    /// scheduled, a consumed nonce or a revert is simply tried again next poll.
    #[test]
    fn snapshot_rows() {
        for k in [TaskKind::SaveSnapshot, TaskKind::SendSnapshot] {
            assert_eq!(outcome(&k, &err("PendingReplacement")), Outcome::Retry, "{}", k.name());
            assert_eq!(outcome(&k, &err("NonceConsumed")), Outcome::Retry, "{}", k.name());
            assert_eq!(outcome(&k, &err("[ARB_TO_ETH] sendSnapshot reverted: Can only send past epoch snapshot.")), Outcome::Retry, "{}", k.name());
        }
    }

    #[test]
    fn execute_relay_rows() {
        assert_eq!(outcome(&relay(), &err("RootNotConfirmed")), Outcome::RescheduleIn(60 * MIN));
        assert_eq!(outcome(&relay(), &err("ExecutionFailed")), Outcome::RescheduleIn(30 * MIN));
        assert_eq!(outcome(&relay(), &err("NonceConsumed")), Outcome::Retry);
        assert_eq!(outcome(&relay(), &err("anything else")), Outcome::RescheduleIn(30 * MIN));
    }

    #[test]
    fn untabled_kinds_retry_on_error() {
        for k in [TaskKind::SaveSnapshot, TaskKind::Claim { state_root: Default::default() }, TaskKind::SendSnapshot] {
            for e in ["EpochNotFinalized", "Invalid claim", "RootNotConfirmed", "boom"] {
                assert_eq!(outcome(&k, &err(e)), Outcome::Retry, "{} / {e}", k.name());
            }
        }
    }

    #[test]
    fn rescheduled_task_stays_with_new_execute_after() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::ValidateClaim, 100);
        store.lock().unwrap().add_task(t.clone());

        apply(&store, &t, outcome(&t.kind, &err("EpochNotFinalized")), 100);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].epoch, 7);
        assert_eq!(tasks[0].execute_after, 100 + 30 * MIN);
    }

    #[test]
    fn completed_task_is_removed() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::ValidateClaim, 100);
        let other = task(8, TaskKind::ValidateClaim, 100);
        store.lock().unwrap().add_task(t.clone());
        store.lock().unwrap().add_task(other);

        apply(&store, &t, Outcome::Done, 100);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].epoch, 8);
    }

    #[test]
    fn unfinished_task_without_reschedule_is_untouched() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::Challenge, 100);
        store.lock().unwrap().add_task(t.clone());

        apply(&store, &t, outcome(&t.kind, &err("PendingReplacement")), 5000);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].execute_after, 100);
    }
}
