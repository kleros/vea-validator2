use alloy::providers::Provider;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::{Route, ValidatorConfig};
use crate::tasks;
use crate::tasks::{Task, TaskKind, TaskStore, ClaimStore};

const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// Post-execution store action: only a finished task leaves the store.
/// A rescheduled task reports `false` and keeps its new `execute_after`.
fn settle(store: &Mutex<TaskStore>, task: &Task, done: bool) {
    if done {
        store.lock().unwrap().remove_task(task);
    }
}

/// Push the task to a later time and report it as not done, so it is not removed.
fn reschedule_and_keep(store: &Mutex<TaskStore>, task: &Task, execute_after: u64) -> bool {
    store.lock().unwrap().reschedule_task(task, execute_after);
    false
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
            let success = self.execute_task(&task, now).await;
            if success {
                info!(logger = "Dispatcher", route = self.route.name, task = task.kind.name(), epoch = task.epoch, "Completed task");
            }
            settle(&self.task_store, &task, success);
        }
    }

    async fn execute_task(&self, task: &Task, current_timestamp: u64) -> bool {
        let epoch = task.epoch;
        let wallet_address = self.config.wallet.default_signer().address();
        match &task.kind {
            TaskKind::SaveSnapshot => {
                tasks::save_snapshot::execute(&self.route, &self.task_store).await.is_ok()
            }
            TaskKind::Claim { .. } => {
                tasks::claim::execute(
                    &self.config,
                    &self.route,
                    epoch,
                    &self.claim_store,
                    current_timestamp,
                    &self.task_store,
                ).await.is_ok()
            }
            TaskKind::ValidateClaim => {
                match tasks::validate_claim::execute(
                    &self.config,
                    &self.route,
                    epoch,
                    &self.claim_store,
                    current_timestamp,
                    &self.task_store,
                ).await {
                    Ok(_) => true,
                    Err(e) if e.to_string() == "EpochNotFinalized" => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                    Err(_) => false,
                }
            }
            TaskKind::Challenge => {
                match tasks::challenge::execute(&self.config, &self.route, epoch, &self.claim_store, &self.task_store).await {
                    Ok(_) => true,
                    // In flight: keep the task as-is so the next cycle replaces it.
                    Err(e) if e.to_string() == "PendingReplacement" => false,
                    Err(e) if e.to_string() == "Insufficient funds" => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                    Err(e) if e.to_string() == "VerificationStarted" => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 20 * 60)
                    }
                    Err(e) if e.to_string().contains("Invalid claim") => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                    Err(_) => false,
                }
            }
            TaskKind::SendSnapshot => {
                tasks::send_snapshot::execute(&self.route, epoch, &self.claim_store).await.is_ok()
            }
            TaskKind::StartVerification => {
                match tasks::start_verification::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await {
                    Ok(_) => true,
                    Err(e) if e.to_string() == "PendingReplacement" => false,
                    Err(e) if e.to_string().contains("Invalid claim") => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                    Err(_) => false,
                }
            }
            TaskKind::VerifySnapshot => {
                match tasks::verify_snapshot::execute(&self.route, epoch, &self.claim_store, &self.task_store, wallet_address).await {
                    Ok(_) => true,
                    Err(e) if e.to_string() == "PendingReplacement" => false,
                    Err(e) if e.to_string().contains("Invalid claim") => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                    Err(_) => false,
                }
            }
            TaskKind::ExecuteRelay { position, l2_sender, dest_addr, l2_block, l1_block, l2_timestamp, amount, data } => {
                match tasks::execute_relay::execute(
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
                ).await {
                    Ok(_) => true,
                    // Must precede the catch-all below, or an in-flight relay gets
                    // pushed 30 minutes into the future instead of being replaced.
                    Err(e) if e.to_string() == "PendingReplacement" => false,
                    Err(e) if e.to_string() == "RootNotConfirmed" => {
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 60 * 60)
                    }
                    Err(e) => {
                        warn!(logger = "Dispatcher", route = self.route.name, task = task.kind.name(), epoch, error = %e, "ExecuteRelay failed, rescheduling in 30 minutes");
                        reschedule_and_keep(&self.task_store, task, current_timestamp + 30 * 60)
                    }
                }
            }
            TaskKind::WithdrawDeposit => {
                tasks::withdraw_deposit::execute(&self.route, epoch, &self.claim_store).await.is_ok()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Mutex<TaskStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = TaskStore::new(dir.path().join("route.json"));
        (dir, Mutex::new(store))
    }

    fn task(epoch: u64, kind: TaskKind, execute_after: u64) -> Task {
        Task { epoch, execute_after, kind }
    }

    #[test]
    fn rescheduled_task_stays_with_new_execute_after() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::ValidateClaim, 100);
        store.lock().unwrap().add_task(t.clone());

        let done = reschedule_and_keep(&store, &t, 1900);
        settle(&store, &t, done);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].epoch, 7);
        assert_eq!(tasks[0].execute_after, 1900);
    }

    #[test]
    fn completed_task_is_removed() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::ValidateClaim, 100);
        let other = task(8, TaskKind::ValidateClaim, 100);
        store.lock().unwrap().add_task(t.clone());
        store.lock().unwrap().add_task(other);

        settle(&store, &t, true);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].epoch, 8);
    }

    #[test]
    fn unfinished_task_without_reschedule_is_untouched() {
        let (_dir, store) = store();
        let t = task(7, TaskKind::Challenge, 100);
        store.lock().unwrap().add_task(t.clone());

        settle(&store, &t, false);

        let tasks = store.lock().unwrap().load().tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].execute_after, 100);
    }
}
