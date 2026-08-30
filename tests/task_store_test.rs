use tempfile::tempdir;
use vea_validator::tasks::{pending_key, Task, TaskKind, TaskStore};

#[test]
fn pending_tx_round_trips_through_disk() {
    let dir = tempdir().unwrap();
    let store = TaskStore::new(dir.path().join("sched.json"));
    let key = pending_key("claim", 42);

    store.set_pending_tx(&key, 47, 30_000_000_000, 1_500_000_000);

    let pending = store.get_pending_tx(&key).expect("pending tx should be recorded");
    assert_eq!(pending.nonce, 47);
    assert_eq!(pending.max_fee, 30_000_000_000);
    assert_eq!(pending.priority_fee, 1_500_000_000);
}

#[test]
fn clear_pending_tx_removes_the_record() {
    let dir = tempdir().unwrap();
    let store = TaskStore::new(dir.path().join("sched.json"));
    let key = pending_key("claim", 42);

    store.set_pending_tx(&key, 47, 30_000_000_000, 1_500_000_000);
    store.clear_pending_tx(&key);

    assert!(store.get_pending_tx(&key).is_none());
}

#[test]
fn actions_and_epochs_get_independent_slots() {
    let dir = tempdir().unwrap();
    let store = TaskStore::new(dir.path().join("sched.json"));

    store.set_pending_tx(&pending_key("claim", 42), 47, 1, 1);
    store.set_pending_tx(&pending_key("challenge", 42), 48, 2, 2);
    store.set_pending_tx(&pending_key("claim", 43), 49, 3, 3);

    assert_eq!(store.get_pending_tx(&pending_key("claim", 42)).unwrap().nonce, 47);
    assert_eq!(store.get_pending_tx(&pending_key("challenge", 42)).unwrap().nonce, 48);
    assert_eq!(store.get_pending_tx(&pending_key("claim", 43)).unwrap().nonce, 49);
}

/// The claim filed by `EpochWatcher` has no `Task` behind it, and `invalidate_tasks`
/// can drop a task while its transaction is still in flight. Neither may lose the record.
#[test]
fn pending_tx_survives_task_removal() {
    let dir = tempdir().unwrap();
    let store = TaskStore::new(dir.path().join("sched.json"));
    let task = Task { epoch: 42, execute_after: 0, kind: TaskKind::Challenge };
    let key = pending_key("challenge", 42);

    store.add_task(task.clone());
    store.set_pending_tx(&key, 47, 1, 1);
    store.remove_task(&task);

    assert!(store.load().tasks.is_empty(), "task should be gone");
    assert_eq!(
        store.get_pending_tx(&key).map(|p| p.nonce),
        Some(47),
        "the in-flight transaction must still be tracked"
    );
}

/// Schedule files written before `pending_txs` existed must still deserialize, or an
/// upgrade would panic in `load()` on real validator state.
#[test]
fn old_schedule_files_without_pending_txs_still_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sched.json");
    std::fs::write(
        &path,
        r#"{"inbox_last_block":1,"outbox_last_block":2,"tasks":[
             {"epoch":7,"execute_after":0,"kind":{"type":"ValidateClaim"}}
           ],"indexing_since":null,"on_sync":false,"last_saved_count":null}"#,
    )
    .unwrap();

    let store = TaskStore::new(&path);
    let state = store.load();
    assert_eq!(state.tasks[0].epoch, 7);
    assert!(state.pending_txs.is_empty());
}
