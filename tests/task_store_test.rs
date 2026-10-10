use tempfile::tempdir;
use alloy::primitives::{Address, FixedBytes};
use vea_validator::tasks::{pending_key, ClaimData, ClaimStore, Task, TaskKind, TaskStore};

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

fn claim(epoch: u64, root: u8) -> ClaimData {
    ClaimData {
        epoch,
        state_root: FixedBytes::<32>::repeat_byte(root),
        claimer: Address::repeat_byte(1),
        timestamp_claimed: 100,
        timestamp_verification: 0,
        blocknumber_verification: 0,
        honest: "None".to_string(),
        challenger: Address::ZERO,
    }
}

#[test]
fn save_leaves_valid_json_and_no_tmp() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sched.json");
    let store = TaskStore::new(&path);
    store.set_pending_tx("k", 1, 2, 3);
    serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert!(!dir.path().join("sched.json.tmp").exists());

    let cpath = dir.path().join("claims.json");
    ClaimStore::new(&cpath).store(claim(1, 9));
    serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(&cpath).unwrap()).unwrap();
    assert!(!dir.path().join("claims.json.tmp").exists());
}

#[test]
fn stale_tmp_does_not_break_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sched.json");
    let store = TaskStore::new(&path);
    store.set_pending_tx("k", 7, 2, 3);
    std::fs::write(dir.path().join("sched.json.tmp"), "{ truncated").unwrap();
    assert_eq!(store.get_pending_tx("k").unwrap().nonce, 7);
    store.set_pending_tx("k2", 8, 2, 3);
    assert_eq!(store.get_pending_tx("k").unwrap().nonce, 7);
    assert!(!dir.path().join("sched.json.tmp").exists());
}

#[test]
fn claim_store_twice_keeps_later_updates() {
    let dir = tempdir().unwrap();
    let store = ClaimStore::new(dir.path().join("claims.json"));
    store.store(claim(5, 9));
    store.update(5, |c| c.challenger = Address::repeat_byte(2));
    store.store(claim(5, 9));
    assert_eq!(store.get(5).challenger, Address::repeat_byte(2));
}

#[test]
fn conflicting_claim_store_keeps_original() {
    let dir = tempdir().unwrap();
    let store = ClaimStore::new(dir.path().join("claims.json"));
    store.store(claim(5, 9));
    store.store(claim(5, 8));
    assert_eq!(store.get(5).state_root, FixedBytes::<32>::repeat_byte(9));
}

#[test]
fn duplicate_add_task_does_not_stack_or_move() {
    let dir = tempdir().unwrap();
    let store = TaskStore::new(dir.path().join("sched.json"));
    store.add_task(Task { epoch: 3, execute_after: 100, kind: TaskKind::ValidateClaim });
    store.add_task(Task { epoch: 3, execute_after: 999, kind: TaskKind::ValidateClaim });
    store.add_task(Task { epoch: 3, execute_after: 50, kind: TaskKind::Challenge });
    let tasks = store.load().tasks;
    assert_eq!(tasks.len(), 2);
    let v = tasks.iter().find(|t| t.kind.name() == "ValidateClaim").unwrap();
    assert_eq!(v.execute_after, 100);
}

/// Child half of `sigkill_mid_write_leaves_a_loadable_file`: when spawned with
/// `CRASH_WRITER_DIR` set, rewrites both stores until it is killed. A no-op otherwise.
#[test]
fn crash_writer_child() {
    let Ok(dir) = std::env::var("CRASH_WRITER_DIR") else { return };
    let dir = std::path::PathBuf::from(dir);
    let tasks = TaskStore::new(dir.join("sched.json"));
    let claims = ClaimStore::new(dir.join("claims.json"));
    for i in 0u64.. {
        // Large enough that a non-atomic write would often be caught half-done.
        for k in 0..50 {
            tasks.set_pending_tx(&format!("action-{k}:{i}"), i, 1, 1);
        }
        claims.store(claim(i, (i % 250) as u8));
    }
}

/// A real crash, not a simulated one: SIGKILL a process in the middle of writing and
/// check both stores still load (F2's atomic write). Repeated with varying timing.
#[test]
fn sigkill_mid_write_leaves_a_loadable_file() {
    for round in 0..5u64 {
        let dir = tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_writer_child", "--test-threads=1"])
            .env("CRASH_WRITER_DIR", dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300 + round * 137));
        child.kill().unwrap(); // SIGKILL on unix
        child.wait().unwrap();

        let tasks = TaskStore::new(dir.path().join("sched.json"));
        let state = tasks.load(); // panics on corrupted JSON
        assert!(!state.pending_txs.is_empty(), "round {round}: child wrote nothing before the kill");
        let claims = ClaimStore::new(dir.path().join("claims.json"));
        assert!(claims.exists(0), "round {round}: claims file lost its first claim");
    }
}
