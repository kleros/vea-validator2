# VEA Validator Design & Rationale

Rust validator for the VEA (Verified Ethereum Attestation) bridge. Monitors claims, challenges fraud, and relays messages between Arbitrum and destination chains (Ethereum, Gnosis).

## Architecture

```
main.rs
  ├── startup checks (RPC health, balances, WETH approval)
  └── per route:
        ├── EpochWatcher  - tracks epoch timing, saves snapshots at epoch end, optionally makes claims
        ├── EventIndexer  - scans inbox/outbox events, schedules tasks
        └── TaskDispatcher - executes scheduled tasks
```

## Routes

- `ARB_TO_ETH`: Arbitrum → Ethereum (native ETH deposits)
- `ARB_TO_GNOSIS`: Arbitrum → Gnosis (WETH deposits)

## Core Components

### EpochWatcher
Polls every 10s. Two responsibilities:
1. **Save snapshot** ~60s before epoch ends (if messages exist and count changed)
2. **Make claim** ~20min after epoch starts (if `MAKE_CLAIMS=true` and synced)

**Snapshot saving logic:** The indexer tracks `last_saved_count` from `SnapshotSaved` events. Before saving, we compare `inbox.count()` to `last_saved_count`:
- If `count == 0`: no messages, skip
- If `count <= last_saved_count`: nothing new, skip
- If `last_saved_count` is unknown (None): treat as 0, save if messages exist

**Cold-start behavior:** If no `SnapshotSaved` events exist in the sync window (fresh chain or long hiatus), `last_saved_count` defaults to 0. This ensures the validator will save a snapshot if any messages exist, preventing bridge staleness when this is the only validator running.

**20min buffer edge case:** Since `SnapshotSaved` events are processed with the same 20min finality buffer as other events, if another validator saves a snapshot within the last 20min of an epoch, this validator won't see it yet and may call `saveSnapshot()` redundantly. This is acceptable, since L2 gas is cheap.

**Why claims are optional:** To challenge fraud, the validator needs ETH/WETH for deposits. Making claims locks funds on the outbox. During an attack, a conservative validator should preserve capital for challenges rather than tie it up in claims.

### EventIndexer
Scans inbox/outbox logs in chunks. Only processes events from blocks older than 20min (finality buffer).

Reacts to events:
- `outbox.Claimed` → schedules `task::validate_claim`
  - if valid → schedules `task::start_verification` (after `start_verification_delay`)
  - if invalid → schedules `task::challenge`
- `outbox.VerificationStarted` → schedules `task::verify_snapshot` (after minChallengePeriod)
- `outbox.Challenged` → schedules `task::send_snapshot`
- `outbox.Verified` → schedules `task::withdraw_deposit` (`honest` read back from `claimHashes`, S10; nothing scheduled when the contract left it None)
- `inbox.SnapshotSent` → schedules `task::execute_relay` (after `relay_delay`), **only if emitted by this validator**
- `inbox.SnapshotSaved` → updates `last_saved_count` (used by save_snapshot logic)

### TaskDispatcher
Polls every 15s. Executes tasks when `execute_after` timestamp reached.

Rules for what happens to a task after it runs:
- **Completed** (or judged obsolete, e.g. already done by another validator): removed from the store.
- **Rescheduled** (`reschedule_task` with a new `execute_after`): **kept** in the store and runs again at the new time.
- The decision is one pure table, `dispatcher::outcome(kind, result)` → `Done` / `Retry` / `RescheduleIn(secs)`, applied by `apply`; only `Done` removes a task. Every row is unit-tested, and every failed attempt is logged with its error and decision.
- **`PendingReplacement`**: the task is left untouched so the next poll replaces the in-flight tx.
- **Other errors**: the task stays as-is and is retried on the next poll.
- Rescheduled tasks have **no retry cap**. They end only by: completing successfully; the on-chain state making them moot (`isSpent(position)` for `execute_relay`, other pre-checks that drop the task); or event-driven invalidation by the indexer (see Proactive Task Invalidation). A permanently failing task (e.g. a relay that always errors) keeps logging every 30 minutes and stays in the schedule file until one of those happens.

### TaskStore / ClaimStore
Each route has its own JSON files for persistence:
- **TaskStore**: scheduled tasks, indexer block cursors, `indexing_since` timestamp, `on_sync` flag
- **ClaimStore**: claim data (state root, claimer, timestamps, challenger, honest party) needed to reconstruct `Claim` structs for contract calls

**Atomic writes (F2):** both stores write `<file>.tmp`, fsync it, and rename it over the original (one shared helper). A crash at any point leaves either the old or the new complete file, never truncated JSON.

**Idempotent replay (F4):** a replayed `Claimed` event (e.g. after a crash between storing the claim and scheduling its task) is safe. The indexer dedups on whether the claim exists, not on whether a task exists. `ClaimStore::store` never panics on an existing epoch and never overwrites it. A claim that exists without its `ValidateClaim` task gets the task re-added. `TaskStore::add_task` keeps at most one task per (epoch, kind); a duplicate is skipped and the stored task stays exactly as it is.

**Estimation revert leaves no record (S6):** only gas estimation executes the call, so a send that fails with "execution reverted" was never broadcast. `send_or_replace` then clears the record it just wrote (or restores the earlier broadcast's record when this was a replacement attempt). Otherwise the record would pin a nonce another action takes next, and the following attempt would "bump" and replace that other action's in-flight tx.

**Stale record above the fee ceiling (S3):** when a bump would exceed the ceiling but nothing of ours is pending (pending nonce == confirmed nonce, or the mempool read failed), the recorded transaction was dropped or never broadcast, so there is nothing to replace and holding would wait forever for a nonce nobody mines. `send_or_replace` then resends at the same nonce at the market fee. The pinned nonce still rules out a duplicate: a mined original makes the resend fail with "nonce too low" (→ `NonceConsumed`), a still-pending one makes it "underpriced" (→ `PendingReplacement`).

**Every outbox tx is pinned (S9):** `withdraw_deposit` goes through `send_or_replace` like claim, challenge, start/verify and relay. alloy's default nonce filler is a `CachedNonceManager`, which never sees the pinned nonces, so a plain `.send()` on the same provider after any pinned send went out with a stale nonce ("nonce too low") and withdrawals failed until restart. The Arbitrum sends go through it too (S18/S12, below), so no task tx anywhere uses the filler.

**No Arbitrum send uses the nonce filler (S18/S12):** `saveSnapshot` and `sendSnapshot` go through `send_or_replace` as well. alloy's `CachedNonceManager` hands out the next nonce in the same `try_join!` as the gas estimate and never takes it back, so a plain `.send()` whose estimate reverted (e.g. `sendSnapshot` for an epoch that is not over: "Can only send past epoch snapshot.") burned a nonce without broadcasting anything; every later Arbitrum `.send()` then went out one above the chain's pending nonce, sat queued forever, and each attempt blocked for the receipt timeout. Both routes share one Arbitrum provider and route restarts reuse it, so only a process restart cleared the gap. The pinned nonce bypasses the filler: an estimation revert leaves no record and takes no nonce (S6), and a receipt timeout is `PendingReplacement`, replaced at the same nonce next time instead of sent again at a new one (S12). `saveSnapshot` records are keyed by the epoch the attempt is made in (one attempt per epoch; a record from an older epoch must not be mistaken for a tx to bump, which would end in `NonceConsumed` and a skipped save); a record left behind by a receipt timeout is never consulted again and is harmless. `sendSnapshot` has no frontrun check: the inbox accepts any number of sends per epoch, so there is no "already done" revert to detect. The dispatcher leaves both kinds scheduled on `PendingReplacement` and retries them next poll on anything else.

**One broadcast at a time per chain (S11):** both routes' dispatchers, both epoch watchers and the relay sign with the same wallet, and on Ethereum the relay uses a second provider instance, so two senders could read the same `pending` nonce between each other's selection and broadcast; the second was rejected as underpriced, and its next "bump" replaced the first one's transaction. `send_or_replace` therefore holds a per-chain `tokio` mutex (keyed by the chain id the node reports, which is the nonce space actually shared) from nonce selection until the broadcast has returned, and releases it before waiting for the receipt. Nothing under the lock sends again, so it cannot deadlock; every early return releases it; the loops sharing a route's `select!` keep running while one of them waits. The cost is one `eth_chainId` per send.

**`honest` is read back from the chain (S10):** the outbox stores only `hashClaim(claim)`, so the `Claim` struct the validator rebuilds must hash to it or every later call reverts "Invalid claim.". `honest` cannot be inferred from the inbox root: `verifySnapshot` writes Claimer whatever the root (a dishonest claim nobody challenged in time), and `resolveDisputedClaim` writes Claimer when the bridged root matches, Challenger when it does not and the claim was challenged, and leaves None for an unchallenged claim with a wrong root, while the `Verified` log looks the same in every case. On `Verified` the indexer therefore hashes the stored claim with each candidate (`ClaimData::hash_with_honest`, the contract's `abi.encodePacked` layout) and keeps the one equal to `claimHashes[epoch]` (`honest_party`). A zero hash means the deposit was already withdrawn (claim dropped); no match is logged as an error and nothing is scheduled. None is accepted only for an unchallenged claim (nothing to withdraw, the claim stays and can still be challenged); for a challenged claim it is impossible after `Verified`, so it is treated as a stale state read from a lagging RPC and handled like any other indexer RPC failure. A persisted `WithdrawDeposit` task that still finds `honest == None` is dropped with a warning instead of panicking, and a withdrawal that reverts "Invalid claim." is rescheduled +30 min like the other claim-bound tasks rather than retried every poll.

**Pending tx reconciliation (F16):** in `send_or_replace`, before holding a pending record (fee ceiling reached, or an earlier nonce is blocking), the saved nonce is compared with the account's confirmed (`latest`) nonce. If the confirmed nonce is above it, the transaction was mined: the record is cleared and `NonceConsumed` is returned (callers already handle it; the dispatcher has no dedicated arm, so such a task is retried on the next poll). If the confirmed nonce cannot be read, the hold is kept.

## Task Types

| Task | Trigger | Action |
|------|---------|--------|
| `save_snapshot` | epoch end | calls `inbox.saveSnapshot()` |
| `claim` | epoch start | claims epoch on outbox |
| `validate_claim` | Claimed event | compares state roots, schedules challenge or start_verification |
| `challenge` | invalid claim | challenges with deposit |
| `send_snapshot` | Challenged event | sends snapshot via native bridge |
| `start_verification` | valid claim (after `start_verification_delay`) | starts verification period |
| `verify_snapshot` | VerificationStarted + minChallengePeriod | finalizes verification |
| `execute_relay` | SnapshotSent (after `relay_delay`) | executes L2→L1 message on Arbitrum Outbox |
| `withdraw_deposit` | Verified event | withdraws to honest party |

## Sync Behavior

On startup, indexer initializes from `now - sync_lookback_secs`. The lookback is computed dynamically from contract parameters: `relay_delay + start_verification_delay + min_challenge_period + buffer`. Events older than sync window are dropped gracefully. Tasks only execute when `on_sync=true`.

## Error Handling

### RPC Resilience
- **30s HTTP timeout** on all RPC providers (10s connect timeout) — prevents infinite hangs on unresponsive endpoints
- **Binary search (`find_block_by_timestamp`)**: retries each individual RPC call 3x with exponential backoff (5s, 15s, 45s) before panicking. Binary search state (lo/hi) preserved between retries.
- **Indexer `scan_chain`**: returns false on RPC failure, retries next cycle (1s later). Does not crash the route.
- **Epoch watcher**: logs warning and continues loop on RPC failure. Claim RPC timeouts/connection errors are also handled gracefully (warn + retry next cycle). Any other claim error (e.g. no funds for the deposit) is logged and retried after 5 minutes, and a failed `saveSnapshot` is logged (one attempt per epoch). Neither panics (S2): the watcher shares the route's `select!` with the indexer and dispatcher, so a panic would restart the whole route and starve the challenge path.
- **Task dispatcher**: logs warning and returns early on RPC failure (retries next cycle, 15s later). Does not crash the route.
- **Fallback layer**: with multiple RPCs configured, failures rotate to the next RPC automatically
- **No RPC secrets in errors (S5)**: every transport (one URL or several) is wrapped so transport errors carry the redacted URL (scheme and host) instead of the full one; reqwest embeds the URL in every error, URLs commonly hold API keys, and these errors are logged verbatim. Infra-error reclassification stays limited to multi-URL setups.

### General Errors
- **RPC failures during indexing**: logged, retry next poll
- **Task failures**: task stays in queue, retried next poll
- **Any `ExecuteRelay` error other than `PendingReplacement` and `RootNotConfirmed`**: rescheduled +30min and logged (route, epoch, error text); not dropped. `RootNotConfirmed` is rescheduled +1h.
- **`Insufficient funds` on Challenge**: rescheduled +30min
- **`Invalid claim` reverts**: rescheduled +30min (see below)

### Proactive Task Invalidation

When the indexer processes certain events, it proactively removes tasks that are no longer possible:

| Event | Tasks Invalidated |
|-------|-------------------|
| `Claimed` | (none) |
| `VerificationStarted` | `StartVerification` |
| `Challenged` | `ValidateClaim`, `Challenge`, `StartVerification`, `VerifySnapshot` |
| `Verified` | `ValidateClaim`, `Challenge`, `SendSnapshot`, `StartVerification`, `VerifySnapshot`, `ExecuteRelay` |

`SendSnapshot` is invalidated by `Verified` (S7): a resync over an already-resolved dispute would otherwise send the snapshot again and relay a resolution that changes nothing, paying L2 and L1 gas.

This prevents wasted execution attempts and keeps the task queue clean.

### Invalid Claim Rescheduling

Due to the 20min finality buffer, someone can change claim state right before we execute a task. When `challenge`, `start_verification`, or `verify_snapshot` reverts with "Invalid claim" (claimHash mismatch), we reschedule +30min to give the indexer time to catch up and update ClaimStore with fresh claim data. The task is kept in the store and retried (see TaskDispatcher).

### Race Conditions

Each task has its own way of detecting and handling race conditions (another validator did the job first):

| Task | Detection | Handling |
|------|-----------|----------|
| `save_snapshot` | snapshot count unchanged | skip (no-op) |
| `claim` | `claimHashes[epoch] != 0` (pre-check) | drop task |
| `claim` | `stateRoot` already matches | drop task |
| `claim` | state root in pending claim | drop task |
| `claim` | on revert: `claimHashes[epoch] != 0` | drop task |
| `challenge` | on revert: `Challenged` event emitted | drop task |
| `challenge` | on revert: `VerificationStarted` event emitted | reschedule +20min |
| `send_snapshot` | (none - cheap, idempotent) | always try |
| `save_snapshot` / `send_snapshot` | in flight (`PendingReplacement`) | leave scheduled |
| `start_verification` | `challenger != 0` | drop task |
| `start_verification` | `timestamp_verification != 0` (already started) | drop task |
| `start_verification` | on revert: `VerificationStarted` event emitted | drop task |
| `start_verification` | on revert: `Challenged` event emitted | drop task |
| `verify_snapshot` | `challenger != 0` | drop task |
| `verify_snapshot` | claim state root != `inbox.snapshots(epoch)` (read as `validate_claim` does; zero counts as a mismatch) | log and drop task, never send (F3) |
| `verify_snapshot` | on revert: `Verified` event emitted | drop task |
| `verify_snapshot` | on revert: `Challenged` event emitted | drop task |
| `execute_relay` | `isSpent(position) == true` | drop task |
| `execute_relay` | `roots(root) == 0` | reschedule +1hr |
| `execute_relay` | any other error / revert | reschedule +30min and log the error (Arbitrum outbox error messages are unreliable, so the cause is logged, not matched) |
| `withdraw_deposit` | `claimHashes[epoch] == 0` (pre-check) | drop task |
| `withdraw_deposit` | in flight (`PendingReplacement`) | leave scheduled |
| `withdraw_deposit` | on revert: `claimHashes[epoch] == 0` | drop task |
| `withdraw_deposit` | on revert: "Invalid claim" | reschedule +30min |

**Frontrun detection via event checks:** When a task reverts, we check if the expected event was emitted in recent blocks (the last 500 blocks up to the head; a `toBlock` past the head is rejected by some nodes, which made the check always answer "no"). The epoch is read from the first indexed topic or, for `Verified(uint256 _epoch)`, whose epoch neither outbox indexes, from the first data word (S15); before that the verify frontrun check could never match and a claim verified by someone else was rescheduled every 30 minutes instead of dropped. This detects frontruns where another validator's tx landed between our pre-check and tx execution. If the event exists, the job was done - we drop the task. For `challenge`, if `VerificationStarted` was emitted (rare edge case during resync), we reschedule +20min to wait for finality before retrying with updated claim data.

### Missing Claim Data (S1)

If we see `Verified`, `VerificationStarted`, or `Challenged` for an epoch whose `Claimed` event was emitted before our sync window, the indexer backfills the claim from the chain instead of dropping the event or treating it as impossible. `startVerification` and `verifySnapshot` have no deadline, so such events can arrive at any time; a dishonest claim older than the window whose verification starts later must still be validated and challenged.

**Backfill:** the outbox accepts a claim for `epoch` only while its clock is in `epoch + 1`, so the `Claimed` log is searched in that one-epoch window of outbox blocks (filtered by the epoch topic). The epoch's `VerificationStarted`/`Challenged` logs between the claim and the current event are fetched too, then everything is replayed through the normal handlers (which schedule `ValidateClaim` etc.), so the stored claim hashes to what the outbox holds. All logs are fetched before anything is stored; an RPC failure panics like the indexer's other RPC failures, and the route restarts and rescans the chunk. Only when no `Claimed` log exists is the event dropped (logged).

**Starting mid-bridge:** The validator can be started at any time. On first run (or after a hiatus longer than `sync_lookback_secs`), `indexing_since` is set to `now - sync_lookback_secs` and persisted. The contract only stores a claim hash, so the full `Claim` struct is reconstructed from the `Claimed` event, either witnessed during the scan or backfilled as above.

## SnapshotSent Filtering

The indexer only processes `SnapshotSent` events from transactions it sent itself (checked via `tx.signer == wallet_address`).

**Why?** `sendSnapshot()` is cheap to call with arbitrary data. An attacker could spam bogus snapshots with invalid `Claim` parameters. If we processed all `SnapshotSent` events, we'd waste gas relaying L2→L1 messages that would revert on Arbitrum's outbox (wrong proof/claim data).

**How it works:** When the validator sees a `Challenged` event, it calls `sendSnapshot` itself. By only indexing our own `SnapshotSent` events, we guarantee we're relaying messages we know are correct.

**Alternative approach (not implemented):** Parse the `L2ToL1Tx` calldata, compare the embedded `Claim` against our `ClaimStore`, verify no prior `SnapshotSent` exists for that epoch, then schedule the relay. This would let us piggyback on other validators' snapshots but adds complexity. The `isSpent` check in `execute_relay` would then serve as the final guard against duplicate relays. For v1, self-filtering is simpler and sufficient.

## Route-Specific Logic

Two types of route-specific branching:

**Deposit logic** (`weth_address.is_some()`): Used when the difference is ETH vs WETH deposits.

| Task | WETH route | ETH route |
|------|------------|-----------|
| `claim` | no `.value(deposit)` | `.value(deposit)` |
| `challenge` | check WETH balance, no `.value(deposit)` | check ETH balance, `.value(deposit)` |

**Contract signatures** (`match route.name`): Used when contracts have different function signatures.

| Task | ARB_TO_ETH | ARB_TO_GNOSIS |
|------|------------|---------------|
| `send_snapshot` | `sendSnapshot(epoch, claim)` | `sendSnapshot(epoch, gas_limit, claim)` |

## L2 Finality Verification

Before executing critical tasks (`validate_claim`, `claim`, `challenge`), the validator verifies the epoch's L2 data is finalized on L1. This prevents acting on data that could theoretically be reorged.

**How it works:**
1. Convert epoch → end timestamp: `(epoch + 1) * epoch_period`
2. Binary search to find the L2 block at that timestamp
3. Query `NodeInterface.findBatchContainingBlock(l2_block)` on Arbitrum to get the batch number
4. Search for `SequencerInbox.SequencerBatchDelivered(batchNum)` event on L1 to find which L1 block contains the batch
5. Compare that L1 block against `eth_getBlockByNumber("finalized")` - if finalized block >= batch block, the epoch is finalized

**Batch event query strategy:** Batches are posted to L1 after L2 transactions, but timing varies significantly - mainnet posts every ~1-3 minutes, while testnet can have gaps of hours or days. To find the `SequencerBatchDelivered` event reliably:
1. Binary search L1 to find the block at `epoch_end_ts` (batch must be posted after the L2 block exists)
2. Paginate forward in 2000-block chunks (~6.6 hours each) until we find the event or reach L1's latest block
3. If not found after scanning to latest, return "not finalized" and reschedule the task for +30 minutes

This approach handles arbitrary batch posting delays without hitting RPC block range limits. If the paginated search becomes too slow in production, we'll switch to indexing `SequencerBatchDelivered` events.

**Snapshot reads are pinned past the epoch end (S8):** `validate_claim`, `claim` and `verify_snapshot` read `inbox.snapshots(epoch)` through `finality::final_snapshot`, which finds the first inbox block at or after the epoch end and reads at that block. If the RPC's head is still before the epoch end, the result is "not ready" (`EpochNotFinalized`; `verify_snapshot` retries next poll). Read at "latest", a lagging or stuck node in the fallback set would return an older root saved earlier in the same epoch, making an honest claim look invalid (a wrong challenge loses the deposit) or making us claim a stale root. The finality check alone does not catch this: on a lagging node the block search returns its head, whose batch is final.

**Configuration:**
- `SEQUENCER_INBOX`: Address of Arbitrum's SequencerInbox on Ethereum L1. **Required in production** - `startup.rs` panics if not set.
- For tests (local Anvil chains), `SEQUENCER_INBOX` is not set. When `sequencer_inbox` is `None`, finality checks return `Ok(true)` immediately, bypassing the check.

**Why finality matters:**
- The 20-minute buffer after epoch end is a heuristic, not a guarantee
- Real finality depends on batch posting to L1 and L1 block finalization (~15-20 min on mainnet)
- Without finality checks, a validator could challenge a valid claim if L2 data reorged after they read it

**Task behavior when not finalized:**
- `ValidateClaim` returns `Err("EpochNotFinalized")` and gets rescheduled +30 minutes
- `Claim` (epoch watcher): returns `Err("EpochNotFinalized")`, retries after 5 minutes. If someone else claims in the meantime, the "Already claimed" check in `claim::execute` detects it and stops.
- This is expected during normal operation when tasks are scheduled close to epoch boundaries, or on testnet where batch posting is sporadic

## Known Issues

### Execute Relay

L2→L1 message relay requires the merkle root to be confirmed on the L1 Arbitrum Outbox before `executeTransaction` can succeed. Roots are posted when rollup assertions are confirmed (after `confirmPeriodBlocks`). If the root isn't confirmed yet, `execute_relay` detects this via `outbox.roots(root) == 0` and reschedules +1 hour.

However, the reason the root is not included may be a computing error, this is being looked into.
