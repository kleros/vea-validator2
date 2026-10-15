use alloy::primitives::Address;
use tracing::info;
use crate::config::Route;
use crate::contracts::IVeaInbox;
use crate::tasks::{send_or_replace, TaskStore};
use std::sync::{Arc, Mutex};

pub async fn execute(
    route: &Route,
    task_store: &Arc<Mutex<TaskStore>>,
    epoch: u64,
    wallet_address: Address,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let inbox = IVeaInbox::new(route.inbox_address, route.inbox_provider.clone());
    let current_count = inbox.count().call().await?;

    if current_count == 0 {
        info!(logger = "SaveSnapshot", route = route.name, epoch, "No messages, skipping");
        return Ok(());
    }

    let last_saved_count = task_store.lock().unwrap().get_last_saved_count().unwrap_or(0);

    if current_count <= last_saved_count {
        info!(logger = "SaveSnapshot", route = route.name, epoch, count = current_count, "Nothing new to save");
        return Ok(());
    }

    info!(logger = "SaveSnapshot", route = route.name, epoch, from = last_saved_count, to = current_count, "Saving snapshot");
    // Through `send_or_replace` like every other tx (S18/S12). The provider's cached
    // nonce filler hands out a nonce while gas is estimated and never takes it back, so
    // one rejected Arbitrum send left a gap every later `.send()` queued behind until a
    // process restart; and a receipt timeout made the next attempt a duplicate.
    //
    // Keyed by epoch although `saveSnapshot` takes none: one attempt per epoch, and a
    // record left behind by a receipt timeout must not be mistaken next epoch for a tx
    // to bump (that would "bump" a long-mined nonce, get `NonceConsumed` and skip the
    // epoch's save). A stale `saveSnapshot:<epoch>` record is dead weight, nothing more:
    // no other action ever uses that key.
    send_or_replace(
        inbox.saveSnapshot(),
        &route.inbox_provider,
        wallet_address,
        task_store,
        epoch,
        "saveSnapshot",
        route.name,
    ).await
}
