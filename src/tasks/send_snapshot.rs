use alloy::primitives::{Address, U256};
use std::sync::{Arc, Mutex};
use crate::config::Route;
use crate::contracts::{IVeaInboxArbToEth, IVeaInboxArbToGnosis};
use crate::tasks::{send_or_replace, ClaimStore, TaskStore};

pub async fn execute(
    route: &Route,
    epoch: u64,
    claim_store: &Arc<Mutex<ClaimStore>>,
    task_store: &Arc<Mutex<TaskStore>>,
    wallet_address: Address,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let claim = claim_store.lock().unwrap().get_claim(epoch);

    // Through `send_or_replace` like every other tx (S18/S12): a `.send()` whose estimate
    // reverted ("Can only send past epoch snapshot.") burned a cached nonce and queued
    // every later Arbitrum send behind the gap; a receipt timeout sent it twice. No
    // frontrun check: the inbox accepts any number of `sendSnapshot`s for an epoch, so
    // "someone else did it" is not a revert here, and an in-flight one of ours is
    // `PendingReplacement`, which the dispatcher leaves scheduled.
    match route.name {
        "ARB_TO_ETH" => {
            let inbox = IVeaInboxArbToEth::new(route.inbox_address, route.inbox_provider.clone());
            send_or_replace(
                inbox.sendSnapshot(U256::from(epoch), claim),
                &route.inbox_provider,
                wallet_address,
                task_store,
                epoch,
                "sendSnapshot",
                route.name,
            ).await
        }
        "ARB_TO_GNOSIS" => {
            let inbox = IVeaInboxArbToGnosis::new(route.inbox_address, route.inbox_provider.clone());
            let gas_limit = U256::from(500000);
            send_or_replace(
                inbox.sendSnapshot(U256::from(epoch), gas_limit, claim),
                &route.inbox_provider,
                wallet_address,
                task_store,
                epoch,
                "sendSnapshot",
                route.name,
            ).await
        }
        _ => panic!("Unknown route: {}", route.name),
    }
}
