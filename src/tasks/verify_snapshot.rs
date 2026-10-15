use alloy::primitives::{Address, FixedBytes, U256};
use std::sync::{Arc, Mutex};
use tracing::{error, info};
use crate::config::Route;
use crate::contracts::{IVeaInbox, IVeaOutbox};
use crate::finality::final_snapshot;
use crate::tasks::{send_or_replace, was_event_emitted, ClaimStore, TaskStore};

/// A claim may only be verified when it matches the inbox snapshot. A zero inbox
/// root means no snapshot was saved for the epoch, which never matches a claim.
fn should_verify(claimed_root: FixedBytes<32>, inbox_root: FixedBytes<32>) -> bool {
    inbox_root != FixedBytes::<32>::ZERO && claimed_root == inbox_root
}

pub async fn execute(
    route: &Route,
    epoch: u64,
    claim_store: &Arc<Mutex<ClaimStore>>,
    task_store: &Arc<Mutex<TaskStore>>,
    wallet_address: Address,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let claim_data = claim_store.lock().unwrap().get(epoch);
    if claim_data.challenger != Address::ZERO {
        info!(logger = "VerifySnapshot", route = route.name, epoch, "Already challenged, dropping task");
        return Ok(());
    }

    let inbox = IVeaInbox::new(route.inbox_address, route.inbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await?.try_into()?;
    // A stale read would drop the task for an honest claim, so it must be final (S8).
    let inbox_root = match final_snapshot(&route.inbox_provider, route.inbox_address, epoch, epoch_period).await {
        Ok(Some(root)) => root,
        Ok(None) => {
            error!(logger = "VerifySnapshot", route = route.name, epoch, "Inbox RPC has not reached the epoch end yet");
            return Err("EpochNotFinalized".into());
        }
        Err(e) => {
            error!(logger = "VerifySnapshot", route = route.name, epoch, "Failed to read inbox snapshot: {e}");
            return Err(e);
        }
    };
    if !should_verify(claim_data.state_root, inbox_root) {
        error!(
            logger = "VerifySnapshot", route = route.name, epoch,
            claimed = ?claim_data.state_root, inbox = ?inbox_root,
            "Claim state root differs from inbox snapshot, not verifying"
        );
        return Ok(());
    }

    let claim = claim_store.lock().unwrap().get_claim(epoch);
    let outbox = IVeaOutbox::new(route.outbox_address, route.outbox_provider.clone());
    let result = send_or_replace(
        outbox.verifySnapshot(U256::from(epoch), claim),
        &route.outbox_provider,
        wallet_address,
        task_store,
        epoch,
        "verifySnapshot",
        route.name,
    ).await;

    if let Err(e) = result {
        // Still in flight - not a failure, so don't fall through to the "someone else
        // did it" checks, which would misread an unconfirmed tx as a lost race.
        if e.to_string() == "PendingReplacement" {
            return Err(e);
        }
        if was_event_emitted(&route.outbox_provider, route.outbox_address, "Verified(uint256)", epoch).await {
            info!(logger = "VerifySnapshot", route = route.name, epoch, "Already verified by another validator");
            return Ok(());
        }
        if was_event_emitted(&route.outbox_provider, route.outbox_address, "Challenged(uint256,address)", epoch).await {
            info!(logger = "VerifySnapshot", route = route.name, epoch, "Was challenged, dropping task");
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_root_verifies() {
        let r = FixedBytes::<32>::repeat_byte(0xab);
        assert!(should_verify(r, r));
    }

    #[test]
    fn mismatching_root_does_not_verify() {
        let a = FixedBytes::<32>::repeat_byte(0xab);
        let b = FixedBytes::<32>::repeat_byte(0xcd);
        assert!(!should_verify(a, b));
    }

    #[test]
    fn zero_inbox_root_is_mismatch() {
        let a = FixedBytes::<32>::repeat_byte(0xab);
        assert!(!should_verify(a, FixedBytes::ZERO));
        assert!(!should_verify(FixedBytes::ZERO, FixedBytes::ZERO));
    }
}
