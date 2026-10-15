use alloy::primitives::{Address, FixedBytes, U256};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};
use crate::config::Route;
use crate::contracts::{IVeaOutbox, Party};
use crate::tasks::{send_or_replace, ClaimStore, TaskStore};

pub async fn execute(
    route: &Route,
    epoch: u64,
    claim_store: &Arc<Mutex<ClaimStore>>,
    task_store: &Arc<Mutex<TaskStore>>,
    wallet_address: Address,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let outbox = IVeaOutbox::new(route.outbox_address, route.outbox_provider.clone());

    let claim_hash = outbox.claimHashes(U256::from(epoch)).call().await?;
    if claim_hash == FixedBytes::<32>::ZERO {
        info!(logger = "WithdrawDeposit", route = route.name, epoch, "Already withdrawn");
        claim_store.lock().unwrap().remove(epoch);
        return Ok(());
    }

    let claim = claim_store.lock().unwrap().get_claim(epoch);
    info!(logger = "WithdrawDeposit", route = route.name, epoch, honest = ?claim.honest, "Withdrawing deposit");

    // Through `send_or_replace` like every other outbox tx (S9): the provider's cached
    // nonce filler never sees the pinned nonces of those sends, so a plain `.send()` here
    // went out with a stale nonce ("nonce too low") after any claim/challenge/verify.
    let result = match claim.honest {
        Party::Claimer => {
            send_or_replace(
                outbox.withdrawClaimDeposit(U256::from(epoch), claim),
                &route.outbox_provider, wallet_address, task_store, epoch, "withdrawClaimDeposit", route.name,
            ).await
        }
        Party::Challenger => {
            send_or_replace(
                outbox.withdrawChallengeDeposit(U256::from(epoch), claim),
                &route.outbox_provider, wallet_address, task_store, epoch, "withdrawChallengeDeposit", route.name,
            ).await
        }
        _ => {
            // The outbox left `honest` at None (an unchallenged claim resolved through
            // the bridge with a wrong root): neither withdrawal applies. The indexer no
            // longer schedules this task for such a claim (S10); one persisted before
            // that is dropped here instead of panicking the route.
            warn!(logger = "WithdrawDeposit", route = route.name, epoch, "No honest party, nothing to withdraw");
            return Ok(());
        }
    };

    if let Err(e) = result {
        if e.to_string() == "PendingReplacement" {
            return Err(e);
        }
        let claim_hash = outbox.claimHashes(U256::from(epoch)).call().await?;
        if claim_hash == FixedBytes::<32>::ZERO {
            info!(logger = "WithdrawDeposit", route = route.name, epoch, "Already withdrawn by another validator");
            claim_store.lock().unwrap().remove(epoch);
            return Ok(());
        }
        return Err(e);
    }

    claim_store.lock().unwrap().remove(epoch);
    Ok(())
}
