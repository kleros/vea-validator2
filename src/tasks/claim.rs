use alloy::primitives::{Address, FixedBytes, U256};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};
use crate::config::{Route, ValidatorConfig};
use crate::contracts::{IVeaInbox, IVeaOutbox, IVeaOutboxArbToEth, IVeaOutboxArbToGnosis};
use crate::finality::{final_snapshot, is_epoch_finalized};
use crate::tasks::{send_or_replace, ClaimStore, TaskStore};

const SEVEN_DAYS_SECS: u32 = 7 * 24 * 3600;

pub async fn execute(
    config: &ValidatorConfig,
    route: &Route,
    epoch: u64,
    claim_store: &Arc<Mutex<ClaimStore>>,
    current_timestamp: u64,
    task_store: &Arc<Mutex<TaskStore>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let inbox = IVeaInbox::new(route.inbox_address, route.inbox_provider.clone());
    let epoch_period: u64 = inbox.epochPeriod().call().await?.try_into()?;

    let finalized = is_epoch_finalized(
        epoch,
        epoch_period,
        &route.inbox_provider,
        &config.ethereum_provider,
        config.sequencer_inbox,
    ).await?;
    if !finalized {
        warn!(logger = "Claim", route = route.name, epoch, "Epoch not yet finalized on L1");
        return Err("EpochNotFinalized".into());
    }

    let outbox = IVeaOutbox::new(route.outbox_address, route.outbox_provider.clone());

    let state_root = match final_snapshot(&route.inbox_provider, route.inbox_address, epoch, epoch_period).await? {
        Some(root) => root,
        None => {
            warn!(logger = "Claim", route = route.name, epoch, "Inbox RPC has not reached the epoch end yet");
            return Err("EpochNotFinalized".into());
        }
    };
    if state_root == FixedBytes::<32>::ZERO {
        info!(logger = "Claim", route = route.name, epoch, "No snapshot");
        return Ok(());
    }

    let claim_hash = outbox.claimHashes(U256::from(epoch)).call().await?;
    if claim_hash != FixedBytes::<32>::ZERO {
        info!(logger = "Claim", route = route.name, epoch, "Already claimed");
        return Ok(());
    }

    let current_state_root = outbox.stateRoot().call().await?;
    if current_state_root == state_root {
        info!(logger = "Claim", route = route.name, epoch, "State root already verified on outbox");
        return Ok(());
    }

    // A pending claim of the same root makes ours redundant, but only an honest one:
    // anyone could otherwise claim a past epoch with this epoch's root (a dishonest
    // claim, lost to our challenge) and make us skip this epoch (S16). So the match
    // counts only if that epoch's final snapshot really is this root.
    let since = (current_timestamp as u32).saturating_sub(SEVEN_DAYS_SECS);
    let recent = claim_store.lock().unwrap().recent_claims_with_root(state_root, since);
    for recent_claim in recent {
        if recent_claim.challenger != Address::ZERO || recent_claim.honest == "Challenger" {
            continue;
        }
        if final_snapshot(&route.inbox_provider, route.inbox_address, recent_claim.epoch, epoch_period).await? == Some(state_root) {
            info!(logger = "Claim", route = route.name, epoch, pending_epoch = recent_claim.epoch, "State root already in pending claim");
            return Ok(());
        }
        warn!(logger = "Claim", route = route.name, epoch, pending_epoch = recent_claim.epoch, "Pending claim with this root is for another snapshot, not skipping");
    }

    let wallet_address = config.wallet.default_signer().address();
    let result = if route.weth_address.is_some() {
        let gnosis_outbox = IVeaOutboxArbToGnosis::new(route.outbox_address, route.outbox_provider.clone());
        send_or_replace(
            gnosis_outbox.claim(U256::from(epoch), state_root),
            &route.outbox_provider,
            wallet_address,
            task_store,
            epoch,
            "claim",
            route.name,
        ).await
    } else {
        let eth_outbox = IVeaOutboxArbToEth::new(route.outbox_address, route.outbox_provider.clone());
        let deposit = eth_outbox.deposit().call().await?;
        send_or_replace(
            eth_outbox.claim(U256::from(epoch), state_root).value(deposit),
            &route.outbox_provider,
            wallet_address,
            task_store,
            epoch,
            "claim",
            route.name,
        ).await
    };

    if let Err(e) = result {
        // Still in flight - not a failed claim, so don't consult the chain or the
        // caller will read "not claimed yet" and treat it as something to retry fresh.
        if e.to_string() == "PendingReplacement" {
            return Err(e);
        }
        let claim_hash = outbox.claimHashes(U256::from(epoch)).call().await?;
        if claim_hash != FixedBytes::<32>::ZERO {
            info!(logger = "Claim", route = route.name, epoch, "Already claimed by another validator");
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}
