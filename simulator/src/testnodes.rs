//! Test node setup and management.
//! 
//! Provides utilities for initializing and controlling simulated blockchain nodes.

use hyperplane::{
    types::ChainId,
    confirmation_layer::node::ConfirmationLayerNode,
    confirmation_layer::ConfirmationLayer,
    hyper_scheduler::node::HyperSchedulerNode,
    hyper_ig::node::HyperIGNode,
    utils::logging,
};
use tokio::time::Duration;
use tokio::sync::mpsc;
use std::sync::Arc;
use tokio::sync::Mutex;

// ------------------------------------------------------------------------------------------------
// Node Setup Functions
// ------------------------------------------------------------------------------------------------

/// Bundle of the nodes created for one simulation run
pub struct TestNodes {
    /// The hyper scheduler node
    pub hs_node: Arc<Mutex<HyperSchedulerNode>>,
    /// The confirmation layer node
    pub cl_node: Arc<Mutex<ConfirmationLayerNode>>,
    /// One HIG node per chain, in chain order (chain-1, chain-2, ...)
    pub hig_nodes: Vec<Arc<Mutex<HyperIGNode>>>,
    /// The current block number at the end of the setup
    pub start_block: u64,
}

impl TestNodes {
    /// Logs the number of preloaded accounts and the first few balances of every chain
    pub async fn log_preloaded_balances(&self) {
        use hyperplane::hyper_ig::HyperIG;
        logging::log("SIMULATOR", "=== Verifying Preloaded Account Balances ===");
        for (i, hig_node) in self.hig_nodes.iter().enumerate() {
            let state = hig_node.lock().await.get_chain_state().await.unwrap();
            logging::log("SIMULATOR", &format!("Chain-{} state: {} accounts with balances", i + 1, state.len()));
            let mut sorted_accounts: Vec<_> = state.iter().collect();
            sorted_accounts.sort_by_key(|(account_id, _)| account_id.parse::<u32>().unwrap_or(0));
            for (account_id, balance) in sorted_accounts.iter().take(10) {
                logging::log("SIMULATOR", &format!("Chain-{} Account {}: {} tokens", i + 1, account_id, balance));
            }
            if sorted_accounts.len() > 10 {
                logging::log("SIMULATOR", &format!("... and {} more accounts", sorted_accounts.len() - 10));
            }
        }
        logging::log("SIMULATOR", "=== Account Balance Verification Complete ===");
    }

    /// Shuts down all nodes so their state is cleared between runs
    pub async fn shutdown(&self) {
        for hig_node in &self.hig_nodes {
            HyperIGNode::shutdown(hig_node.clone()).await;
        }
        ConfirmationLayerNode::shutdown(self.cl_node.clone()).await;
        HyperSchedulerNode::shutdown(self.hs_node.clone()).await;
    }
}

/// Creates one HS, one CL and one HIG per chain, registers the chains and waits for block production.
///
/// The number of chains equals `chain_delays.len()`; chains are named chain-1 .. chain-N.
///
/// # Arguments
///
/// * `block_interval` - The block interval to use for the confirmation layer node
/// * `chain_delays` - The HIG-to-HS delay of each chain (in blocks), one entry per chain
/// * `allow_cat_pending_dependencies` - Whether to allow CATs to depend on locked keys
/// * `cat_lifetime_blocks` - The default lifetime for CATs in blocks
/// * `num_accounts` - Number of accounts to preload (0 for no preloading)
/// * `preload_value` - Value to preload each account with
/// * `channel_buffer_size` - Buffer size for communication channels
pub async fn setup_test_nodes(block_interval: Duration, chain_delays: &[f64], allow_cat_pending_dependencies: bool, cat_lifetime_blocks: u64, num_accounts: u32, preload_value: u32, channel_buffer_size: usize) -> TestNodes {
    // Note: Logging should be initialized by the calling code before calling this function
    let num_chains = chain_delays.len();

    let (sender_hs_to_cl, receiver_hs_to_cl) = mpsc::channel(channel_buffer_size);
    let hs_node = Arc::new(Mutex::new(HyperSchedulerNode::new(sender_hs_to_cl)));
    let cl_node = Arc::new(Mutex::new(ConfirmationLayerNode::new_with_block_interval(
        receiver_hs_to_cl,
        block_interval,
    ).expect("Failed to create confirmation node")));

    // Create one HIG per chain with its own channels
    let mut hig_nodes = Vec::with_capacity(num_chains);
    let mut cl_to_hig_senders = Vec::with_capacity(num_chains);
    let mut hig_to_hs_receivers = Vec::with_capacity(num_chains);
    let chain_ids: Vec<ChainId> = (1..=num_chains).map(|i| ChainId(format!("chain-{}", i))).collect();
    for chain_id in &chain_ids {
        let (sender_hig_to_hs, receiver_hig_to_hs) = mpsc::channel(channel_buffer_size);
        let (sender_cl_to_hig, receiver_cl_to_hig) = mpsc::channel(channel_buffer_size);
        hig_nodes.push(Arc::new(Mutex::new(HyperIGNode::new_with_preloaded_accounts(receiver_cl_to_hig, sender_hig_to_hs, chain_id.clone(), cat_lifetime_blocks, allow_cat_pending_dependencies, num_accounts, preload_value))));
        cl_to_hig_senders.push(sender_cl_to_hig);
        hig_to_hs_receivers.push(receiver_hig_to_hs);
    }

    // Start the nodes
    HyperSchedulerNode::start(hs_node.clone()).await;
    for hig_node in &hig_nodes {
        HyperIGNode::start(hig_node.clone()).await;
    }
    ConfirmationLayerNode::start(cl_node.clone()).await;

    // Register chains in CL and HS, in chain order
    {
        let mut cl_node_guard = cl_node.lock().await;
        for (chain_id, sender) in chain_ids.iter().zip(cl_to_hig_senders) {
            cl_node_guard.register_chain(chain_id.clone(), sender).await.expect("Failed to register chain");
        }
    }
    {
        let mut hs_node_guard = hs_node.lock().await;
        for (chain_id, receiver) in chain_ids.iter().zip(hig_to_hs_receivers) {
            hs_node_guard.register_chain(chain_id.clone(), receiver).await.expect("Failed to register chain");
        }
    }

    // Wait for block production to be ready
    let mut attempts = 0;
    while attempts < 10 {
        if let Ok(interval) = cl_node.lock().await.get_block_interval().await {
            if interval == block_interval {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        logging::log("TEST", &format!("Waiting for block production to be ready.. attempt: {}", attempts));
        attempts += 1;
    }

    // Wait a couple of blocks to ensure the block production is ready
    tokio::time::sleep(block_interval * 2).await;
    let current_block = cl_node.lock().await.get_current_block().await.unwrap();
    logging::log("NODES SETUP", &format!("Nodes setup complete with {} chains, current block: {}", num_chains, current_block));

    // Set the provided delays (convert from blocks to time)
    for (hig_node, delay_blocks) in hig_nodes.iter().zip(chain_delays) {
        hig_node.lock().await.set_hs_message_delay(Duration::from_secs_f64(block_interval.as_secs_f64() * delay_blocks));
    }

    TestNodes { hs_node, cl_node, hig_nodes, start_block: current_block }
}
