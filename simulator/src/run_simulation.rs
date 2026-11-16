//! Core simulation logic for the Hyperplane simulator.
//! 
//! Handles transaction generation, submission, and status tracking during simulations.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use indicatif::{ProgressBar, ProgressStyle};
use hyperplane::{
    types::{TransactionId, Transaction, CLTransaction, CLTransactionId, ChainId},
    hyper_scheduler::node::HyperSchedulerNode,
    confirmation_layer::{ConfirmationLayerNode, ConfirmationLayer},
    hyper_ig::node::HyperIGNode,
    hyper_ig::HyperIG,
    utils::logging,
};
use crate::zipf_account_selection::AccountSelector;
use rand::Rng;
use crate::SimulationResults;
use crate::simulation_results::{ChainSeries, IssuedCAT};
use crate::config::CatSpan;
use rand::seq::index::sample;
use std::time::Instant;

// ------------------------------------------------------------------------------------------------
// Main Simulation Function
// ------------------------------------------------------------------------------------------------

/// Runs the simulation for the specified number of blocks
///
/// # Arguments
///
/// * `nodes` - A tuple containing two vectors:
///   - The first vector contains Arc<Mutex<ConfirmationLayerNode>>, the confirmation layer nodes
///   - The second vector contains Arc<Mutex<HyperIGNode>>, the HyperIG nodes
/// * `sim_total_block_number` - A u64, the total number of blocks to simulate
/// * `initial_balance` - A u64, the initial balance for transactions
/// * `num_accounts` - A usize, the number of accounts
/// * `target_tps` - A u64, the target TPS
/// * `zipf_parameter` - A f64, the Zipf parameter for account selection
/// * `chain_delays` - A Vec<u64>, the chain delays for each node (in blocks)
/// * `block_interval` - A f64, the interval between blocks
/// * `ratio_cats` - A f64, the ratio of CAT transactions
///
pub async fn run_simulation(
    cl_node: Arc<Mutex<ConfirmationLayerNode>>,
    hig_nodes: Vec<Arc<Mutex<HyperIGNode>>>,
    results: &mut SimulationResults,
) -> Result<(), String> {
    run_simulation_with_message(cl_node, hig_nodes, results, None).await
}

pub async fn run_simulation_with_message(
    cl_node: Arc<Mutex<ConfirmationLayerNode>>,
    hig_nodes: Vec<Arc<Mutex<HyperIGNode>>>,
    results: &mut SimulationResults,
    run_message: Option<String>,
) -> Result<(), String> {
    run_simulation_with_message_and_retries(cl_node, hig_nodes, results, run_message, None).await
}

pub async fn run_simulation_with_message_and_retries(
    cl_node: Arc<Mutex<ConfirmationLayerNode>>,
    hig_nodes: Vec<Arc<Mutex<HyperIGNode>>>,
    results: &mut SimulationResults,
    run_message: Option<String>,
    retry_count: Option<usize>,
) -> Result<(), String> {
    
    // Get the current block at the start
    let start_block = cl_node.lock().await.get_current_block().await.map_err(|e| e.to_string())?;
    
    let target_block = start_block + results.initialization_wait_blocks;
    
    logging::log("SIMULATOR", &format!("Starting at block {}, waiting until block {} for initialization and stable block production...", start_block, target_block));
    
    // Wait until we reach the target block
    loop {
        let current_block = cl_node.lock().await.get_current_block().await.map_err(|e| e.to_string())?;
        if current_block >= target_block {
            break;
        }
        // Sleep for a short duration before checking again
        sleep(Duration::from_millis(100)).await;
    }
    
    // Get the current block after initialization
    let initial_block = cl_node.lock().await.get_current_block().await.map_err(|e| e.to_string())?;
    logging::log("SIMULATOR", &format!("Initialization complete, starting simulation at block {}", initial_block));

    // Record the start time for transaction sending (after initialization)
    let _transaction_spam_start_time = Instant::now();

    // Initialize random number generator
    let mut rng = rand::thread_rng();
    
    // Initialize sender account selector with uniform distribution
    let mut account_selector_sender = AccountSelector::new(results.num_accounts, 0.0);    
    // Initialize receiver account selector with Zipf distribution
    let mut account_selector_receiver = AccountSelector::new(results.num_accounts, results.zipf_parameter);
    
    // Calculate target block number for simulation termination
    let final_simulation_block = initial_block + results.sim_total_block_number;
    
    // Create progress bar for blocks
    let progress_bar = ProgressBar::new(results.sim_total_block_number);
    let template = if let Some(ref msg) = run_message {
        let retry_suffix = if let Some(retries) = retry_count {
            if retries > 0 {
                format!(" [RETRY {}]", retries)
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        format!("[{{elapsed_precise}}] {{bar:40.cyan/blue}} Block {{pos}}/{{len}} ({{eta}}) {}{}", msg, retry_suffix)
    } else {
        let retry_suffix = if let Some(retries) = retry_count {
            if retries > 0 {
                format!(" [RETRY {}]", retries)
            } else {
                String::new()
            }
        } else {
            String::new()
        };
        format!("[{{elapsed_precise}}] {{bar:40.cyan/blue}} Block {{pos}}/{{len}} ({{eta}}){}", retry_suffix)
    };
    progress_bar.set_style(ProgressStyle::default_bar()
        .template(&template)
        .unwrap()
        .progress_chars("##-"));
    

    // Now set the actual chain delays for the main simulation
    logging::log("SIMULATOR", "Setting actual chain delays for main simulation...");
    for (i, delay_blocks) in results.chain_delays.iter().enumerate() {
        let delay_time = Duration::from_secs_f64(results.block_interval * *delay_blocks as f64);
        hig_nodes[i].lock().await.set_hs_message_delay(delay_time);
        logging::log("SIMULATOR", &format!("Set chain {} delay to {} blocks ({:?})", i + 1, delay_blocks, delay_time));
    }
    
    let mut current_block = initial_block;
    
    // Registered chains, in registration order (chain-1 .. chain-N)
    let chains = cl_node.lock().await.get_registered_chains().await.map_err(|e| e.to_string())?;
    if chains.len() != hig_nodes.len() {
        return Err(format!("{} chains registered but {} HIG nodes given", chains.len(), hig_nodes.len()));
    }
    if chains.len() < 2 && results.ratio_cats > 0.0 {
        return Err("CATs need at least two chains".to_string());
    }
    results.chains = vec![ChainSeries::default(); chains.len()];
    
    // ------- main simulation loop -------

    // Calculate target TPS from target TPB and block interval
    let target_tps = results.target_tpb as f64 / results.block_interval;
    let transactions_per_block = results.target_tpb as u64;
    logging::log("SIMULATOR", &format!("Target TPB: {}, Block interval: {}s, Calculated TPS: {:.2}, Transactions per block: {}", 
        results.target_tpb, results.block_interval, target_tps, transactions_per_block));
    

    // a counter to track how many times we have entered the following block without releasing transactions
    let mut block_counter = 0;

    // Main simulation loop - waits for new blocks and releases transactions in batches
    while current_block < final_simulation_block {
        // Get current block height from CL
        let new_block = cl_node.lock().await.get_current_block().await.map_err(|e| e.to_string())?;
        
        // Stop processing transactions if we've reached the target block
        if new_block >= final_simulation_block {
            logging::log("SIMULATOR", &format!("Reached target block {}, stopping transaction processing", final_simulation_block));
            break;
        }

        // Check if we've moved to a new block
        if new_block != current_block {

            // Record the block counter for the previous block before resetting it
            results.loop_steps_without_tx_issuance.push((current_block, block_counter));
            
            // Record the block height delta (how many blocks we skipped)
            let height_delta = new_block - current_block;
            results.block_height_delta.push((current_block, height_delta));
            
            // reset the block counter
            block_counter = 0;

            logging::log("SIMULATOR", &format!("🎯 NEW BLOCK CREATED - Height: {} 🎯", new_block));
            
            // Process and record all data for this height
            process_block_data(&cl_node, &hig_nodes, &chains, results, new_block).await?;
            
            current_block = new_block;
            
            // Update progress bar for new block
            let blocks_completed = new_block - initial_block;
            progress_bar.set_position(blocks_completed);
            
            // Release transactions for this block, compensating for missed blocks
            release_transactions_for_block(
                &cl_node,
                &mut rng,
                &mut account_selector_sender,
                &mut account_selector_receiver,
                results,
                &chains,
                transactions_per_block,
                new_block,
                height_delta,
            ).await?;
        } else {
            // increment the block counter
            block_counter += 1;

            // Wait in intervals based on transaction submission frequency
            let wait_interval = Duration::from_secs_f64(results.block_interval / results.transaction_submission_frequency as f64);
            tokio::time::sleep(wait_interval).await;
        }
    }

    results.final_block = current_block;
    collect_cat_latency_records(&hig_nodes, results).await;
    
    Ok(())
}

/// Copies the per-chain CAT finality records from the HIG nodes into the results
pub async fn collect_cat_latency_records(hig_nodes: &[Arc<Mutex<HyperIGNode>>], results: &mut SimulationResults) {
    for (chain, hig_node) in results.chains.iter_mut().zip(hig_nodes) {
        chain.cat_latency_records = hig_node.lock().await.get_cat_latency_records().await;
    }
}

/// Copies the hyper scheduler message counters into the results
pub async fn collect_hs_message_counts(hs_node: &Arc<Mutex<HyperSchedulerNode>>, results: &mut SimulationResults) {
    let hs = hs_node.lock().await;
    results.hs_message_counts = hs.get_message_counts().await;
    let mut per_cat: Vec<(String, _)> = hs.get_per_cat_message_counts().await.into_iter().map(|(id, c)| (id.0.0, c)).collect();
    per_cat.sort_by(|a, b| a.0.cmp(&b.0));
    results.hs_per_cat_message_counts = per_cat;
}

// ------------------------------------------------------------------------------------------------
// Data Processing Functions
// ------------------------------------------------------------------------------------------------

/// Counts the user transactions of one chain in one block, excluding HS status updates
async fn count_user_txs(cl_node: &Arc<Mutex<ConfirmationLayerNode>>, chain_id: &ChainId, block_height: u64) -> (u64, u64) {
    cl_node.lock().await.get_subblock(chain_id.clone(), block_height).await
        .map(|subblock| {
            let total = subblock.transactions.len() as u64;
            let user = subblock.transactions.iter().filter(|tx| !tx.data.starts_with("STATUS_UPDATE")).count() as u64;
            (user, total)
        })
        .unwrap_or((0, 0))
}

/// Processes and records all data for a single block
async fn process_block_data(
    cl_node: &Arc<Mutex<ConfirmationLayerNode>>,
    hig_nodes: &[Arc<Mutex<HyperIGNode>>],
    chains: &[ChainId],
    results: &mut SimulationResults,
    block_height: u64,
) -> Result<(), String> {
    // Each account gets one initialization credit transaction per chain
    let init_tx_count = results.num_accounts as u64;

    for (index, (hig_node, chain_id)) in hig_nodes.iter().zip(chains).enumerate() {
        let hig = hig_node.lock().await;
        let (cat_pending, cat_success, cat_failure) = hig.get_transaction_status_counts_cats().await.map_err(|e| e.to_string())?;
        let (cat_pending_resolving, cat_pending_postponed) = hig.get_cat_pending_detailed_counts().await.map_err(|e| e.to_string())?;
        let (regular_pending, regular_success, regular_failure) = hig.get_transaction_status_counts_regular().await.map_err(|e| e.to_string())?;
        let locked_keys = hig.get_total_locked_keys_count().await;
        let avg_latency = hig.get_average_regular_tx_latency().await;
        let max_latency = hig.get_max_regular_tx_latency().await;
        let finalized_count = hig.get_regular_tx_finalized_count().await;
        drop(hig);

        let (tx_per_block, total_in_block) = count_user_txs(cl_node, chain_id, block_height).await;
        if total_in_block != tx_per_block {
            logging::log("SIMULATOR", &format!("Block {}: {} TPB: {} (filtered from {} total)", block_height, chain_id.0, tx_per_block, total_in_block));
        }

        let series = &mut results.chains[index];
        series.pending.push((block_height, cat_pending + regular_pending));
        series.success.push((block_height, (cat_success + regular_success).saturating_sub(init_tx_count)));
        series.failure.push((block_height, cat_failure + regular_failure));
        series.cat_pending.push((block_height, cat_pending));
        series.cat_success.push((block_height, cat_success));
        series.cat_failure.push((block_height, cat_failure));
        series.cat_pending_resolving.push((block_height, cat_pending_resolving));
        series.cat_pending_postponed.push((block_height, cat_pending_postponed));
        series.regular_pending.push((block_height, regular_pending));
        series.regular_success.push((block_height, regular_success));
        series.regular_failure.push((block_height, regular_failure));
        series.locked_keys.push((block_height, locked_keys));
        series.tx_per_block.push((block_height, tx_per_block));
        series.regular_tx_avg_latency.push((block_height, avg_latency));
        series.regular_tx_max_latency.push((block_height, max_latency));
        series.regular_tx_finalized_count.push((block_height, finalized_count));
    }
    
    // Record system metrics for this block
    results.memory_usage.push((block_height, crate::SimulationResults::get_current_memory_usage()));
    results.total_memory.push((block_height, crate::SimulationResults::get_current_total_memory()));
    results.cpu_usage.push((block_height, crate::SimulationResults::get_current_cpu_usage()));
    results.total_cpu_usage.push((block_height, crate::SimulationResults::get_current_total_cpu_usage()));
    
    // Record CL queue length for this block
    let cl_queue_length = cl_node.lock().await.get_pending_transactions().await
        .map_err(|e| format!("Failed to get CL queue length: {}", e))?;
    results.cl_queue_length.push((block_height, cl_queue_length as u64));
    
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Transaction Release Functions
// ------------------------------------------------------------------------------------------------

/// Picks the chains a CAT touches, as indices into `chains`, sorted ascending
fn select_cat_chains(rng: &mut rand::rngs::ThreadRng, num_chains: usize, cat_span: CatSpan) -> Vec<usize> {
    match cat_span {
        CatSpan::AllChains => (0..num_chains).collect(),
        CatSpan::TwoChains => {
            let mut pair = sample(rng, num_chains, 2).into_vec();
            pair.sort_unstable();
            pair
        }
    }
}

/// Releases all transactions for a single block
async fn release_transactions_for_block(
    cl_node: &Arc<Mutex<ConfirmationLayerNode>>,
    rng: &mut rand::rngs::ThreadRng,
    account_selector_sender: &mut AccountSelector,
    account_selector_receiver: &mut AccountSelector,
    results: &mut SimulationResults,
    chains: &[ChainId],
    transactions_per_block: u64,
    current_block: u64,
    height_delta: u64,
) -> Result<(), String> {
    // Calculate total transactions to send (compensate for missed blocks)
    let total_transactions = transactions_per_block * height_delta;
    
    logging::log("SIMULATOR", &format!("Releasing {} transactions ({} per block × {} blocks)", 
        total_transactions, transactions_per_block, height_delta));
    
    for tx_index in 0..total_transactions {
        // Select accounts for transaction
        let from_account = account_selector_sender.select_account(rng);
        let to_account = account_selector_receiver.select_account(rng);
        
        // Record transaction in account statistics
        results.account_stats.record_transaction(from_account as u64, to_account as u64);
        
        // Determine if this should be a CAT transaction based on configured ratio
        let is_cat = rng.gen_bool(results.ratio_cats);
        
        // Create transaction data
        let tx_data = format!("{}.send {} {} 1", 
            if is_cat { "CAT" } else { "REGULAR" },
            from_account,
            to_account
        );
        
        // Create and submit transaction
        let cl_id = CLTransactionId(format!("cl-{}-tx_{}", 
            if is_cat { "cat" } else { "reg" }, 
            results.transactions_sent
        ));
        
        let success = if is_cat {
            results.cat_transactions += 1;
            let indices = select_cat_chains(rng, chains.len(), results.cat_span);
            let cat_chains: Vec<ChainId> = indices.iter().map(|&i| chains[i].clone()).collect();
            let submitted = create_and_submit_cat_transaction(cl_node, cl_id.clone(), &cat_chains, tx_data.clone()).await?;
            results.cats_issued.push(IssuedCAT { cat_id: cl_id.0.clone(), block: current_block, chains: indices.iter().map(|i| i + 1).collect() });
            submitted
        } else {
            results.regular_transactions += 1;
            create_and_submit_regular_transaction(cl_node, cl_id, chains, tx_data.clone()).await?
        };

        if success {
            logging::log("SIMULATOR", &format!("Transaction {} successful: {}", tx_index + 1, tx_data));
        } else {
            logging::log("SIMULATOR", &format!("Transaction {} failed: {}", tx_index + 1, tx_data));
            panic!("Transaction failed submitted to CL");
        }
        
        results.transactions_sent += 1;
    }
    
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Transaction Creation and Submission
// ------------------------------------------------------------------------------------------------

/// Creates and submits a CAT transaction with one sub-transaction per chain in `cat_chains`
///
/// # Arguments
///
/// * `cl_node` - A reference to the confirmation layer node
/// * `cl_id` - A CLTransactionId, the ID of the CL transaction
/// * `cat_chains` - The chains the CAT touches (at least two)
/// * `tx_data` - A String, the data of the transaction
///
/// # Returns
///
/// Whether the CL accepted the transaction
async fn create_and_submit_cat_transaction(
    cl_node: &Arc<Mutex<ConfirmationLayerNode>>,
    cl_id: CLTransactionId,
    cat_chains: &[ChainId],
    tx_data: String,
) -> Result<bool, String> {
    let mut sub_txs = Vec::with_capacity(cat_chains.len());
    for (i, chain_id) in cat_chains.iter().enumerate() {
        let tx = Transaction::new(
            TransactionId(format!("{:?}:tx{}", cl_id, i + 1)),
            chain_id.clone(),
            cat_chains.to_vec(),
            tx_data.clone(),
            cl_id.clone(),
        ).map_err(|e| {
            logging::log("SIMULATOR", &format!("Failed to create CAT-sub-transaction {}: {}", i + 1, e));
            e.to_string()
        })?;
        sub_txs.push(tx);
    }

    let cl_tx = CLTransaction::new(cl_id.clone(), cat_chains.to_vec(), sub_txs).map_err(|e| {
        logging::log("SIMULATOR", &format!("Failed to create CAT CL transaction: {}", e));
        e.to_string()
    })?;

    logging::log("SIMULATOR", &format!("Created CAT CL transaction with ID: {:?}", cl_id));

    match cl_node.lock().await.submit_transaction(cl_tx).await {
        Ok(_) => {
            logging::log("SIMULATOR", &format!("CAT transaction submitted successfully: {}", tx_data));
            Ok(true)
        }
        Err(e) => {
            logging::log("SIMULATOR", &format!("Failed to submit CAT CL transaction: {}", e));
            Ok(false)
        }
    }
}

/// Creates and submits a regular transaction, mirrored to every chain as its own CL transaction
///
/// # Arguments
///
/// * `cl_node` - A reference to the confirmation layer node
/// * `cl_id` - A CLTransactionId, the base ID of the CL transactions
/// * `chains` - All chains; each gets one copy of the transaction
/// * `tx_data` - A String, the data of the transaction
///
/// # Returns
///
/// Whether the CL accepted every copy
async fn create_and_submit_regular_transaction(
    cl_node: &Arc<Mutex<ConfirmationLayerNode>>,
    cl_id: CLTransactionId,
    chains: &[ChainId],
    tx_data: String,
) -> Result<bool, String> {
    let mut all_submitted = true;
    for (i, chain_id) in chains.iter().enumerate() {
        let cl_id_i = CLTransactionId(format!("{:?}_{}", cl_id, i + 1));
        let tx = Transaction::new(
            TransactionId(format!("{:?}:tx", cl_id_i)),
            chain_id.clone(),
            vec![chain_id.clone()],
            tx_data.clone(),
            cl_id_i.clone(),
        ).map_err(|e| {
            logging::log("SIMULATOR", &format!("Failed to create regular transaction for {}: {}", chain_id.0, e));
            e.to_string()
        })?;
        let cl_tx = CLTransaction::new(cl_id_i, vec![chain_id.clone()], vec![tx]).map_err(|e| {
            logging::log("SIMULATOR", &format!("Failed to create regular CL transaction for {}: {}", chain_id.0, e));
            e.to_string()
        })?;
        match cl_node.lock().await.submit_transaction(cl_tx).await {
            Ok(_) => logging::log("SIMULATOR", &format!("Regular transaction submitted successfully to {}: {}", chain_id.0, tx_data)),
            Err(e) => {
                logging::log("SIMULATOR", &format!("Failed to submit regular transaction to {}: {}", chain_id.0, e));
                all_submitted = false;
            }
        }
    }
    Ok(all_submitted)
}
