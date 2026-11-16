//! Integration tests for CATs on more than two chains, using the simulator's N-chain node setup.

use std::sync::Arc;
use hyperplane::{
    confirmation_layer::{ConfirmationLayer, ConfirmationLayerNode},
    hyper_ig::HyperIG,
    types::{ChainId, CLTransaction, CLTransactionId, Transaction, TransactionId, TransactionStatus},
};
use simulator::testnodes::{setup_test_nodes, TestNodes};
use tokio::sync::Mutex;
use tokio::time::Duration;

const BLOCK_INTERVAL: Duration = Duration::from_millis(100);

/// Sets up N chains with 10 preloaded accounts of 100 tokens each and zero HS delays
async fn setup(num_chains: usize) -> TestNodes {
    setup_test_nodes(BLOCK_INTERVAL, &vec![0.0; num_chains], true, 20, 10, 100, 1000).await
}

fn chain(i: usize) -> ChainId {
    ChainId(format!("chain-{}", i))
}

/// Submits one CAT with one sub-transaction per chain in `chains` and returns the sub-transaction ids
async fn submit_cat(cl_node: &Arc<Mutex<ConfirmationLayerNode>>, id: &str, chains: &[ChainId], data: &str) -> Vec<TransactionId> {
    let cl_id = CLTransactionId(id.to_string());
    let txs: Vec<Transaction> = chains.iter().enumerate().map(|(i, c)| {
        Transaction::new(TransactionId(format!("{}:tx{}", id, i + 1)), c.clone(), chains.to_vec(), data.to_string(), cl_id.clone()).unwrap()
    }).collect();
    let ids = txs.iter().map(|t| t.id.clone()).collect();
    cl_node.lock().await.submit_transaction(CLTransaction::new(cl_id, chains.to_vec(), txs).unwrap()).await.unwrap();
    ids
}

/// Submits one regular transaction to a single chain
async fn submit_regular(cl_node: &Arc<Mutex<ConfirmationLayerNode>>, id: &str, chain_id: ChainId, data: &str) {
    let cl_id = CLTransactionId(id.to_string());
    let tx = Transaction::new(TransactionId(format!("{}:tx", id)), chain_id.clone(), vec![chain_id.clone()], data.to_string(), cl_id.clone()).unwrap();
    cl_node.lock().await.submit_transaction(CLTransaction::new(cl_id, vec![chain_id], vec![tx]).unwrap()).await.unwrap();
}

/// Reads the status of each sub-transaction on its own chain (sub-transaction i lives on chain i+1 of `chain_indices`)
async fn statuses(nodes: &TestNodes, chain_indices: &[usize], tx_ids: &[TransactionId]) -> Vec<TransactionStatus> {
    let mut out = Vec::new();
    for (&c, tx_id) in chain_indices.iter().zip(tx_ids) {
        out.push(nodes.hig_nodes[c - 1].lock().await.get_transaction_status(tx_id.clone()).await.unwrap());
    }
    out
}

/// A CAT across all N chains succeeds on every chain, and the HS exchanges N proposals and N status sub-txs.
#[tokio::test]
async fn test_cat_across_all_chains_succeeds() {
    for num_chains in [3usize, 4] {
        let nodes = setup(num_chains).await;
        let indices: Vec<usize> = (1..=num_chains).collect();
        let chains: Vec<ChainId> = indices.iter().map(|&i| chain(i)).collect();
        let tx_ids = submit_cat(&nodes.cl_node, &format!("cat-all-{}", num_chains), &chains, "CAT.send 1 2 10").await;
        tokio::time::sleep(BLOCK_INTERVAL * 8).await;

        assert_eq!(statuses(&nodes, &indices, &tx_ids).await, vec![TransactionStatus::Success; num_chains], "N={}", num_chains);
        let counts = nodes.hs_node.lock().await.get_message_counts().await;
        assert_eq!(counts.proposals_received, num_chains as u64, "one proposal per chain, N={}", num_chains);
        assert_eq!(counts.status_updates_sent, 1, "one status update per CAT, N={}", num_chains);
        assert_eq!(counts.status_update_subtxs_sent, num_chains as u64, "one status sub-tx per chain, N={}", num_chains);
        nodes.shutdown().await;
    }
}

/// A CAT across 4 chains fails on every chain when it can only fail on one of them (chain-3 lacks the funds).
#[tokio::test]
async fn test_cat_failure_on_one_chain_aborts_all() {
    let nodes = setup(4).await;
    // Drain account 5 on chain-3 only
    submit_regular(&nodes.cl_node, "drain-5", chain(3), "REGULAR.send 5 6 100").await;
    tokio::time::sleep(BLOCK_INTERVAL * 4).await;

    let indices = vec![1, 2, 3, 4];
    let chains: Vec<ChainId> = indices.iter().map(|&i| chain(i)).collect();
    let tx_ids = submit_cat(&nodes.cl_node, "cat-fail", &chains, "CAT.send 5 7 50").await;
    tokio::time::sleep(BLOCK_INTERVAL * 8).await;

    assert_eq!(statuses(&nodes, &indices, &tx_ids).await, vec![TransactionStatus::Failure; 4]);
    // The failure decision is sent once, even though later proposals still arrive
    let per_cat = nodes.hs_node.lock().await.get_per_cat_message_counts().await;
    let counts = per_cat.iter().find(|(id, _)| id.0.0 == "cat-fail").map(|(_, c)| *c).expect("HS saw the CAT");
    assert_eq!(counts.status_updates_sent, 1);
    nodes.shutdown().await;
}

/// A two-chain CAT inside a four-chain network touches only its two chains.
#[tokio::test]
async fn test_two_chain_cat_in_four_chain_network() {
    let nodes = setup(4).await;
    let indices = vec![2, 4];
    let chains: Vec<ChainId> = indices.iter().map(|&i| chain(i)).collect();
    let tx_ids = submit_cat(&nodes.cl_node, "cat-pair", &chains, "CAT.send 1 2 10").await;
    tokio::time::sleep(BLOCK_INTERVAL * 8).await;

    assert_eq!(statuses(&nodes, &indices, &tx_ids).await, vec![TransactionStatus::Success; 2]);
    let counts = nodes.hs_node.lock().await.get_message_counts().await;
    assert_eq!(counts.proposals_received, 2);
    assert_eq!(counts.status_update_subtxs_sent, 2);
    // chain-1 and chain-3 never saw the CAT
    for c in [1usize, 3] {
        let (cat_pending, cat_success, cat_failure) = nodes.hig_nodes[c - 1].lock().await.get_transaction_status_counts_cats().await.unwrap();
        assert_eq!(cat_pending + cat_success + cat_failure, 0, "chain-{} should not see the CAT", c);
    }
    nodes.shutdown().await;
}

/// A CAT postponed behind another CAT's lock is decided once the blocker resolves, well before its lifetime ends.
#[tokio::test]
async fn test_postponed_cat_resumes_after_blocker() {
    let nodes = setup(3).await;
    let indices = vec![1, 2, 3];
    let chains: Vec<ChainId> = indices.iter().map(|&i| chain(i)).collect();
    // Both CATs touch account 1, so the second waits for the first on every chain
    let first = submit_cat(&nodes.cl_node, "cat-first", &chains, "CAT.send 1 2 10").await;
    let second = submit_cat(&nodes.cl_node, "cat-second", &chains, "CAT.send 1 3 10").await;
    // Lifetime is 20 blocks; 10 blocks is enough for two sequential decisions
    tokio::time::sleep(BLOCK_INTERVAL * 10).await;

    assert_eq!(statuses(&nodes, &indices, &first).await, vec![TransactionStatus::Success; 3]);
    assert_eq!(statuses(&nodes, &indices, &second).await, vec![TransactionStatus::Success; 3], "the postponed CAT must not wait for its timeout");
    nodes.shutdown().await;
}
