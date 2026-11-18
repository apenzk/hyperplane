#![cfg(feature = "test")]

use hyperplane::{
    confirmation_layer::ConfirmationLayer,
    hyper_ig::HyperIG,
    types::{constants, TransactionId, TransactionStatus},
};
use crate::integration::common::{submit_transactions, testnodes};
use tokio::time::Duration;

const BLOCK_INTERVAL: Duration = Duration::from_millis(100);

/// A CAT postponed behind another CAT's lock is decided once the blocker resolves, within its lifetime.
/// - CAT A and CAT B both touch account 1, so B waits for A on both chains
/// - after A succeeds, B is re-executed and must send its proposal, so it succeeds instead of timing out
#[tokio::test]
async fn test_postponed_cat_resumes_after_blocker() {
    let (_hs, cl_node, hig_node_1, hig_node_2, _start) =
        testnodes::setup_test_nodes_with_preloaded_accounts(BLOCK_INTERVAL, 10, 100).await;

    submit_transactions::create_and_submit_cat_transaction(&cl_node, "send 1 2 10", "cat-first").await.unwrap();
    submit_transactions::create_and_submit_cat_transaction(&cl_node, "send 1 3 10", "cat-second").await.unwrap();
    // The CAT lifetime in this setup is 4 blocks; two sequential decisions need about 3
    tokio::time::sleep(BLOCK_INTERVAL * 8).await;

    for (hig, chain) in [(&hig_node_1, constants::chain_1()), (&hig_node_2, constants::chain_2())] {
        for cat in ["cat-first", "cat-second"] {
            let status = hig.lock().await
                .get_transaction_status(TransactionId(format!("{}.{}", cat, chain.0)))
                .await
                .unwrap();
            assert_eq!(status, TransactionStatus::Success, "{} on {}", cat, chain.0);
        }
    }
}

/// The hyper scheduler sends each CAT decision exactly once.
/// - 20 CATs on disjoint accounts are decided in the same blocks
/// - every CAT has exactly one status update in chain-1's subblocks
/// The duplicate came from a race between the per-chain receive loops, so this test guards it rather than
/// reproducing it deterministically.
#[tokio::test]
async fn test_each_cat_decision_sent_once() {
    let (_hs, cl_node, _hig_node_1, _hig_node_2, start) =
        testnodes::setup_test_nodes_with_preloaded_accounts(BLOCK_INTERVAL, 60, 100).await;

    for i in 0..20 {
        let data = format!("send {} {} 1", 2 * i + 1, 2 * i + 2);
        submit_transactions::create_and_submit_cat_transaction(&cl_node, &data, &format!("cat-{}", i)).await.unwrap();
    }
    tokio::time::sleep(BLOCK_INTERVAL * 8).await;

    let current = cl_node.lock().await.get_current_block().await.unwrap();
    let mut updates = std::collections::HashMap::new();
    for height in start..=current {
        if let Ok(subblock) = cl_node.lock().await.get_subblock(constants::chain_1(), height).await {
            for tx in subblock.transactions.iter().filter(|tx| tx.data.starts_with("STATUS_UPDATE")) {
                *updates.entry(tx.cl_id.0.clone()).or_insert(0) += 1;
            }
        }
    }
    assert_eq!(updates.len(), 20, "every CAT gets a decision: {:?}", updates);
    for (cl_id, count) in &updates {
        assert_eq!(*count, 1, "{} received {} status updates", cl_id, count);
    }
}
