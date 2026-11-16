//! Non-interactive runner for the multi-chain comparison.
//!
//! Runs every combination of chain count N and CAT span (all chains, or a random pair) and saves each
//! run under `simulator/results/sim_multichain/<span>/n_<N>/run_<r>/data/`.
//!
//! Usage (from the repository root):
//! `cargo run --release -p simulator --bin multichain -- [--chains 2,3,4,5] [--spans all_chains,two_chains] [--runs 3] [--blocks 100] [--zipf 0,0.8]`
//!
//! With `--slow-chain-delay D`, chain-1 sends to the coordinator D blocks late and the other chains stay fast.
//!
//! With `--zipf`, each Zipf value gets its own folder level: `<out>/zipf_<z>/<span>/n_<N>/run_<r>/`.

use simulator::config::{CatSpan, Config};
use simulator::SimulationResults;
use hyperplane::utils::logging;
use std::time::{Duration, Instant};

/// Parsed command-line options; unset options fall back to the config file
struct Options {
    chains: Vec<usize>,
    spans: Vec<CatSpan>,
    runs: Option<u32>,
    blocks: Option<u64>,
    zipfs: Vec<Option<f64>>,
    slow_chain_delay: Option<f64>,
    config_path: String,
    out_dir: String,
}

fn parse_span(s: &str) -> CatSpan {
    match s {
        "all_chains" | "all" => CatSpan::AllChains,
        "two_chains" | "two" | "pair" => CatSpan::TwoChains,
        other => panic!("Unknown span '{}', expected all_chains or two_chains", other),
    }
}

fn parse_options() -> Options {
    let mut options = Options {
        chains: vec![2, 3, 4, 5],
        spans: vec![CatSpan::AllChains, CatSpan::TwoChains],
        runs: None,
        blocks: None,
        zipfs: vec![None],
        slow_chain_delay: None,
        config_path: "simulator/src/scenarios/sim_multichain/config.toml".to_string(),
        out_dir: "simulator/results/sim_multichain".to_string(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let value = args.get(i + 1).unwrap_or_else(|| panic!("Missing value for {}", args[i])).clone();
        match args[i].as_str() {
            "--chains" => options.chains = value.split(',').map(|v| v.trim().parse().expect("chains must be integers")).collect(),
            "--spans" => options.spans = value.split(',').map(|v| parse_span(v.trim())).collect(),
            "--runs" => options.runs = Some(value.parse().expect("runs must be an integer")),
            "--zipf" => options.zipfs = value.split(',').map(|v| Some(v.trim().parse().expect("zipf must be a number"))).collect(),
            "--slow-chain-delay" => options.slow_chain_delay = Some(value.parse().expect("slow-chain-delay must be a number")),
            "--blocks" => options.blocks = Some(value.parse().expect("blocks must be an integer")),
            "--config" => options.config_path = value,
            "--out" => options.out_dir = value,
            other => panic!("Unknown option '{}'", other),
        }
        i += 2;
    }
    options
}

/// Builds the results container for one run from the cell's config
fn initialize_results(config: &Config) -> SimulationResults {
    let mut results = SimulationResults::default();
    results.initial_balance = config.account_config.initial_balance as u64;
    results.num_accounts = config.account_config.num_accounts;
    results.target_tpb = config.transaction_config.target_tpb as u64;
    results.sim_total_block_number = config.simulation_config.sim_total_block_number;
    results.zipf_parameter = config.transaction_config.zipf_parameter;
    results.ratio_cats = config.transaction_config.ratio_cats;
    results.block_interval = config.network_config.block_interval;
    results.cat_lifetime = config.transaction_config.cat_lifetime_blocks;
    results.initialization_wait_blocks = config.simulation_config.initialization_wait_blocks;
    results.chain_delays = config.network_config.chain_delays.clone();
    results.cat_span = config.transaction_config.cat_span;
    results.start_time = Instant::now();
    results
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options();
    let base: Config = toml::from_str(&std::fs::read_to_string(&options.config_path)?)?;
    let base_delay = base.network_config.chain_delays.first().copied().unwrap_or(0.0);
    logging::init_logging_with_config(false, false, None);

    std::fs::create_dir_all(&options.out_dir)?;
    std::fs::copy(&options.config_path, format!("{}/config.toml", options.out_dir))?;

    for &zipf in &options.zipfs {
    for &span in &options.spans {
        for &num_chains in &options.chains {
            let mut config = base.clone();
            config.network_config.num_chains = num_chains;
            config.network_config.chain_delays = vec![base_delay; num_chains];
            // One slow chain, as in the two-chain evaluation: chain-1 gets the slow delay, the others stay fast
            if let Some(d) = options.slow_chain_delay { config.network_config.chain_delays[0] = d; }
            config.transaction_config.cat_span = span;
            if let Some(runs) = options.runs { config.simulation_config.num_runs = runs; }
            if let Some(blocks) = options.blocks { config.simulation_config.sim_total_block_number = blocks; }
            if let Some(z) = zipf { config.transaction_config.zipf_parameter = z; }
            // With a zipf override the cell gets its own folder level
            let cell_root = match zipf { Some(z) => format!("{}/zipf_{}", options.out_dir, z), None => options.out_dir.clone() };
            config.validate()?;

            let num_runs = config.simulation_config.num_runs;
            for run in 0..num_runs {
                let run_dir = format!("{}/{}/n_{}/run_{}", cell_root, span.label(), num_chains, run);
                println!("=== {} N={} run {}/{} -> {} ===", span.label(), num_chains, run + 1, num_runs, run_dir);
                let mut results = initialize_results(&config);
                let nodes = simulator::testnodes::setup_test_nodes(
                    Duration::from_secs_f64(config.network_config.block_interval),
                    &config.network_config.chain_delays,
                    config.transaction_config.allow_cat_pending_dependencies,
                    config.transaction_config.cat_lifetime_blocks,
                    config.account_config.num_accounts as u32,
                    config.account_config.initial_balance as u32,
                    config.network_config.channel_buffer_size,
                ).await;
                let message = format!("{} N={} run {}/{}", span.label(), num_chains, run + 1, num_runs);
                simulator::run_simulation::run_simulation_with_message_and_retries(
                    nodes.cl_node.clone(), nodes.hig_nodes.clone(), &mut results, Some(message), None,
                ).await?;
                simulator::run_simulation::collect_hs_message_counts(&nodes.hs_node, &mut results).await;
                nodes.shutdown().await;
                results.save_to_directory(&run_dir).await?;
            }
        }
    }
    }
    println!("All runs complete. Results in {}", options.out_dir);
    Ok(())
}
