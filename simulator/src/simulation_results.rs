//! Simulation results storage and serialization.
//! 
//! Handles saving simulation data to JSON files for analysis.

use std::time::Instant;
use std::fs;
use serde_json;
use crate::account_selection::AccountSelectionStats;
use crate::config::CatSpan;
use hyperplane::hyper_ig::node::CATLatencyRecord;
use hyperplane::hyper_scheduler::node::HSMessageCounts;
use hyperplane::utils::logging;
use sysinfo::System;
use std::sync::Mutex;
use lazy_static::lazy_static;

// ------------------------------------------------------------------------------------------------
// Global System Instance
// ------------------------------------------------------------------------------------------------

lazy_static! {
    static ref SYSTEM: Mutex<System> = Mutex::new(System::new_all());
}

fn get_system() -> &'static Mutex<System> {
    &SYSTEM
}

// ------------------------------------------------------------------------------------------------
// Data Structures
// ------------------------------------------------------------------------------------------------

/// Per-block time series of one chain. Every entry is (block_height, value).
#[derive(Debug, Clone, Default)]
pub struct ChainSeries {
    // Combined totals (CAT + regular)
    pub pending: Vec<(u64, u64)>,
    pub success: Vec<(u64, u64)>,
    pub failure: Vec<(u64, u64)>,
    // CAT transactions
    pub cat_pending: Vec<(u64, u64)>,
    pub cat_success: Vec<(u64, u64)>,
    pub cat_failure: Vec<(u64, u64)>,
    // Detailed CAT pending states
    pub cat_pending_resolving: Vec<(u64, u64)>,
    pub cat_pending_postponed: Vec<(u64, u64)>,
    // Regular transactions
    pub regular_pending: Vec<(u64, u64)>,
    pub regular_success: Vec<(u64, u64)>,
    pub regular_failure: Vec<(u64, u64)>,
    // Locked keys
    pub locked_keys: Vec<(u64, u64)>,
    // Transactions per block (excluding status updates)
    pub tx_per_block: Vec<(u64, u64)>,
    // Regular transaction timing metrics
    pub regular_tx_avg_latency: Vec<(u64, f64)>, // average latency in ms
    pub regular_tx_max_latency: Vec<(u64, f64)>, // max latency in ms
    pub regular_tx_finalized_count: Vec<(u64, u64)>,
    /// Final status record of every CAT finalized on this chain, collected at the end of the run
    pub cat_latency_records: Vec<CATLatencyRecord>,
}

/// One CAT issued by the simulator
#[derive(Debug, Clone)]
pub struct IssuedCAT {
    /// The CAT identifier (equals its CL transaction id)
    pub cat_id: String,
    /// Block height at which the simulator submitted it
    pub block: u64,
    /// The chains it touches, 1-based (1 = chain-1)
    pub chains: Vec<usize>,
}

/// Stores all simulation results and statistics
#[derive(Debug, Clone)]
pub struct SimulationResults {
    // Transaction counts
    pub transactions_sent: u64,
    pub cat_transactions: u64,
    pub regular_transactions: u64,
    
    // Simulation parameters
    pub initial_balance: u64,
    pub num_accounts: usize,
    pub target_tpb: u64,
    pub sim_total_block_number: u64,  // Total number of blocks to simulate
    pub zipf_parameter: f64,
    pub ratio_cats: f64,
    pub block_interval: f64,
    pub cat_lifetime: u64,
    pub initialization_wait_blocks: u64,
    pub transaction_submission_frequency: u64,  // How many times per block to check for transaction submission
    pub chain_delays: Vec<f64>,  // Chain delays in blocks, one entry per chain
    pub cat_span: CatSpan,  // Which chains each CAT touches

    /// Per-chain series, index 0 is chain-1
    pub chains: Vec<ChainSeries>,

    // Memory usage tracking
    pub memory_usage: Vec<(u64, u64)>, // (block_height, memory_usage_bytes)
    pub total_memory: Vec<(u64, u64)>, // (block_height, total_memory_bytes)
    
    // CPU usage tracking
    pub cpu_usage: Vec<(u64, f64)>, // (block_height, process_cpu_usage_percent)
    pub total_cpu_usage: Vec<(u64, f64)>, // (block_height, total_system_cpu_usage_percent)
    
    // Loop steps without transaction issuance tracking
    pub loop_steps_without_tx_issuance: Vec<(u64, u64)>, // (block_height, loop_steps_count)
    
    // Block height delta tracking
    pub block_height_delta: Vec<(u64, u64)>, // (block_height, height_delta)
    
    // CL queue length tracking
    pub cl_queue_length: Vec<(u64, u64)>, // (block_height, queue_length)

    /// Hyper scheduler message counters at the end of the run
    pub hs_message_counts: HSMessageCounts,
    /// Hyper scheduler message counters per CAT id, at the end of the run
    pub hs_per_cat_message_counts: Vec<(String, HSMessageCounts)>,
    /// Every CAT the simulator issued, in issue order
    pub cats_issued: Vec<IssuedCAT>,
    /// Last block height observed by the simulation loop
    pub final_block: u64,
    
    // Statistics
    pub account_stats: AccountSelectionStats,
    pub start_time: Instant,
}

// ------------------------------------------------------------------------------------------------
// Implementations
// ------------------------------------------------------------------------------------------------

impl Default for SimulationResults {
    fn default() -> Self {
        Self {
            transactions_sent: 0,
            cat_transactions: 0,
            regular_transactions: 0,
            initial_balance: 0,
            num_accounts: 0,
            target_tpb: 0,
            sim_total_block_number: 0,
            zipf_parameter: 0.0,
            ratio_cats: 0.0,
            block_interval: 0.0,
            cat_lifetime: 0,
            initialization_wait_blocks: 0,
            transaction_submission_frequency: 10,  // Default to 10 times per block
            chain_delays: Vec::new(),
            cat_span: CatSpan::default(),
            chains: Vec::new(),
            memory_usage: Vec::new(),
            total_memory: Vec::new(),
            cpu_usage: Vec::new(),
            total_cpu_usage: Vec::new(),
            loop_steps_without_tx_issuance: Vec::new(),
            block_height_delta: Vec::new(),
            cl_queue_length: Vec::new(),
            hs_message_counts: HSMessageCounts::default(),
            hs_per_cat_message_counts: Vec::new(),
            cats_issued: Vec::new(),
            final_block: 0,
            account_stats: AccountSelectionStats::new(),
            start_time: Instant::now(),
        }
    }
}

/// Writes one (height, value) series as `{ "<key>": [{"height": h, "<field>": v}, ...] }`
fn write_series<V: serde::Serialize>(path: &str, key: &str, field: &str, series: &[(u64, V)]) -> Result<(), String> {
    let entries: Vec<serde_json::Value> = series.iter().map(|(height, value)| {
        let mut entry = serde_json::Map::new();
        entry.insert("height".to_string(), serde_json::json!(height));
        entry.insert(field.to_string(), serde_json::json!(value));
        serde_json::Value::Object(entry)
    }).collect();
    let data = serde_json::json!({ key: entries });
    fs::write(path, serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    logging::log("SIMULATOR", &format!("Saved {} to {}", key, path));
    Ok(())
}

impl SimulationResults {
    /// Gets the current memory usage in bytes
    pub fn get_current_memory_usage() -> u64 {
        // Use sysinfo crate or similar for more accurate memory measurement
        // For now, use a simple approximation based on process memory
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
                for line in contents.lines() {
                    if line.starts_with("VmRSS:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                return kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
            0 // Fallback if we can't read the file or find VmRSS
        }
        
        #[cfg(target_os = "macos")]
        {
            // On macOS, try to get memory usage using libc
            #[allow(deprecated)]
            unsafe {
                use std::mem;
                let mut info: libc::mach_task_basic_info = mem::zeroed();
                let mut count = mem::size_of::<libc::mach_task_basic_info>() as libc::mach_msg_type_number_t;
                
                let result = libc::task_info(
                    libc::mach_task_self(),
                    libc::MACH_TASK_BASIC_INFO,
                    &mut info as *mut _ as *mut i32,
                    &mut count,
                );
                
                if result == libc::KERN_SUCCESS {
                    return info.resident_size;
                }
            }
            
            // Fallback: try to read from /proc/self/statm if available
            if let Ok(contents) = std::fs::read_to_string("/proc/self/statm") {
                if let Some(first) = contents.split_whitespace().next() {
                    if let Ok(pages) = first.parse::<u64>() {
                        return pages * 4096; // Convert pages to bytes (assuming 4KB pages)
                    }
                }
            }
            
            // Final fallback: return a reasonable estimate
            // Since we can't easily get heap usage, return 0 for now
            0
        }
        
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            // On other platforms, try to read from /proc/self/statm if available
            if let Ok(contents) = std::fs::read_to_string("/proc/self/statm") {
                if let Some(first) = contents.split_whitespace().next() {
                    if let Ok(pages) = first.parse::<u64>() {
                        return pages * 4096; // Convert pages to bytes (assuming 4KB pages)
                    }
                }
            }
            
            // Fallback: return 0 for now
            0
        }
    }

    /// Gets the current total RAM usage in bytes
    pub fn get_current_total_memory() -> u64 {
        let system = get_system();
        if let Ok(mut sys) = system.lock() {
            // Refresh memory information
            sys.refresh_memory();
            
            return sys.used_memory(); // sysinfo returns bytes directly
        }
        
        // Fallback if we can't get system info
        0
    }

    /// Gets the current process CPU usage as a percentage
    /// Uses sysinfo crate for accurate cross-platform CPU measurement
    pub fn get_current_cpu_usage() -> f64 {
        let system = get_system();
        if let Ok(mut sys) = system.lock() {
            // Refresh process information
            sys.refresh_processes();
            
            // Get current process ID
            let pid = sysinfo::Pid::from(std::process::id() as usize);
            
            // Get process CPU usage percentage
            if let Some(process) = sys.process(pid) {
                return process.cpu_usage() as f64;
            }
        }
        
        // Fallback if we can't get process info
        0.0
    }

    /// Gets the current total system CPU usage as a percentage
    /// Uses sysinfo crate for accurate cross-platform CPU measurement
    pub fn get_current_total_cpu_usage() -> f64 {
        let system = get_system();
        if let Ok(mut sys) = system.lock() {
            // Refresh CPU information
            sys.refresh_cpu();
            
            // Get overall CPU usage percentage
            let cpu_usage = sys.global_cpu_info().cpu_usage();
            return cpu_usage as f64;
        }
        
        // Fallback if we can't get system info
        0.0
    }

    /// Saves results to the default directory
    pub async fn save(&self) -> Result<(), String> {
        self.save_to_directory("simulator/results/sim_simple").await
    }

    /// Saves all simulation data to JSON files in the specified directory
    pub async fn save_to_directory(&self, base_dir: &str) -> Result<(), String> {
        // Print final statistics
        logging::log("SIMULATOR", "\n=== Simulation Statistics ===");
        logging::log("SIMULATOR", &format!("Total Transactions: {}", self.transactions_sent));
        logging::log("SIMULATOR", &format!("CAT Transactions: {}", self.cat_transactions));
        logging::log("SIMULATOR", &format!("Regular Transactions: {}", self.regular_transactions));
        logging::log("SIMULATOR", &format!("Actual TPS: {:.2}", self.transactions_sent as f64 / self.start_time.elapsed().as_secs_f64()));
        logging::log("SIMULATOR", "===========================");
        
        // Save statistics to JSON file
        let stats = serde_json::json!({
            "parameters": {
                "initial_balance": self.initial_balance,
                "num_accounts": self.num_accounts,
                "target_tpb": self.target_tpb,
                "sim_total_block_number": self.sim_total_block_number,
                "zipf_parameter": self.zipf_parameter,
                "ratio_cats": self.ratio_cats,
                "block_interval": self.block_interval,
                "chain_delays": self.chain_delays.clone(),
                "num_chains": self.chains.len(),
                "cat_span": self.cat_span.label(),
                "cat_lifetime": self.cat_lifetime
            },
            "results": {
                "total_transactions": self.transactions_sent,
                "cat_transactions": self.cat_transactions,
                "regular_transactions": self.regular_transactions,
                "final_block": self.final_block,
                "hs_proposals_received": self.hs_message_counts.proposals_received,
                "hs_status_updates_sent": self.hs_message_counts.status_updates_sent,
                "hs_status_update_subtxs_sent": self.hs_message_counts.status_update_subtxs_sent
            }
        });

        // Create results directories if they don't exist
        fs::create_dir_all(&format!("{}/data", base_dir)).expect("Failed to create results directory");

        // Save simulation stats
        let stats_file = format!("{}/data/simulation_stats.json", base_dir);
        fs::write(&stats_file, serde_json::to_string_pretty(&stats).expect("Failed to serialize stats")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved simulation statistics to {}", stats_file));

        // Save the list of issued CATs (the denominator for CAT success rate and finality)
        let issued: Vec<serde_json::Value> = self.cats_issued.iter().map(|c| serde_json::json!({
            "cat_id": c.cat_id,
            "block": c.block,
            "chains": c.chains,
        })).collect();
        let issued_file = format!("{}/data/cats_issued.json", base_dir);
        fs::write(&issued_file, serde_json::to_string_pretty(&serde_json::json!({ "cats_issued": issued })).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;

        // Save the hyper scheduler message counts per CAT
        let per_cat: Vec<serde_json::Value> = self.hs_per_cat_message_counts.iter().map(|(cat_id, c)| serde_json::json!({
            "cat_id": cat_id,
            "proposals_received": c.proposals_received,
            "status_updates_sent": c.status_updates_sent,
            "status_update_subtxs_sent": c.status_update_subtxs_sent,
        })).collect();
        let per_cat_file = format!("{}/data/hs_cat_messages.json", base_dir);
        fs::write(&per_cat_file, serde_json::to_string_pretty(&serde_json::json!({ "hs_cat_messages": per_cat })).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;

        // Save per-chain series. File names and JSON keys follow <prefix>_chain_<i>.json / chain_<i>_<metric>.
        for (index, chain) in self.chains.iter().enumerate() {
            let i = index + 1;
            let counts: [(&str, &str, &Vec<(u64, u64)>); 14] = [
                ("pending_transactions", "pending", &chain.pending),
                ("success_transactions", "success", &chain.success),
                ("failure_transactions", "failure", &chain.failure),
                ("cat_pending_transactions", "cat_pending", &chain.cat_pending),
                ("cat_success_transactions", "cat_success", &chain.cat_success),
                ("cat_failure_transactions", "cat_failure", &chain.cat_failure),
                ("cat_pending_resolving_transactions", "cat_pending_resolving", &chain.cat_pending_resolving),
                ("cat_pending_postponed_transactions", "cat_pending_postponed", &chain.cat_pending_postponed),
                ("regular_pending_transactions", "regular_pending", &chain.regular_pending),
                ("regular_success_transactions", "regular_success", &chain.regular_success),
                ("regular_failure_transactions", "regular_failure", &chain.regular_failure),
                ("locked_keys", "locked_keys", &chain.locked_keys),
                ("tx_per_block", "tx_per_block", &chain.tx_per_block),
                ("regular_tx_finalized_count", "regular_tx_finalized_count", &chain.regular_tx_finalized_count),
            ];
            for (prefix, metric, series) in counts {
                write_series(&format!("{}/data/{}_chain_{}.json", base_dir, prefix, i), &format!("chain_{}_{}", i, metric), "count", series)?;
            }
            write_series(&format!("{}/data/regular_tx_avg_latency_chain_{}.json", base_dir, i), &format!("chain_{}_regular_tx_avg_latency", i), "latency", &chain.regular_tx_avg_latency)?;
            write_series(&format!("{}/data/regular_tx_max_latency_chain_{}.json", base_dir, i), &format!("chain_{}_regular_tx_max_latency", i), "latency", &chain.regular_tx_max_latency)?;

            // Per-CAT finality records on this chain
            let records: Vec<serde_json::Value> = chain.cat_latency_records.iter().map(|r| serde_json::json!({
                "cat_id": r.cat_id.0.0,
                "start_block": r.start_block,
                "final_block": r.final_block,
                "latency_blocks": r.final_block.saturating_sub(r.start_block),
                "latency_ms": r.latency_ms,
                "success": r.success,
            })).collect();
            let cat_latency_file = format!("{}/data/cat_latency_chain_{}.json", base_dir, i);
            fs::write(&cat_latency_file, serde_json::to_string_pretty(&serde_json::json!({ format!("chain_{}_cat_latency", i): records })).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        }

        // Save account selection data to files
        let (sender_json, receiver_json) = self.account_stats.to_json();
        let sender_file = format!("{}/data/account_sender_selection.json", base_dir);
        fs::write(&sender_file, serde_json::to_string_pretty(&sender_json).expect("Failed to serialize sender stats")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved sender selection data to {}", sender_file));
        let receiver_file = format!("{}/data/account_receiver_selection.json", base_dir);
        fs::write(&receiver_file, serde_json::to_string_pretty(&receiver_json).expect("Failed to serialize receiver stats")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved receiver selection data to {}", receiver_file));

        // Save system memory usage data
        let system_memory_data = serde_json::json!({
            "system_memory": self.memory_usage.iter().map(|(height, bytes)| {
                serde_json::json!({
                    "height": height,
                    "bytes": bytes
                })
            }).collect::<Vec<_>>()
        });
        let system_memory_file = format!("{}/data/system_memory.json", base_dir);
        fs::write(&system_memory_file, serde_json::to_string_pretty(&system_memory_data).expect("Failed to serialize system memory")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved system memory data to {}", system_memory_file));

        // Save system total memory usage data
        let system_total_memory_data = serde_json::json!({
            "system_total_memory": self.total_memory.iter().map(|(height, bytes)| {
                serde_json::json!({
                    "height": height,
                    "bytes": bytes
                })
            }).collect::<Vec<_>>()
        });
        let system_total_memory_file = format!("{}/data/system_total_memory.json", base_dir);
        fs::write(&system_total_memory_file, serde_json::to_string_pretty(&system_total_memory_data).expect("Failed to serialize system total memory")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved system total memory data to {}", system_total_memory_file));

        // Save system CPU usage data
        let system_cpu_data = serde_json::json!({
            "system_cpu": self.cpu_usage.iter().map(|(height, percent)| {
                serde_json::json!({
                    "height": height,
                    "percent": percent
                })
            }).collect::<Vec<_>>()
        });
        let system_cpu_file = format!("{}/data/system_cpu.json", base_dir);
        fs::write(&system_cpu_file, serde_json::to_string_pretty(&system_cpu_data).expect("Failed to serialize system CPU")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved system CPU data to {}", system_cpu_file));

        // Save system total CPU usage data
        let system_total_cpu_data = serde_json::json!({
            "system_total_cpu": self.total_cpu_usage.iter().map(|(height, percent)| {
                serde_json::json!({
                    "height": height,
                    "percent": percent
                })
            }).collect::<Vec<_>>()
        });
        let system_total_cpu_file = format!("{}/data/system_total_cpu.json", base_dir);
        fs::write(&system_total_cpu_file, serde_json::to_string_pretty(&system_total_cpu_data).expect("Failed to serialize system total CPU")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved system total CPU data to {}", system_total_cpu_file));

        // Save loop steps without transaction issuance data
        let loop_steps_data = serde_json::json!({
            "loop_steps_without_tx_issuance": self.loop_steps_without_tx_issuance.iter().map(|(height, count)| {
                serde_json::json!({
                    "height": height,
                    "count": count
                })
            }).collect::<Vec<_>>()
        });
        let loop_steps_file = format!("{}/data/loop_steps_without_tx_issuance.json", base_dir);
        fs::write(&loop_steps_file, serde_json::to_string_pretty(&loop_steps_data).expect("Failed to serialize loop steps data")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved loop steps data to {}", loop_steps_file));

        // Save block height delta data
        let block_height_delta_data = serde_json::json!({
            "block_height_delta": self.block_height_delta.iter().map(|(height, delta)| {
                serde_json::json!({
                    "height": height,
                    "delta": delta
                })
            }).collect::<Vec<_>>()
        });
        let block_height_delta_file = format!("{}/data/block_height_delta.json", base_dir);
        fs::write(&block_height_delta_file, serde_json::to_string_pretty(&block_height_delta_data).expect("Failed to serialize block height delta data")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved block height delta data to {}", block_height_delta_file));

        // Save CL queue length data
        let cl_queue_length_data = serde_json::json!({
            "cl_queue_length": self.cl_queue_length.iter().map(|(height, count)| {
                serde_json::json!({
                    "height": height,
                    "count": count
                })
            }).collect::<Vec<_>>()
        });
        let cl_queue_length_file = format!("{}/data/cl_queue_length.json", base_dir);
        fs::write(&cl_queue_length_file, serde_json::to_string_pretty(&cl_queue_length_data).expect("Failed to serialize CL queue length data")).map_err(|e| e.to_string())?;
        logging::log("SIMULATOR", &format!("Saved CL queue length data to {}", cl_queue_length_file));

        Ok(())
    }
}
