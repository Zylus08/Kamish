mod doctor;
mod init;
mod policy;
mod start;
mod status;
mod topic;

use clap::{Parser, Subcommand};
use start::StartArgs;

#[derive(Parser, Debug)]
#[command(
    name = "kamish",
    about = "Unified sovereign event streaming CLI, broker daemon, and embedded dashboard (ADR-006)",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Launch the unified broker daemon, REST/SSE gateway, and embedded dashboard
    Start(StartArgs),

    /// Run the unified broker, gateway, and dashboard (alias for start, e.g. 'kamish run --dev')
    Run(StartArgs),

    /// Diagnose system readiness, port availability, consensus reachability, and configuration
    Doctor,

    /// Author cluster configuration skeleton, Ed25519 signing keys, and genesis policy
    Init {
        /// Force overwrite of existing configuration and keys
        #[arg(short, long, default_value_t = false)]
        force: bool,
    },

    /// Query cluster state, authority consensus model, active leases, and quorum status
    Status {
        /// Target gateway endpoint address
        #[arg(short, long, default_value = "127.0.0.1:3001")]
        addr: String,
    },

    /// Create and inspect sovereign topics and partition catalogs
    #[command(subcommand)]
    Topic(topic::TopicCommands),

    /// Cryptographically sign, verify, and publish Ed25519 residency policies
    #[command(subcommand)]
    Policy(policy::PolicyCommands),

    /// Produce a record with cryptographic residency policy enforcement
    Produce {
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(short, long, default_value = "telemetry")]
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partition: u32,
        #[arg(short, long, default_value = "general")]
        class_id: String,
        #[arg(short = 'v', long)]
        payload: String,
        #[arg(short, long, default_value = "EU_WEST")]
        region: String,
        #[arg(long, value_enum, default_value = "strict-local")]
        policy: kamish_ctl::PolicyChoice,
    },

    /// Read records sequentially from a partition offset up to high watermark
    Consume {
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(short, long, default_value = "telemetry")]
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partition: u32,
        #[arg(short, long, default_value = "general")]
        class_id: String,
        #[arg(short, long, default_value_t = 0)]
        offset: u64,
        #[arg(short, long, default_value_t = 10)]
        max_records: usize,
    },

    /// Alias for consume: fetch records sequentially from a partition
    Fetch {
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(short, long, default_value = "telemetry")]
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partition: u32,
        #[arg(short, long, default_value = "general")]
        class_id: String,
        #[arg(short, long, default_value_t = 0)]
        offset: u64,
        #[arg(short, long, default_value_t = 10)]
        max_records: usize,
    },

    /// Verify cryptographic integrity of SHA-256 hash-chained audit logs
    VerifyAudit {
        #[arg(short, long)]
        log_path: String,
        #[arg(short, long)]
        stream: Option<String>,
        #[arg(short, long)]
        pubkey: Option<String>,
    },

    /// Run the Developer B 3-Scenario verification suite (Residency, Slow-Link, Outage)
    Demo,

    /// Benchmark sustained produce throughput (msg/s) and p50/p99 latency
    Benchmark {
        #[arg(short, long, default_value_t = 1000)]
        count: usize,
        #[arg(short, long)]
        addr: Option<String>,
        #[arg(short = 'j', long, default_value_t = 1)]
        concurrency: usize,
    },

    /// Trigger regional partition failover promotion on a candidate broker
    Promote {
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(short, long, default_value = "telemetry")]
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partition: u32,
        #[arg(short, long)]
        class_id: Option<String>,
        #[arg(long)]
        min_watermark: Option<u64>,
    },

    /// Query the durable replication watermark from a remote broker
    Watermark {
        #[arg(short, long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(short, long, default_value = "telemetry")]
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partition: u32,
        #[arg(short, long, default_value = "EU_WEST")]
        source_region: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) | Commands::Run(args) => {
            start::run_start(args).await?;
        }
        Commands::Doctor => {
            doctor::run_doctor().await?;
        }
        Commands::Init { force } => {
            init::run_init(force).await?;
        }
        Commands::Status { addr } => {
            status::run_status(&addr).await?;
        }
        Commands::Topic(cmd) => {
            topic::run_topic(cmd).await?;
        }
        Commands::Policy(cmd) => {
            policy::run_policy(cmd).await?;
        }
        Commands::Produce {
            addr,
            topic,
            partition,
            class_id,
            payload,
            region,
            policy,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::Produce {
                addr,
                topic,
                partition,
                class_id,
                payload,
                region,
                policy,
            })
            .await?;
        }
        Commands::Consume {
            addr,
            topic,
            partition,
            class_id,
            offset,
            max_records,
        }
        | Commands::Fetch {
            addr,
            topic,
            partition,
            class_id,
            offset,
            max_records,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::Consume {
                addr,
                topic,
                partition,
                class_id,
                offset,
                max_records,
            })
            .await?;
        }
        Commands::VerifyAudit {
            log_path,
            stream,
            pubkey,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::VerifyAudit {
                log_path,
                stream,
                pubkey,
            })
            .await?;
        }
        Commands::Demo => {
            kamish_ctl::run_command(kamish_ctl::Commands::Demo).await?;
        }
        Commands::Benchmark {
            count,
            addr,
            concurrency,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::Benchmark {
                count,
                addr,
                concurrency,
            })
            .await?;
        }
        Commands::Promote {
            addr,
            topic,
            partition,
            class_id,
            min_watermark,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::Promote {
                addr,
                topic,
                partition,
                class_id,
                min_watermark,
            })
            .await?;
        }
        Commands::Watermark {
            addr,
            topic,
            partition,
            source_region,
        } => {
            kamish_ctl::run_command(kamish_ctl::Commands::Watermark {
                addr,
                topic,
                partition,
                source_region,
            })
            .await?;
        }
    }

    Ok(())
}

