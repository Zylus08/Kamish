use clap::Args;
use kamish_broker::catalog::{TenantCatalog, TopicMetadata};
use kamish_broker::BrokerServer;
use kamish_enforce::Enforcer;
use kamish_gateway::{app, GatewayState};
use kamish_meta::MetadataStore;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Args, Debug, Clone)]
pub struct StartArgs {
    /// Port for HTTP REST, SSE Gateway, and Embedded Dashboard
    #[arg(short, long, default_value_t = 3001)]
    pub port: u16,

    /// Region identifier for the broker
    #[arg(short, long, default_value = "EU_WEST")]
    pub region: String,

    /// Data directory for partition logs and persistent checkpoints
    #[arg(short, long, default_value = "data/gateway-broker")]
    pub data_dir: PathBuf,

    /// Known peer regions (comma-separated)
    #[arg(long, default_value = "US_EAST,AP_SOUTH")]
    pub peers: String,

    /// Host interface to bind to (defaults to loopback for security)
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Allow development mode credentials and pre-provision test topics
    #[arg(long, default_value_t = false)]
    pub dev: bool,

    /// Allow non-loopback binding without TLS (insecure dev mode)
    #[arg(long, default_value_t = false)]
    pub insecure_dev: bool,

    /// Allowed CORS origins for separate-origin deployments (comma-separated)
    #[arg(long, default_value = "")]
    pub allowed_origins: String,
}

pub async fn run_start(args: StartArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\x1b[1;35m==============================================================================\x1b[0m");
    println!("\x1b[1;36m  KAMISH ALL-IN-ONE UNIFIED BROKER, GATEWAY & DASHBOARD (ADR-006)             \x1b[0m");
    println!("\x1b[1;35m==============================================================================\x1b[0m\n");

    let peers: Vec<String> = args
        .peers
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    std::fs::create_dir_all(&args.data_dir)?;

    let audit_log = kamish_audit::AuditLog::open(args.data_dir.join("audit.log"))?;
    let (policy_cache, _signing_key) =
        kamish_gateway::create_default_policy_cache(&["EU_WEST", "US_EAST", "AP_SOUTH"]);
    let enforcer = Arc::new(Enforcer::new(&args.region, policy_cache, audit_log));
    let meta = Arc::new(MetadataStore::new());

    let mut catalog = TenantCatalog::new();
    let mut sovereign_classes = HashSet::new();
    sovereign_classes.insert("sovereign-class".to_string());
    catalog.register_topic(TopicMetadata::new(
        "financial-transactions",
        "tenant-sovereign",
        3,
        sovereign_classes.clone(),
    ));
    catalog.register_topic(TopicMetadata::new(
        "sovereign-stream",
        "tenant-sovereign",
        3,
        sovereign_classes,
    ));

    let mut general_classes = HashSet::new();
    general_classes.insert("general".to_string());
    catalog.register_topic(TopicMetadata::new(
        "general-topic",
        "tenant-default",
        3,
        general_classes,
    ));

    let broker = Arc::new(
        BrokerServer::try_new(args.region.clone(), enforcer, meta, peers, &args.data_dir)?
            .with_catalog(catalog),
    );

    // Register active initial partitions
    for p in 0..3 {
        let _ = broker.claim_partition_ownership("financial-transactions", p, 0, 1, 30_000);
        let _ = broker.claim_partition_ownership("sovereign-stream", p, 0, 1, 30_000);
        let _ = broker.claim_partition_ownership("general-topic", p, 0, 1, 30_000);
    }

    if args.host != "127.0.0.1"
        && args.host != "localhost"
        && args.host != "::1"
        && !args.insecure_dev
    {
        eprintln!(
            "Refusing to bind to non-loopback interface '{}' without TLS unless --insecure-dev is explicitly specified",
            args.host
        );
        std::process::exit(1);
    }

    let user_store = Arc::new(kamish_gateway::auth::UserStore::load_from_env_or_file(
        args.dev,
    )?);
    let origins_str = if args.allowed_origins.is_empty() {
        std::env::var("KAMISH_GATEWAY_ALLOWED_ORIGINS").unwrap_or_default()
    } else {
        args.allowed_origins.clone()
    };
    let allowed_origins: Vec<String> = origins_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let state =
        GatewayState::new_with_store(broker, user_store).with_allowed_origins(allowed_origins);
    let router = app(state);

    let bind_addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&bind_addr).await?;

    println!("\x1b[1;32m[✓]\x1b[0m Broker Daemon       : \x1b[1mActive\x1b[0m (Region: {}, Ingestion WAL: {:?})", args.region, args.data_dir);
    println!("\x1b[1;32m[✓]\x1b[0m REST & SSE Gateway  : \x1b[1mhttp://{}/api\x1b[0m", bind_addr);
    println!("\x1b[1;32m[✓]\x1b[0m Metrics Endpoint    : \x1b[1mhttp://{}/metrics\x1b[0m", bind_addr);
    println!("\x1b[1;32m[✓]\x1b[0m Embedded Dashboard  : \x1b[1;36mhttp://{}\x1b[0m", bind_addr);
    if args.dev {
        println!("\x1b[1;33m[i]\x1b[0m Development Mode    : \x1b[1mActive\x1b[0m (Demo accounts: admin, operator, auditor, producer, consumer)");
    }
    println!("\n\x1b[1mPress Ctrl+C to stop the unified service.\x1b[0m\n");

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for Ctrl+C");
            println!("\n\x1b[1;33m[kamish] Shutting down unified server gracefully...\x1b[0m");
        })
        .await?;

    Ok(())
}
