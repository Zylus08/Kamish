use clap::Subcommand;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Subcommand, Debug, Clone)]
pub enum TopicCommands {
    /// List all registered topics and partition configuration
    List {
        #[arg(short, long, default_value = "127.0.0.1:3001")]
        addr: String,
    },
    /// Create a new topic with partition assignments and sovereign data classes
    Create {
        /// Name of the topic
        name: String,

        /// Number of partitions
        #[arg(short, long, default_value_t = 3)]
        partitions: u32,

        /// Owning tenant identifier
        #[arg(short, long, default_value = "tenant-default")]
        tenant: String,

        /// Data class (e.g. sovereign-class, general)
        #[arg(short, long, default_value = "general")]
        class_id: String,

        #[arg(short, long, default_value = "127.0.0.1:3001")]
        addr: String,
    },
}

async fn http_request(
    addr: &str,
    method: &str,
    path: &str,
    body_json: Option<serde_json::Value>,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let clean_addr = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let mut stream = TcpStream::connect(clean_addr).await?;

    let (body_str, content_len) = match body_json {
        Some(b) => {
            let s = serde_json::to_string(&b)?;
            let len = s.len();
            (s, len)
        }
        None => (String::new(), 0),
    };

    let req = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccept: application/json\r\n\r\n{}",
        method, path, clean_addr, content_len, body_str
    );
    stream.write_all(req.as_bytes()).await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;

    let resp_str = String::from_utf8_lossy(&response);
    if let Some(body_start) = resp_str.find("\r\n\r\n") {
        let body = &resp_str[body_start + 4..];
        let val: serde_json::Value = serde_json::from_str(body)?;
        Ok(val)
    } else {
        Err("Invalid HTTP response".into())
    }
}

pub async fn run_topic(cmd: TopicCommands) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        TopicCommands::List { addr } => {
            println!("\x1b[1;35m==============================================================================\x1b[0m");
            println!("\x1b[1;36m  KAMISH SOVEREIGN TOPIC DIRECTORY (ADR-006)                                  \x1b[0m");
            println!("\x1b[1;35m==============================================================================\x1b[0m\n");

            let res = http_request(&addr, "GET", "/api/topics", None).await?;
            if let Some(topics) = res["topics"].as_array() {
                println!("{:<28} {:<18} {:<12} {}", "TOPIC NAME", "TENANT", "PARTITIONS", "CLASSES");
                println!("{:-<28} {:-<18} {:-<12} {:-<20}", "", "", "", "");
                for t in topics {
                    let name = t["topic_name"].as_str().unwrap_or("-");
                    let tenant = t["tenant_id"].as_str().unwrap_or("-");
                    let parts = t["partition_count"].as_u64().unwrap_or(0);
                    let classes: Vec<&str> = t["allowed_classes"]
                        .as_array()
                        .map(|arr| arr.iter().filter_map(|c| c.as_str()).collect())
                        .unwrap_or_default();
                    println!("{:<28} {:<18} {:<12} {}", name, tenant, parts, classes.join(", "));
                }
                println!("\nTotal registered topics: {}", topics.len());
            } else {
                println!("No topics returned or unexpected response: {}", res);
            }
        }
        TopicCommands::Create {
            name,
            partitions,
            tenant,
            class_id,
            addr,
        } => {
            println!(
                "\x1b[1;34m[kamish topic]\x1b[0m Registering topic '{}' (Partitions: {}, Tenant: {}, Class: {})...",
                name, partitions, tenant, class_id
            );

            let body = serde_json::json!({
                "topic_name": name,
                "tenant_id": tenant,
                "partition_count": partitions,
                "allowed_classes": [class_id]
            });

            let res = http_request(&addr, "POST", "/api/topics", Some(body)).await?;
            if res["status"] == "ok" || res["status"] == "created" || res["topic"] == name {
                println!(
                    "\x1b[1;32m[kamish topic]\x1b[0m  Topic '{}' successfully created and active in catalog!",
                    name
                );
            } else {
                println!(
                    "\x1b[1;33m[kamish topic]\x1b[0m Response: {}",
                    serde_json::to_string_pretty(&res)?
                );
            }
        }
    }
    Ok(())
}

