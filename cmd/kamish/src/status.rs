use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn http_get(addr: &str, path: &str) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect(addr).await?;
    let req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: application/json\r\n\r\n",
        path, addr
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

pub async fn run_status(target: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\x1b[1;35m==============================================================================\x1b[0m");
    println!("\x1b[1;36m  KAMISH CLUSTER & BROKER STATUS (ADR-006)                                    \x1b[0m");
    println!("\x1b[1;35m==============================================================================\x1b[0m\n");

    let clean_target = target
        .trim_start_matches("http://")
        .trim_start_matches("https://");

    println!("Querying gateway node at: \x1b[1m{}\x1b[0m...\n", clean_target);

    match http_get(clean_target, "/api/status").await {
        Ok(status) => {
            println!(">> GATEWAY & REGION STATE");
            println!("  Status               : \x1b[1;32m{}\x1b[0m", status["status"].as_str().unwrap_or("UNKNOWN"));
            println!("  Version              : {}", status["version"].as_str().unwrap_or("0.1.0"));
            println!("  Local Region         : \x1b[1;36m{}\x1b[0m", status["region"].as_str().unwrap_or("EU_WEST"));
            println!("  Operating Mode       : \x1b[1;33m{}\x1b[0m", status["mode"].as_str().unwrap_or("LIVE"));
            println!("  Active Epoch Time    : {}", status["timestamp_utc"].as_str().unwrap_or("N/A"));

            if let Some(peers) = status["known_peers"].as_array() {
                let peer_list: Vec<&str> = peers.iter().filter_map(|p| p.as_str()).collect();
                println!("  Known Peer Regions   : {}", peer_list.join(", "));
            }
        }
        Err(e) => {
            eprintln!("\x1b[1;31m[!] Could not connect to gateway at {}: {}\x1b[0m", clean_target, e);
            eprintln!("    Make sure the service is running via '\x1b[1mkamish start\x1b[0m'.");
            return Ok(());
        }
    }

    if let Ok(auth) = http_get(clean_target, "/api/authority").await {
        println!("\n>> CONSENSUS & LEASE AUTHORITY");
        println!("  Authority Model      : \x1b[1;32m{}\x1b[0m", auth["authority_type"].as_str().unwrap_or("N/A"));
        println!("  Consensus Backing    : {}", auth["consensus_backing"].as_str().unwrap_or("LOCAL_PERSISTENT_STORAGE"));
        println!("  Active Region        : {}", auth["active_region"].as_str().unwrap_or("N/A"));
        if let Some(leases) = auth["active_leases_count"].as_u64() {
            println!("  Active Leases Count  : {}", leases);
        }
    }

    if let Ok(quorum) = http_get(clean_target, "/api/quorum").await {
        println!("\n>> IN-REGION QUORUM CONSENSUS");
        println!("  Configured           : {}", quorum["configured"].as_bool().unwrap_or(false));
        println!("  Role                 : \x1b[1m{}\x1b[0m", quorum["role"].as_str().unwrap_or("LEADER"));
        println!("  Current Term         : {}", quorum["current_term"].as_u64().unwrap_or(0));
        println!("  Ack Level            : \x1b[1;36m{}\x1b[0m", quorum["ack_level"].as_str().unwrap_or("LeaderFsync"));
        println!("  Total Voters         : {}", quorum["total_voters"].as_u64().unwrap_or(1));
    }

    println!("\n\x1b[1;32m✓ Status query completed.\x1b[0m\n");
    Ok(())
}

