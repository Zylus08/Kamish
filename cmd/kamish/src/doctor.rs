use std::net::TcpListener;
use std::path::Path;

pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub status_text: String,
    pub remediation: Option<String>,
}

pub async fn run_doctor() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("\x1b[1;35m==============================================================================\x1b[0m");
    println!("\x1b[1;36m  KAMISH SYSTEM PREREQUISITES & HEALTH DOCTOR (ADR-006)                       \x1b[0m");
    println!("\x1b[1;35m==============================================================================\x1b[0m\n");

    let mut checks = Vec::new();

    // 1. Check Gateway Port (3001)
    let gw_available = TcpListener::bind("127.0.0.1:3001").is_ok();
    checks.push(CheckResult {
        name: "Gateway HTTP Port (3001)".to_string(),
        passed: gw_available,
        status_text: if gw_available {
            "Available for binding".to_string()
        } else {
            "Port 3001 in use (may be an active Kamish gateway)".to_string()
        },
        remediation: if gw_available {
            None
        } else {
            Some("Check running processes with: netstat -ano | findstr :3001".to_string())
        },
    });

    // 2. Check Broker TCP Port (8080)
    let broker_available = TcpListener::bind("127.0.0.1:8080").is_ok();
    checks.push(CheckResult {
        name: "Broker TCP Port (8080)".to_string(),
        passed: broker_available,
        status_text: if broker_available {
            "Available for binding".to_string()
        } else {
            "Port 8080 in use (may be an active Kamish broker)".to_string()
        },
        remediation: if broker_available {
            None
        } else {
            Some("Check running processes with: netstat -ano | findstr :8080".to_string())
        },
    });

    // 3. Storage Directory & Write Permissions
    let data_dir = Path::new("data");
    let storage_ok = match std::fs::create_dir_all(data_dir) {
        Ok(_) => {
            let test_file = data_dir.join(".write_test");
            match std::fs::write(&test_file, b"kamish-doctor-test") {
                Ok(_) => {
                    let _ = std::fs::remove_file(test_file);
                    true
                }
                Err(_) => false,
            }
        }
        Err(_) => false,
    };
    checks.push(CheckResult {
        name: "Storage WAL Directory (./data)".to_string(),
        passed: storage_ok,
        status_text: if storage_ok {
            "Directory exists with write permissions".to_string()
        } else {
            "Cannot create or write to ./data directory".to_string()
        },
        remediation: if storage_ok {
            None
        } else {
            Some("Ensure write permissions on current directory".to_string())
        },
    });

    // 4. Web Dashboard Static Assets
    let dist_exists = Path::new("frontend/dist/index.html").exists()
        || Path::new("../frontend/dist/index.html").exists()
        || Path::new("dist/index.html").exists();
    checks.push(CheckResult {
        name: "Embedded Web Dashboard Assets".to_string(),
        passed: dist_exists,
        status_text: if dist_exists {
            "Compiled static assets found (frontend/dist/index.html)".to_string()
        } else {
            "frontend/dist/index.html not found".to_string()
        },
        remediation: if dist_exists {
            None
        } else {
            Some("Run 'cd frontend && npm run build' to generate static dashboard assets".to_string())
        },
    });

    // 5. Consensus Backend Connectivity (etcd probe)
    let etcd_available = tokio::net::TcpStream::connect("127.0.0.1:2379").await.is_ok();
    checks.push(CheckResult {
        name: "Consensus Authority Backend".to_string(),
        passed: true, // Non-fatal per ADR-006 §2.5 single-node mode
        status_text: if etcd_available {
            "etcd cluster reachable at 127.0.0.1:2379 (QUORUM_CONSENSUS)".to_string()
        } else {
            "AUTHORITY: LOCAL (in-memory & local fsync; quorum requires etcd)".to_string()
        },
        remediation: if etcd_available {
            None
        } else {
            Some("Single-node local mode is operational. For multi-region CAS quorum, launch etcd via 'scripts/start_etcd_cluster.sh'".to_string())
        },
    });

    // 6. Configuration File
    let config_exists = Path::new("kamish.toml").exists();
    checks.push(CheckResult {
        name: "Cluster Configuration (kamish.toml)".to_string(),
        passed: true,
        status_text: if config_exists {
            "Found custom configuration file 'kamish.toml'".to_string()
        } else {
            "Default parameters active (no kamish.toml found)".to_string()
        },
        remediation: if config_exists {
            None
        } else {
            Some("Run 'kamish init' to author a cluster configuration file and Ed25519 signing keys".to_string())
        },
    });

    // Print Report
    let mut all_passed = true;
    for check in &checks {
        let icon = if check.passed {
            "\x1b[1;32m[PASS]\x1b[0m"
        } else {
            all_passed = false;
            "\x1b[1;31m[FAIL]\x1b[0m"
        };
        println!("{:<7} {:<34} : {}", icon, check.name, check.status_text);
        if let Some(ref rem) = check.remediation {
            println!("        \x1b[1;33m↳ Note: {}\x1b[0m", rem);
        }
    }

    println!("\n\x1b[1;35m------------------------------------------------------------------------------\x1b[0m");
    if all_passed {
        println!("\x1b[1;32m✓ System is ready! You can launch Kamish with: kamish start --dev\x1b[0m\n");
    } else {
        println!("\x1b[1;31m✗ Some system checks failed. Review remediation notes above before launching.\x1b[0m\n");
    }

    Ok(())
}
