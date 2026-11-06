use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Copy, Debug)]
pub struct AuthOptions<'a> {
    pub token: Option<&'a str>,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub status: String,
    pub version: String,
    pub region: String,
    pub mode: String,
    pub known_peers: Vec<String>,
    pub degraded_links: Vec<String>,
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_connections: usize,
    pub timestamp_utc: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthorityStatus {
    pub authority_type: String,
    pub consensus_backing: String,
    pub quorum_backed: bool,
    pub active_region: String,
    pub monotonic_clock_ms: u64,
    pub active_leases_count: usize,
    pub total_quarantined_records: usize,
}

pub async fn run_status(gateway_url: &str, auth: AuthOptions<'_>, json_output: bool) -> i32 {
    let token = match get_or_login_token(gateway_url, auth).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Authentication error querying Kamish gateway at '{gateway_url}': {e}");
            eprintln!(
                "Ensure 'kamish start' is running or specify credentials with --username and --password."
            );
            return 1;
        }
    };

    match fetch_cluster_telemetry(gateway_url, &token).await {
        Ok((status, auth)) => {
            if json_output {
                let combined = serde_json::json!({
                    "cluster": status,
                    "authority": auth,
                });
                println!("{}", serde_json::to_string_pretty(&combined).unwrap());
            } else {
                render_status_table(&status, &auth);
            }
            0
        }
        Err(e) => {
            eprintln!("Error querying Kamish gateway at '{gateway_url}': {e}");
            1
        }
    }
}

pub async fn get_or_login_token(
    gateway_url: &str,
    auth: AuthOptions<'_>,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(t) = auth.token {
        return Ok(t.to_string());
    }

    let username = auth.username.unwrap_or("admin");
    let password = if let Some(p) = auth.password {
        p.to_string()
    } else if let Ok(p) = std::env::var("KAMISH_ADMIN_PASSWORD") {
        p
    } else {
        "kamish-admin-2026".to_string()
    };

    let base = gateway_url.trim_end_matches('/');
    let login_url = format!("{base}/api/auth/login");
    let payload = serde_json::json!({
        "username": username,
        "password": password,
    });

    let res = post_json_raw(&login_url, &payload).await?;
    let val: serde_json::Value = serde_json::from_slice(&res)?;
    if let Some(tok) = val
        .get("session_token")
        .or_else(|| val.get("token"))
        .and_then(|t| t.as_str())
    {
        Ok(tok.to_string())
    } else {
        Err(format!(
            "Login failed: {}",
            val.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("invalid credentials")
        )
        .into())
    }
}

async fn fetch_cluster_telemetry(
    gateway_url: &str,
    token: &str,
) -> Result<(ClusterStatus, AuthorityStatus), Box<dyn std::error::Error>> {
    let base = gateway_url.trim_end_matches('/');
    let url_status = format!("{base}/api/status");
    let url_auth = format!("{base}/api/authority");

    let status_json: ClusterStatus = fetch_authenticated_json(&url_status, token).await?;
    let auth_json: AuthorityStatus = fetch_authenticated_json(&url_auth, token).await?;

    Ok((status_json, auth_json))
}

pub async fn fetch_authenticated_json<T: for<'de> Deserialize<'de>>(
    url_str: &str,
    token: &str,
) -> Result<T, Box<dyn std::error::Error>> {
    let bytes = request_json(url_str, hyper::Method::GET, None, Some(token), None).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn post_authenticated_json(
    gateway_url: &str,
    endpoint: &str,
    body: &serde_json::Value,
    token: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let base = gateway_url.trim_end_matches('/');
    let me: serde_json::Value =
        fetch_authenticated_json(&format!("{base}/api/auth/me"), token).await?;
    let csrf = me["csrf_token"]
        .as_str()
        .ok_or("Gateway did not return a CSRF token")?;
    let bytes = request_json(
        &format!("{base}{endpoint}"),
        hyper::Method::POST,
        Some(body),
        Some(token),
        Some(csrf),
    )
    .await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub async fn post_json_raw(
    url_str: &str,
    body: &serde_json::Value,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    request_json(url_str, hyper::Method::POST, Some(body), None, None).await
}

async fn request_json(
    url_str: &str,
    method: hyper::Method,
    body: Option<&serde_json::Value>,
    token: Option<&str>,
    csrf: Option<&str>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let parsed: axum::http::Uri = url_str.parse()?;
    if parsed.scheme_str() != Some("http") {
        return Err("CLI gateway URL must use http://; HTTPS transport is not configured".into());
    }
    let host = parsed.host().ok_or("Gateway URL must include a host")?;
    let port = parsed.port_u16().unwrap_or(80);
    let path = parsed.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let stream = tokio::net::TcpStream::connect((host, port)).await?;
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = hyper::Request::builder()
        .method(method)
        .uri(path)
        .header(
            hyper::header::HOST,
            parsed.authority().ok_or("Missing gateway host")?.as_str(),
        )
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::USER_AGENT, "kamish-cli");
    if let Some(token) = token {
        req = req.header(hyper::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(csrf) = csrf {
        req = req.header("x-csrf-token", csrf);
    }
    let bytes = body
        .map(serde_json::to_vec)
        .transpose()?
        .unwrap_or_default();
    let res = sender
        .send_request(req.body(http_body_util::Full::new(hyper::body::Bytes::from(bytes)))?)
        .await?;
    let status = res.status();
    let bytes = http_body_util::BodyExt::collect(res.into_body())
        .await?
        .to_bytes()
        .to_vec();
    if !status.is_success() {
        return Err(format!(
            "Gateway returned {status}: {}",
            String::from_utf8_lossy(&bytes)
        )
        .into());
    }
    Ok(bytes)
}

fn render_status_table(status: &ClusterStatus, auth: &AuthorityStatus) {
    let use_color = std::env::var("NO_COLOR").is_err();
    let green = if use_color { "\x1b[1;32m" } else { "" };
    let cyan = if use_color { "\x1b[1;36m" } else { "" };
    let yellow = if use_color { "\x1b[1;33m" } else { "" };
    let reset = if use_color { "\x1b[0m" } else { "" };

    println!("{cyan}=== Kamish Cluster Status ==={reset}");
    println!("Status          : {green}{}{reset}", status.status);
    println!("Version         : {}", status.version);
    println!("Active Region   : {cyan}{}{reset}", status.region);
    println!("Operating Mode  : {}", status.mode);
    println!("Known Peers     : {:?}", status.known_peers);
    println!("Degraded Links  : {:?}", status.degraded_links);
    println!("Max Frame Bytes : {}", status.max_frame_bytes);
    println!("--------------------------------------------------");
    println!("{cyan}=== Sovereignty & Consensus Authority ==={reset}");
    println!("Authority Type  : {yellow}{}{reset}", auth.authority_type);
    println!("Consensus Model : {}", auth.consensus_backing);
    println!(
        "Quorum Backed   : {}",
        if auth.quorum_backed {
            format!("{green}true{reset}")
        } else {
            format!("{yellow}false (single-node local storage){reset}")
        }
    );
    println!("Monotonic Clock : {} ms", auth.monotonic_clock_ms);
    println!("Active Leases   : {}", auth.active_leases_count);
    println!("Quarantined     : {}", auth.total_quarantined_records);
    println!("Timestamp (UTC) : {}\n", status.timestamp_utc);
}
