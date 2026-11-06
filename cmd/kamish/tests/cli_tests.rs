use axum::http::Uri;
use kamish::assets::{EmbeddedAssets, static_handler};

#[tokio::test]
async fn test_embedded_assets_inventory() {
    let count = EmbeddedAssets::iter().count();
    assert!(
        count >= 10,
        "Expected at least 10 embedded assets, found {count}"
    );
    assert!(
        EmbeddedAssets::get("index.html").is_some(),
        "index.html must be embedded"
    );
    assert!(
        EmbeddedAssets::get("favicon.svg").is_some(),
        "favicon.svg must be embedded"
    );
}

#[tokio::test]
async fn test_static_handler_index_and_caching() {
    let uri: Uri = "/".parse().unwrap();
    let res = static_handler(uri).await;
    assert_eq!(res.status(), axum::http::StatusCode::OK);

    let headers = res.headers();
    let content_type = headers.get("content-type").unwrap().to_str().unwrap();
    assert!(
        content_type.starts_with("text/html"),
        "content-type must start with text/html, got {content_type}"
    );
    assert_eq!(
        headers.get("cache-control").unwrap(),
        "no-cache, no-store, must-revalidate"
    );
    assert!(headers.get("content-security-policy").is_some());
    assert_eq!(headers.get("x-frame-options").unwrap(), "DENY");
    assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
}

#[tokio::test]
async fn test_static_handler_spa_routing_fallback() {
    // Non-file route should serve index.html with SPA fallback
    let uri: Uri = "/dashboard".parse().unwrap();
    let res = static_handler(uri).await;
    assert_eq!(res.status(), axum::http::StatusCode::OK);
    let content_type = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(content_type.starts_with("text/html"));

    // Missing file with extension should 404
    let uri_missing: Uri = "/assets/non-existent-chunk.js".parse().unwrap();
    let res_missing = static_handler(uri_missing).await;
    assert_eq!(res_missing.status(), axum::http::StatusCode::NOT_FOUND);
}

#[test]
fn test_doctor_diagnostic_run() {
    let code = kamish::doctor::run_doctor(None, true);
    // Doctor runs and returns 0 or 1 depending on whether etcd is online, but must never panic
    assert!(code == 0 || code == 1);
}

#[test]
fn test_init_and_idempotence_guard() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dir_str = temp_dir.path().to_str().unwrap();

    let opts = kamish::init::InitOptions {
        dir: dir_str,
        secret_file: None,
        admin_user: "test-admin",
        admin_password: Some("secret123"),
        region: "EU_WEST",
        force: false,
    };

    let code = kamish::init::run_init(opts);
    assert_eq!(code, 0, "First init must succeed");

    assert!(temp_dir.path().join("kamish.toml").exists());
    assert!(temp_dir.path().join("users.txt").exists());
    assert!(temp_dir.path().join("admin.key").exists());
    assert!(temp_dir.path().join("admin.pub").exists());
    assert!(temp_dir.path().join("policy.signed.json").exists());
    let config: toml::Value =
        toml::from_str(&std::fs::read_to_string(temp_dir.path().join("kamish.toml")).unwrap())
            .unwrap();
    assert_eq!(config["gateway"]["users_file"].as_str(), Some("users.txt"));
    assert_eq!(
        config["policy"]["trusted_keys_file"].as_str(),
        Some("admin.pub")
    );

    // Without force, second init must refuse to overwrite
    let opts_second = kamish::init::InitOptions {
        dir: dir_str,
        secret_file: None,
        admin_user: "test-admin",
        admin_password: Some("secret123"),
        region: "EU_WEST",
        force: false,
    };
    let code_second = kamish::init::run_init(opts_second);
    assert_eq!(code_second, 1, "Second init without --force must fail");

    // With force, second init must succeed
    let opts_force = kamish::init::InitOptions {
        dir: dir_str,
        secret_file: None,
        admin_user: "test-admin",
        admin_password: Some("secret123"),
        region: "EU_WEST",
        force: true,
    };
    let code_force = kamish::init::run_init(opts_force);
    assert_eq!(code_force, 0, "Init with --force must succeed");
}

#[test]
fn test_policy_sign_and_verify_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dir_str = temp_dir.path().to_str().unwrap();

    // Init to get key and pubkey
    let opts = kamish::init::InitOptions {
        dir: dir_str,
        secret_file: None,
        admin_user: "admin",
        admin_password: Some("pass"),
        region: "EU_WEST",
        force: true,
    };
    assert_eq!(kamish::init::run_init(opts), 0);

    let _key_file = temp_dir.path().join("admin.key");
    let pub_file = temp_dir.path().join("admin.pub");
    let policy_file = temp_dir.path().join("policy.signed.json");

    let verify_code = kamish::policy_cmd::verify_policy(
        policy_file.to_str().unwrap(),
        pub_file.to_str().unwrap(),
    );
    assert_eq!(verify_code, 0, "Policy verification must pass");
}

#[tokio::test]
async fn embedded_assets_reject_traversal_and_unknown_api_routes() {
    for path in [
        "/models/../../Cargo.toml",
        "/models/%2e%2e/policy.key",
        "/api/unknown",
    ] {
        assert_eq!(
            static_handler(path.parse().unwrap()).await.status(),
            axum::http::StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn cli_authenticates_topic_mutations_and_signed_policy_publication() {
    use kamish::status::{AuthOptions, fetch_authenticated_json, get_or_login_token};
    use std::{collections::HashSet, sync::Arc};
    let dir = tempfile::tempdir().unwrap();
    let audit = kamish_audit::AuditLog::open(dir.path().join("audit.log")).unwrap();
    let (cache, signing_key) = kamish_gateway::create_default_policy_cache(&["EU_WEST"]);
    let enforcer = Arc::new(kamish_enforce::Enforcer::new(
        "EU_WEST",
        cache.clone(),
        audit,
    ));
    let broker = Arc::new(
        kamish_broker::BrokerServer::try_new(
            "EU_WEST",
            enforcer,
            Arc::new(kamish_meta::MetadataStore::new()),
            Vec::new(),
            dir.path(),
        )
        .unwrap(),
    );
    let router = kamish_gateway::app(kamish_gateway::GatewayState::new(broker));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let auth = AuthOptions {
        token: None,
        username: Some("admin"),
        password: Some("kamish-admin-2026"),
    };
    let token = get_or_login_token(&url, auth).await.unwrap();
    // A bearer token and the independently fetched CSRF token must both be used.
    let token_auth = AuthOptions {
        token: Some(&token),
        ..Default::default()
    };
    assert_eq!(
        kamish::topic::create_topic(
            &url,
            "cli-regression",
            "tenant-default",
            2,
            &["general".to_string()],
            token_auth
        )
        .await,
        0
    );
    let topics: serde_json::Value = fetch_authenticated_json(&format!("{url}/api/topics"), &token)
        .await
        .unwrap();
    let topic = topics["topics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["topic"] == "cli-regression")
        .unwrap();
    assert_eq!(topic["partitions"], 2);
    assert_eq!(topic["owner_tenant"], "tenant-default");
    assert_eq!(kamish::status::run_status(&url, token_auth, true).await, 0);
    assert_eq!(kamish::topic::list_topics(&url, token_auth, true).await, 0);

    let mut document = cache.read().unwrap().current().unwrap().policy.clone();
    document.version += 1;
    document.classes.get_mut("general").unwrap().legal_regions =
        HashSet::from(["EU_WEST".to_string()]);
    let signed = kamish_policy::SignedPolicy::sign(document, &signing_key);
    let policy_file = dir.path().join("next-policy.json");
    std::fs::write(&policy_file, serde_json::to_vec(&signed).unwrap()).unwrap();
    assert_eq!(
        kamish::policy_cmd::publish_policy(policy_file.to_str().unwrap(), &url, token_auth).await,
        0
    );
    assert_eq!(cache.read().unwrap().current().unwrap().policy.version, 2);

    let (_, untrusted_key) = kamish_gateway::create_default_policy_cache(&["EU_WEST"]);
    let mut untrusted_doc = signed.policy.clone();
    untrusted_doc.version = 3;
    untrusted_doc.issuer_pubkey_hex = hex::encode(untrusted_key.verifying_key().to_bytes());
    let untrusted = kamish_policy::SignedPolicy::sign(untrusted_doc, &untrusted_key);
    std::fs::write(&policy_file, serde_json::to_vec(&untrusted).unwrap()).unwrap();
    assert_eq!(
        kamish::policy_cmd::publish_policy(policy_file.to_str().unwrap(), &url, token_auth).await,
        1
    );
    assert_eq!(cache.read().unwrap().current().unwrap().policy.version, 2);
    server.abort();
}

#[tokio::test]
async fn unified_start_serves_tcp_and_renews_leases_past_initial_expiry() {
    use kamish::status::{AuthOptions, fetch_authenticated_json, get_or_login_token};
    use kamish_broker::{BrokerClient, ProduceRequest};
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        kamish::init::run_init(kamish::init::InitOptions {
            dir: dir.path().to_str().unwrap(),
            secret_file: None,
            admin_user: "test-admin",
            admin_password: Some("test-password"),
            region: "EU_WEST",
            force: false,
        }),
        0
    );
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }
    let http_port = free_port();
    let tcp_port = free_port();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_kamish"))
        .args([
            "start",
            "--config",
            dir.path().join("kamish.toml").to_str().unwrap(),
            "--port",
            &http_port.to_string(),
            "--broker-port",
            &tcp_port.to_string(),
            "--data-dir",
            dir.path().join("data").to_str().unwrap(),
        ])
        .current_dir(dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let addr = format!("127.0.0.1:{tcp_port}");
    let mut client = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(client) = BrokerClient::connect(&addr).await {
                break client;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "Unified service exited before opening TCP"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let make_request = |sequence| ProduceRequest {
        topic: "general-topic".to_string(),
        partition: 0,
        producer_id: 98765,
        producer_epoch: 1,
        sequence_base: sequence,
        class_id: "general".to_string(),
        payload: b"lease-regression".to_vec(),
        residency_rule: None,
    };
    assert!(
        client
            .produce(make_request(0))
            .await
            .unwrap()
            .sovereignty_allowed
    );
    tokio::time::sleep(std::time::Duration::from_secs(31)).await;
    let result = client.produce(make_request(1)).await.unwrap();
    assert!(result.sovereignty_allowed);
    assert_eq!(result.base_offset, 1);
    let url = format!("http://127.0.0.1:{http_port}");
    let token = get_or_login_token(
        &url,
        AuthOptions {
            token: None,
            username: Some("test-admin"),
            password: Some("test-password"),
        },
    )
    .await
    .unwrap();
    let authority: serde_json::Value =
        fetch_authenticated_json(&format!("{url}/api/authority"), &token)
            .await
            .unwrap();
    assert!(
        authority["ownerships"]
            .as_array()
            .unwrap()
            .iter()
            .all(|owner| owner["active_lease"] == true)
    );
    child.kill().await.unwrap();
}
