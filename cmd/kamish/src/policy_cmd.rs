use ed25519_dalek::SigningKey;
use kamish_policy::{
    PolicyDocument, SignedPolicy, load_trusted_keys_from_file, load_verified_policy_from_file,
};
use std::path::Path;

pub fn sign_policy(policy_path: &str, key_path: &str, output_path: Option<&str>) -> i32 {
    let key_hex = match std::fs::read_to_string(key_path) {
        Ok(k) => k.trim().to_string(),
        Err(e) => {
            eprintln!("Error reading private key '{key_path}': {e}");
            return 1;
        }
    };

    let key_bytes = match hex::decode(&key_hex) {
        Ok(b) if b.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&b);
            arr
        }
        _ => {
            eprintln!("Error: Key in '{key_path}' is not a valid 32-byte hex-encoded seed.");
            return 1;
        }
    };

    let signing_key = SigningKey::from_bytes(&key_bytes);

    let doc_str = match std::fs::read_to_string(policy_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading policy document '{policy_path}': {e}");
            return 1;
        }
    };

    let doc: PolicyDocument = match serde_json::from_str(&doc_str) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error parsing policy document '{policy_path}': {e}");
            return 1;
        }
    };

    let signed = SignedPolicy::sign(doc, &signing_key);
    let output_json = match serde_json::to_string_pretty(&signed) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("Error serializing signed policy: {e}");
            return 1;
        }
    };

    if let Some(out) = output_path {
        if let Err(e) = std::fs::write(out, &output_json) {
            eprintln!("Error writing signed policy to '{out}': {e}");
            return 1;
        }
        println!("[kamish policy sign] Signed policy written to '{out}'");
    } else {
        println!("{output_json}");
    }

    0
}

pub fn verify_policy(policy_path: &str, trusted_keys_path: &str) -> i32 {
    let trusted_keys = match load_trusted_keys_from_file(Path::new(trusted_keys_path)) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error loading trusted keys from '{trusted_keys_path}': {e}");
            return 1;
        }
    };

    let now_sec = chrono::Utc::now().timestamp();
    match load_verified_policy_from_file(policy_path, &trusted_keys, 1, now_sec) {
        Ok(signed_policy) => {
            let use_color = std::env::var("NO_COLOR").is_err();
            let green = if use_color { "\x1b[1;32m" } else { "" };
            let reset = if use_color { "\x1b[0m" } else { "" };
            println!(
                "{green}[PASS]{reset} Policy '{}' (version {}) cryptographically verified by authority key '{}'",
                signed_policy.policy.policy_id,
                signed_policy.policy.version,
                signed_policy.policy.issuer_pubkey_hex
            );
            0
        }
        Err(e) => {
            let use_color = std::env::var("NO_COLOR").is_err();
            let red = if use_color { "\x1b[1;31m" } else { "" };
            let reset = if use_color { "\x1b[0m" } else { "" };
            eprintln!("{red}[FAIL]{reset} Cryptographic policy verification failed: {e}");
            1
        }
    }
}

pub async fn publish_policy(
    policy_path: &str,
    gateway_url: &str,
    auth: crate::status::AuthOptions<'_>,
) -> i32 {
    let result = async {
        let policy_str = std::fs::read_to_string(policy_path)?;
        let signed: SignedPolicy = serde_json::from_str(&policy_str)?;
        signed.verify_signature()?;
        let token = crate::status::get_or_login_token(gateway_url, auth).await?;
        crate::status::post_authenticated_json(
            gateway_url,
            "/api/policy/publish",
            &serde_json::to_value(signed)?,
            &token,
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    match result {
        Ok(()) => {
            println!("[kamish policy publish] Signed policy verified and activated by gateway.");
            0
        }
        Err(error) => {
            eprintln!("Policy publication failed: {error}");
            1
        }
    }
}
