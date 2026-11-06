use clap::Subcommand;
use ed25519_dalek::SigningKey;
use kamish_policy::{PolicyDocument, SignedPolicy};
use std::fs;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Subcommand, Debug, Clone)]
pub enum PolicyCommands {
    /// Author and cryptographically sign a policy document with an Ed25519 private key
    Sign {
        /// Path to the JSON policy document
        #[arg(short, long)]
        doc: PathBuf,

        /// Path to the hex-encoded Ed25519 private key file
        #[arg(short, long)]
        key: PathBuf,

        /// Output path for the signed policy JSON
        #[arg(short, long, default_value = "signed_policy.json")]
        out: PathBuf,
    },

    /// Verify the Ed25519 cryptographic signature of a signed policy file
    Verify {
        /// Path to the signed policy JSON file
        #[arg(short, long)]
        file: PathBuf,

        /// Expected public key hex of the issuer
        #[arg(short, long)]
        pubkey: String,
    },

    /// Publish a verified signed policy to the running gateway
    Publish {
        /// Path to the signed policy JSON file
        #[arg(short, long)]
        file: PathBuf,

        #[arg(short, long, default_value = "127.0.0.1:3001")]
        addr: String,
    },
}

pub async fn run_policy(cmd: PolicyCommands) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match cmd {
        PolicyCommands::Sign { doc, key, out } => {
            let doc_str = fs::read_to_string(&doc)?;
            let policy_doc: PolicyDocument = serde_json::from_str(&doc_str)?;

            let key_str = fs::read_to_string(&key)?;
            let key_bytes = hex::decode(key_str.trim())?;
            let key_arr: [u8; 32] = key_bytes
                .try_into()
                .map_err(|_| "Private key must be exactly 32 bytes hex")?;
            let signing_key = SigningKey::from_bytes(&key_arr);

            let signed = SignedPolicy::sign(policy_doc, &signing_key);
            let out_json = serde_json::to_string_pretty(&signed)?;
            fs::write(&out, &out_json)?;

            println!("\x1b[1;32m[kamish policy]\x1b[0m Successfully signed policy '{}' (v{}) with Ed25519 key.", signed.policy.policy_id, signed.policy.version);
            println!("Saved signed policy to: \x1b[1m{:?}\x1b[0m", out);
        }

        PolicyCommands::Verify { file, pubkey } => {
            let file_str = fs::read_to_string(&file)?;
            let signed: SignedPolicy = serde_json::from_str(&file_str)?;

            if !pubkey.trim().is_empty() && !pubkey.trim().eq_ignore_ascii_case(&signed.policy.issuer_pubkey_hex) {
                eprintln!("\x1b[1;31m[✗] Issuer public key mismatch: declared '{}', expected '{}'\x1b[0m", signed.policy.issuer_pubkey_hex, pubkey);
                std::process::exit(1);
            }

            match signed.verify_signature() {
                Ok(_) => {
                    println!("\x1b[1;32m[✓] Cryptographic signature VALID!\x1b[0m");
                    println!("  Policy ID     : {}", signed.policy.policy_id);
                    println!("  Version       : {}", signed.policy.version);
                    println!("  Issuer Pubkey : {}", signed.policy.issuer_pubkey_hex);
                    println!("  Classes Count : {}", signed.policy.classes.len());
                }
                Err(e) => {
                    eprintln!("\x1b[1;31m[✗] Signature verification FAILED: {:?}\x1b[0m", e);
                    std::process::exit(1);
                }
            }
        }

        PolicyCommands::Publish { file, addr } => {
            let file_str = fs::read_to_string(&file)?;
            let signed: SignedPolicy = serde_json::from_str(&file_str)?;

            let clean_addr = addr
                .trim_start_matches("http://")
                .trim_start_matches("https://");

            println!(
                "\x1b[1;34m[kamish policy]\x1b[0m Publishing policy '{}' (v{}) to gateway at {}...",
                signed.policy.policy_id, signed.policy.version, clean_addr
            );

            let mut stream = TcpStream::connect(clean_addr).await?;
            let body_json = serde_json::to_string(&signed)?;
            let req = format!(
                "POST /api/policy/publish HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                clean_addr,
                body_json.len(),
                body_json
            );
            stream.write_all(req.as_bytes()).await?;

            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await?;
            let resp_str = String::from_utf8_lossy(&resp);

            if resp_str.contains("200 OK") {
                println!(
                    "\x1b[1;32m[kamish policy]\x1b[0m  Policy published and active on cluster!"
                );
            } else {
                eprintln!("\x1b[1;31m[kamish policy] Gateway returned error:\x1b[0m\n{}", resp_str);
            }
        }
    }
    Ok(())
}
