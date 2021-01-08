use std::net::SocketAddr;

use cashweb::bitcoin::Network;
use clap::App;
use config::{Config, ConfigError, File};
use serde::Deserialize;

const FOLDER_DIR: &str = ".relay";
const DEFAULT_BIND: &str = "127.0.0.1:8080";
const DEFAULT_RPC_ADDR: &str = "http://127.0.0.1:18443";
const DEFAULT_RPC_USER: &str = "user";
const DEFAULT_RPC_PASSWORD: &str = "password";
const DEFAULT_NETWORK: &str = "regtest";
const DEFAULT_PING_INTERVAL: u64 = 10_000;
const DEFAULT_MESSAGE_LIMIT: usize = 1024 * 1024 * 20; // 20Mb
const DEFAULT_PROFILE_LIMIT: usize = 1024 * 512; // 512Kb
const DEFAULT_PAYMENT_LIMIT: usize = 1024 * 3; // 3Kb
const DEFAULT_PAYMENT_TIMEOUT: usize = 1_000 * 60; // 60 seconds
const DEFAULT_TRUNCATION_LENGTH: usize = 500;
const DEFAULT_TOKEN_FEE: u64 = 100_000;
const DEFAULT_MEMO: &str = "Thanks for your custom!";

#[cfg(feature = "monitoring")]
const DEFAULT_BIND_PROM: &str = "127.0.0.1:9095";

#[derive(Debug, Deserialize)]
pub struct BitcoinRpc {
    pub address: String,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Limits {
    pub message_size: u64,
    pub profile_size: u64,
    pub payment_size: u64,
}

#[derive(Debug, Deserialize)]
pub struct Payment {
    pub timeout: u64,
    pub token_fee: u64,
    pub memo: String,
    pub hmac_secret: String,
}

#[derive(Debug, Deserialize)]
pub struct Websocket {
    pub ping_interval: u64,
    pub truncation_length: u64,
}

#[derive(Debug, Deserialize)]
pub struct Peering {
    pub peers: Vec<String>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub bind: SocketAddr,
    #[cfg(feature = "monitoring")]
    pub bind_prom: SocketAddr,
    pub db_path: String,
    pub network: Network,
    pub bitcoin_rpc: BitcoinRpc,
    pub limits: Limits,
    pub payments: Payment,
    pub websocket: Websocket,
    pub peering: Peering
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        // Set defaults
        let yaml = load_yaml!("cli.yml");
        #[allow(deprecated)]
        let matches = App::from_yaml(yaml)
            .about(crate_description!())
            .author(crate_authors!("\n"))
            .version(crate_version!())
            .get_matches();
        let home_dir = match dirs::home_dir() {
            Some(some) => some,
            None => return Err(ConfigError::Message("no home directory".to_string())),
        };
        s.set_default("bind", DEFAULT_BIND)?;
        #[cfg(feature = "monitoring")]
        s.set_default("bind_prom", DEFAULT_BIND_PROM)?;
        s.set_default("network", DEFAULT_NETWORK)?;
        let mut default_db = home_dir.clone();
        default_db.push(format!("{}/db", FOLDER_DIR));
        s.set_default("db_path", default_db.to_str())?;
        s.set_default("bitcoin_rpc.address", DEFAULT_RPC_ADDR)?;
        s.set_default("bitcoin_rpc.username", DEFAULT_RPC_USER)?;
        s.set_default("bitcoin_rpc.password", DEFAULT_RPC_PASSWORD)?;
        s.set_default("limits.message_size", DEFAULT_MESSAGE_LIMIT as i64)?;
        s.set_default("limits.profile_size", DEFAULT_PROFILE_LIMIT as i64)?;
        s.set_default("limits.payment_size", DEFAULT_PAYMENT_LIMIT as i64)?;
        s.set_default("payments.token_fee", DEFAULT_TOKEN_FEE as i64)?;
        s.set_default("payments.memo", DEFAULT_MEMO)?;
        s.set_default("payments.timeout", DEFAULT_PAYMENT_TIMEOUT as i64)?;
        s.set_default(
            "websocket.truncation_length",
            DEFAULT_TRUNCATION_LENGTH as i64,
        )?;
        s.set_default("websocket.ping_interval", DEFAULT_PING_INTERVAL as i64)?;

        // NOTE: Don't set HMAC key to a default during release for security reasons
        #[cfg(debug_assertions)]
        {
            s.set_default("payments.hmac_secret", "1234")?;
        }

        // Load config from file
        let mut default_config = home_dir;
        default_config.push(format!("{}/config", FOLDER_DIR));
        let default_config_str = default_config.to_str().unwrap();
        let config_path = matches.value_of("config").unwrap_or(default_config_str);
        s.merge(File::with_name(config_path).required(false))?;

        // Set bind address from cmd line
        if let Some(bind) = matches.value_of("bind") {
            s.set("bind", bind)?;
        }

        // Set bind address from cmd line
        if let Some(bind_prom) = matches.value_of("bind-prom") {
            s.set("bind_prom", bind_prom)?;
        }

        // Set the bitcoin network
        if let Some(network) = matches.value_of("network") {
            s.set("network", network)?;
        }

        // Set db from cmd line
        if let Some(db_path) = matches.value_of("db-path") {
            s.set("db_path", db_path)?;
        }

        // Set node IP from cmd line
        if let Some(node_ip) = matches.value_of("rpc-addr") {
            s.set("bitcoin_rpc.address", node_ip)?;
        }

        // Set rpc username from cmd line
        if let Some(rpc_username) = matches.value_of("rpc-username") {
            s.set("bitcoin_rpc.username", rpc_username)?;
        }

        // Set rpc password from cmd line
        if let Some(rpc_password) = matches.value_of("rpc-password") {
            s.set("bitcoin_rpc.password", rpc_password)?;
        }

        // Set secret from cmd line
        if let Some(hmac_secret) = matches.value_of("hmac-secret") {
            s.set("payments.hmac_secret", hmac_secret)?;
        }

        s.try_into()
    }
}
