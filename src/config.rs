use anyhow::{bail, Context, Result};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub stellar_rpc_url: String,
    pub mixer_contract_id: String,
    pub start_ledger: Option<u64>,
    pub db_path: PathBuf,
    pub poll_interval: Duration,
    pub batch_ledgers: u64,
    pub events_limit: u32,
    pub event_finality_lag: u64,
    pub catchup_sleep: Duration,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("TREEPIR_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:3000".to_string())
            .parse()
            .context("invalid TREEPIR_BIND_ADDR")?;

        let stellar_rpc_url = env::var("TREEPIR_STELLAR_RPC_URL")
            .unwrap_or_else(|_| "https://soroban-testnet.stellar.org".to_string());

        let mixer_contract_id =
            env::var("TREEPIR_MIXER_CONTRACT_ID").context("missing TREEPIR_MIXER_CONTRACT_ID")?;

        if mixer_contract_id.trim().is_empty() {
            bail!("TREEPIR_MIXER_CONTRACT_ID is empty");
        }

        let start_ledger = match env::var("TREEPIR_START_LEDGER") {
            Ok(value) if !value.trim().is_empty() => Some(
                value
                    .parse::<u64>()
                    .context("invalid TREEPIR_START_LEDGER")?,
            ),
            _ => None,
        };

        let db_path = env::var("TREEPIR_DB_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./treepir-server-state.rocksdb"));

        let poll_interval_ms = env::var("TREEPIR_POLL_INTERVAL_MS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("invalid TREEPIR_POLL_INTERVAL_MS")?
            .unwrap_or(2000);

        let batch_ledgers = env::var("TREEPIR_BATCH_LEDGERS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("invalid TREEPIR_BATCH_LEDGERS")?
            .unwrap_or(10_000)
            .clamp(1, 10_000);

        let events_limit = env::var("TREEPIR_EVENTS_LIMIT")
            .ok()
            .map(|value| value.parse::<u32>())
            .transpose()
            .context("invalid TREEPIR_EVENTS_LIMIT")?
            .unwrap_or(10_000)
            .clamp(1, 10_000);

        let event_finality_lag = env::var("TREEPIR_EVENT_FINALITY_LAG")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("invalid TREEPIR_EVENT_FINALITY_LAG")?
            .unwrap_or(8);

        let catchup_sleep_ms = env::var("TREEPIR_CATCHUP_SLEEP_MS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("invalid TREEPIR_CATCHUP_SLEEP_MS")?
            .unwrap_or(300);

        Ok(Self {
            bind_addr,
            stellar_rpc_url,
            mixer_contract_id,
            start_ledger,
            db_path,
            poll_interval: Duration::from_millis(poll_interval_ms),
            batch_ledgers,
            events_limit,
            event_finality_lag,
            catchup_sleep: Duration::from_millis(catchup_sleep_ms),
        })
    }
}
