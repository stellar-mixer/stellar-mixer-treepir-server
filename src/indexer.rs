use crate::config::ServerConfig;
use crate::state_store::PersistentTreeStore;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use treepir::{Hash, TreePirServer};

use stellar_rpc_client::{
    Client as StellarRpcClient, Event as StellarEvent, EventStart, EventType,
};
use stellar_xdr::{
    ContractEvent, ContractEventBody, LedgerCloseMeta, Limits, ReadXdr, ScVal, TransactionMeta,
};

#[derive(Debug)]
pub struct StellarMixerIndexer<const DEPTH: usize> {
    config: ServerConfig,
    rpc: StellarEventClient,
    store: PersistentTreeStore,
    server: Arc<RwLock<TreePirServer<DEPTH>>>,
}

#[derive(Debug, Clone)]
struct StellarEventClient {
    client: StellarRpcClient,
    http: reqwest::Client,
    rpc_url: String,
    contract_id: String,
    contract_id_raw: [u8; 32],
    events_limit: usize,
    ledgers_limit: usize,
}

#[derive(Debug, Clone)]
struct LocalLedgerEvent {
    id: String,
    ledger: u32,
    order: u32,
    parsed: MixerTreeEvent,
}

#[derive(Debug, Clone)]
enum MixerTreeEvent {
    Deposit {
        index: u64,
        leaf: Hash,
        root: Hash,
    },
    WithdrawChange {
        index: u64,
        leaf: Hash,
        root: Hash,
    },
    Transfer {
        start_index: u64,
        output_leaves: Vec<Hash>,
        root: Hash,
    },
}

impl<const DEPTH: usize> StellarMixerIndexer<DEPTH> {
    pub fn new(
        config: ServerConfig,
        store: PersistentTreeStore,
        server: Arc<RwLock<TreePirServer<DEPTH>>>,
    ) -> Self {
        let contract_id_raw = config
            .mixer_contract_id
            .parse::<stellar_strkey::Contract>()
            .expect("invalid TREEPIR_MIXER_CONTRACT_ID strkey")
            .0;

        let rpc = StellarEventClient {
            client: StellarRpcClient::new(&config.stellar_rpc_url)
                .expect("invalid TREEPIR_STELLAR_RPC_URL"),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .pool_idle_timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build reqwest client"),
            rpc_url: config.stellar_rpc_url.clone(),
            contract_id: config.mixer_contract_id.clone(),
            contract_id_raw,
            events_limit: config.events_limit as usize,
            ledgers_limit: (config.events_limit as usize).clamp(1, 200),
        };

        Self {
            config,
            rpc,
            store,
            server,
        }
    }

    pub async fn catch_up_once(&mut self) -> Result<()> {
        let latest = self.rpc.latest_ledger().await?;
        let target = latest.saturating_sub(self.config.event_finality_lag);

        self.catch_up_to(target).await
    }

    pub async fn run_forever(mut self) -> Result<()> {
        let mut consecutive_transient_failures = 0u32;

        loop {
            match self.catch_up_live_once().await {
                Ok(()) => {
                    consecutive_transient_failures = 0;
                    sleep(self.config.poll_interval).await;
                }
                Err(error) if is_probably_transient_rpc_error(&error) => {
                    consecutive_transient_failures =
                        consecutive_transient_failures.saturating_add(1);

                    let backoff = transient_rpc_backoff(consecutive_transient_failures);

                    warn!(
                        %error,
                        consecutive_transient_failures,
                        backoff_ms = backoff.as_millis(),
                        last_indexed_ledger = self.store.last_indexed_ledger(),
                        leaf_count = self.store.leaf_count(),
                        "transient Stellar RPC error; retrying live indexer"
                    );

                    sleep(backoff).await;
                }
                Err(error) => {
                    return Err(error);
                }
            }
        }
    }

    async fn catch_up_live_once(&mut self) -> Result<()> {
        let latest = self.rpc.latest_ledger().await?;
        self.catch_up_live_ledgers_to(latest).await
    }

    async fn catch_up_to(&mut self, latest_ledger: u64) -> Result<()> {
        let mut from = self.store.last_indexed_ledger().saturating_add(1);

        if from > latest_ledger {
            return Ok(());
        }

        while from <= latest_ledger {
            let end_exclusive = from
                .saturating_add(self.config.batch_ledgers)
                .min(latest_ledger.saturating_add(1));

            let events = self.rpc.events_for_range(from, end_exclusive).await?;

            if !events.is_empty() {
                info!(
                    from,
                    end_exclusive,
                    events = events.len(),
                    "startup fetched Stellar mixer events through getEvents"
                );
            }

            for event in events {
                self.apply_rpc_event(event).await?;
            }

            self.store
                .set_last_indexed_ledger(end_exclusive.saturating_sub(1));
            self.store.save()?;

            info!(
                from,
                end_exclusive,
                last_indexed_ledger = self.store.last_indexed_ledger(),
                leaf_count = self.store.leaf_count(),
                "startup indexed Stellar mixer ledger range"
            );

            from = end_exclusive;

            if from <= latest_ledger && self.config.catchup_sleep.as_millis() > 0 {
                sleep(self.config.catchup_sleep).await;
            }
        }

        Ok(())
    }

    async fn catch_up_live_ledgers_to(&mut self, latest_ledger: u64) -> Result<()> {
        let mut from = self.store.last_indexed_ledger().saturating_add(1);

        if from > latest_ledger {
            return Ok(());
        }

        while from <= latest_ledger {
            let end_exclusive = from
                .saturating_add(self.config.batch_ledgers)
                .min(latest_ledger.saturating_add(1));

            let events = self
                .rpc
                .events_from_ledger_close_meta_range(from, end_exclusive)
                .await?;

            if !events.is_empty() {
                info!(
                    from,
                    end_exclusive,
                    events = events.len(),
                    "live fetched Stellar mixer events from LedgerCloseMeta"
                );
            }

            for event in events {
                self.apply_local_ledger_event(event).await?;
            }

            self.store
                .set_last_indexed_ledger(end_exclusive.saturating_sub(1));
            self.store.save()?;

            info!(
                from,
                end_exclusive,
                last_indexed_ledger = self.store.last_indexed_ledger(),
                leaf_count = self.store.leaf_count(),
                "live indexed Stellar mixer ledger range from LedgerCloseMeta"
            );

            from = end_exclusive;

            if from <= latest_ledger && self.config.catchup_sleep.as_millis() > 0 {
                sleep(self.config.catchup_sleep).await;
            }
        }

        Ok(())
    }

    async fn apply_rpc_event(&mut self, event: StellarEvent) -> Result<()> {
        if event.contract_id != self.config.mixer_contract_id {
            return Ok(());
        }

        if self.store.has_event_id(&event.id) {
            return Ok(());
        }

        let Some(parsed) = parse_mixer_tree_event(&event)? else {
            return Ok(());
        };

        self.apply_parsed_tree_event(&event.id, u64::from(event.ledger), parsed)
            .await
    }

    async fn apply_local_ledger_event(&mut self, event: LocalLedgerEvent) -> Result<()> {
        if self.store.has_event_id(&event.id) {
            return Ok(());
        }

        self.apply_parsed_tree_event(&event.id, u64::from(event.ledger), event.parsed)
            .await
    }

    async fn apply_parsed_tree_event(
        &mut self,
        event_id: &str,
        event_ledger: u64,
        parsed: MixerTreeEvent,
    ) -> Result<()> {
        let server_arc = Arc::clone(&self.server);
        let mut server = server_arc.write().await;

        match parsed {
            MixerTreeEvent::Deposit { index, leaf, root } => {
                self.apply_one_leaf(
                    &mut server,
                    index,
                    leaf,
                    root,
                    event_id,
                    event_ledger,
                    "deposit",
                )?;
            }
            MixerTreeEvent::WithdrawChange { index, leaf, root } => {
                self.apply_one_leaf(
                    &mut server,
                    index,
                    leaf,
                    root,
                    event_id,
                    event_ledger,
                    "withdraw",
                )?;
            }
            MixerTreeEvent::Transfer {
                start_index,
                output_leaves,
                root,
            } => {
                let first_transfer_leaf_event_id = format!("{event_id}#0");
                if self.store.has_event_id(&first_transfer_leaf_event_id) {
                    return Ok(());
                }

                if server.leaf_count() as u64 != start_index {
                    bail!(
                        "transfer index mismatch at event {}: local next={}, event start_index={}",
                        event_id,
                        server.leaf_count(),
                        start_index
                    );
                }

                let mut appended = Vec::with_capacity(output_leaves.len());

                for (offset, leaf) in output_leaves.iter().copied().enumerate() {
                    let expected_index = start_index + offset as u64;
                    let got_index = server.append_leaf(leaf)? as u64;

                    if got_index != expected_index {
                        bail!(
                            "tree append index mismatch at event {}: expected {}, got {}",
                            event_id,
                            expected_index,
                            got_index
                        );
                    }

                    appended.push((got_index, leaf));
                }

                if server.root() != root {
                    bail!(
                        "transfer root mismatch at event {}: local={}, event={}",
                        event_id,
                        hex::encode(server.root()),
                        hex::encode(root)
                    );
                }

                for (offset, (got_index, leaf)) in appended.into_iter().enumerate() {
                    let event_leaf_id = format!("{event_id}#{offset}");

                    self.store.append_leaf_record(
                        got_index,
                        leaf,
                        &event_leaf_id,
                        event_ledger,
                        "transfer",
                    )?;
                }
            }
        }

        self.store.save()?;

        Ok(())
    }

    fn apply_one_leaf(
        &mut self,
        server: &mut TreePirServer<DEPTH>,
        index: u64,
        leaf: Hash,
        root: Hash,
        event_id: &str,
        event_ledger: u64,
        source: &str,
    ) -> Result<()> {
        if server.leaf_count() as u64 != index {
            bail!(
                "{source} index mismatch at event {}: local next={}, event index={}",
                event_id,
                server.leaf_count(),
                index
            );
        }

        let got_index = server.append_leaf(leaf)? as u64;

        if got_index != index {
            bail!(
                "tree append index mismatch at event {}: expected {}, got {}",
                event_id,
                index,
                got_index
            );
        }

        if server.root() != root {
            bail!(
                "{source} root mismatch at event {}: local={}, event={}",
                event_id,
                hex::encode(server.root()),
                hex::encode(root)
            );
        }

        self.store
            .append_leaf_record(index, leaf, event_id, event_ledger, source)?;

        Ok(())
    }
}

impl StellarEventClient {
    async fn latest_ledger(&self) -> Result<u64> {
        let latest = self.client.get_latest_ledger().await?;
        Ok(u64::from(latest.sequence))
    }

    async fn events_for_range(
        &self,
        start_ledger: u64,
        end_ledger: u64,
    ) -> Result<Vec<StellarEvent>> {
        if start_ledger >= end_ledger {
            return Ok(Vec::new());
        }

        let start = u32::try_from(start_ledger).context("start ledger does not fit u32")?;
        let end_inclusive =
            u32::try_from(end_ledger.saturating_sub(1)).context("end ledger does not fit u32")?;

        let mut out = Vec::new();
        let mut start_at = EventStart::ledger_range(start, end_inclusive)
            .map_err(|error| anyhow::anyhow!("invalid Stellar event ledger range: {error}"))?;

        loop {
            let page = self
                .client
                .get_events(
                    start_at.clone(),
                    Some(EventType::Contract),
                    &[self.contract_id.clone()],
                    &[],
                    Some(self.events_limit),
                )
                .await
                .with_context(|| {
                    format!("getEvents failed for ledger range [{start_ledger}, {end_ledger})")
                })?;

            if u64::from(page.latest_ledger) < start_ledger {
                warn!(
                    latest_ledger = page.latest_ledger,
                    start_ledger, "RPC latest ledger is behind requested range"
                );
            }

            let page_count = page.events.len();
            let reached_end = page
                .events
                .iter()
                .any(|event| u64::from(event.ledger) >= end_ledger);

            out.extend(
                page.events
                    .into_iter()
                    .filter(|event| u64::from(event.ledger) < end_ledger),
            );

            if reached_end || page_count < self.events_limit || page.cursor.is_empty() {
                break;
            }

            start_at = EventStart::Cursor(page.cursor);
        }

        out.sort_by(|a, b| (a.ledger, a.id.as_str()).cmp(&(b.ledger, b.id.as_str())));

        Ok(out)
    }

    async fn events_from_ledger_close_meta_range(
        &self,
        start_ledger: u64,
        end_ledger: u64,
    ) -> Result<Vec<LocalLedgerEvent>> {
        if start_ledger >= end_ledger {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let params = if let Some(cursor) = cursor.as_ref() {
                serde_json::json!({
                    "pagination": {
                        "cursor": cursor,
                        "limit": self.ledgers_limit
                    },
                    "xdrFormat": "base64"
                })
            } else {
                serde_json::json!({
                    "startLedger": start_ledger,
                    "pagination": {
                        "limit": self.ledgers_limit
                    },
                    "xdrFormat": "base64"
                })
            };

            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "treepir-get-ledgers",
                "method": "getLedgers",
                "params": params
            });

            let response = self
                .http
                .post(&self.rpc_url)
                .json(&request)
                .send()
                .await
                .with_context(|| {
                    format!(
                        "getLedgers request failed for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?
                .error_for_status()
                .with_context(|| {
                    format!(
                        "getLedgers HTTP error for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?
                .json::<JsonRpcResponse<GetLedgersResult>>()
                .await
                .with_context(|| {
                    format!(
                        "getLedgers response decode failed for ledger range [{start_ledger}, {end_ledger})"
                    )
                })?;

            if let Some(error) = response.error {
                bail!(
                    "getLedgers RPC error for ledger range [{}, {}): code={}, message={}",
                    start_ledger,
                    end_ledger,
                    error.code,
                    error.message
                );
            }

            let result = response
                .result
                .context("getLedgers response missing result")?;

            if result.latest_ledger < start_ledger {
                warn!(
                    latest_ledger = result.latest_ledger,
                    start_ledger, "RPC latest ledger is behind requested ledger range"
                );
            }

            let page_count = result.ledgers.len();
            let mut reached_end = false;

            for ledger in result.ledgers {
                let ledger_seq = u64::from(ledger.sequence);

                if ledger_seq >= end_ledger {
                    reached_end = true;
                    continue;
                }

                if ledger_seq < start_ledger {
                    continue;
                }

                let meta = LedgerCloseMeta::from_xdr_base64(&ledger.metadata_xdr, Limits::none())
                    .with_context(|| {
                    format!(
                        "failed to decode LedgerCloseMeta for ledger {}",
                        ledger.sequence
                    )
                })?;

                let before = out.len();

                collect_mixer_events_from_ledger_close_meta(
                    ledger.sequence,
                    &meta,
                    &self.contract_id_raw,
                    &mut out,
                )
                .with_context(|| {
                    format!(
                        "failed to extract mixer events from LedgerCloseMeta for ledger {}",
                        ledger.sequence
                    )
                })?;

                let found = out.len().saturating_sub(before);
                if found > 0 {
                    info!(
                        ledger = ledger.sequence,
                        events = found,
                        "extracted mixer events from typed LedgerCloseMeta"
                    );
                }
            }

            let next_cursor = result.cursor.unwrap_or_default();

            if reached_end || page_count < self.ledgers_limit || next_cursor.is_empty() {
                break;
            }

            cursor = Some(next_cursor);
        }

        out.sort_by(|a, b| (a.ledger, a.id.as_str()).cmp(&(b.ledger, b.id.as_str())));

        Ok(out)
    }
}

fn is_probably_transient_rpc_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();

    message.contains("sendrequest")
        || message.contains("error sending request")
        || message.contains("request failed")
        || message.contains("connection")
        || message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("unexpected eof")
        || message.contains("broken pipe")
        || message.contains("timeout")
        || message.contains("timed out")
        || message.contains("dns")
        || message.contains("tls")
        || message.contains("hyper")
        || message.contains("http error")
        || message.contains("429")
        || message.contains("502")
        || message.contains("503")
        || message.contains("504")
}

fn transient_rpc_backoff(failures: u32) -> Duration {
    match failures {
        0 | 1 => Duration::from_secs(2),
        2 => Duration::from_secs(4),
        3 => Duration::from_secs(8),
        4 => Duration::from_secs(16),
        _ => Duration::from_secs(30),
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetLedgersResult {
    ledgers: Vec<RpcLedger>,
    latest_ledger: u64,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLedger {
    sequence: u32,
    metadata_xdr: String,
}

fn collect_mixer_events_from_ledger_close_meta(
    ledger: u32,
    meta: &LedgerCloseMeta,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerEvent>,
) -> Result<()> {
    match meta {
        LedgerCloseMeta::V0(v0) => {
            for (tx_index, tx_meta) in v0.tx_processing.iter().enumerate() {
                collect_mixer_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
        LedgerCloseMeta::V1(v1) => {
            for (tx_index, tx_meta) in v1.tx_processing.iter().enumerate() {
                collect_mixer_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
        LedgerCloseMeta::V2(v2) => {
            for (tx_index, tx_meta) in v2.tx_processing.iter().enumerate() {
                collect_mixer_events_from_transaction_meta(
                    ledger,
                    tx_index,
                    &tx_meta.tx_apply_processing,
                    mixer_contract_id,
                    out,
                )?;
            }
        }
    }

    Ok(())
}

fn collect_mixer_events_from_transaction_meta(
    ledger: u32,
    tx_index: usize,
    tx_meta: &TransactionMeta,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerEvent>,
) -> Result<()> {
    match tx_meta {
        TransactionMeta::V3(v3) => {
            if let Some(soroban_meta) = v3.soroban_meta.as_ref() {
                for (event_index, event) in soroban_meta.events.iter().enumerate() {
                    maybe_push_contract_event(
                        ledger,
                        format!("ledger-meta:{ledger}:tx:{tx_index}:soroban:{event_index}"),
                        event,
                        mixer_contract_id,
                        out,
                    )?;
                }
            }
        }
        TransactionMeta::V4(v4) => {
            for (op_index, op_meta) in v4.operations.iter().enumerate() {
                for (event_index, event) in op_meta.events.iter().enumerate() {
                    maybe_push_contract_event(
                        ledger,
                        format!(
                            "ledger-meta:{ledger}:tx:{tx_index}:op:{op_index}:event:{event_index}"
                        ),
                        event,
                        mixer_contract_id,
                        out,
                    )?;
                }
            }
        }
        TransactionMeta::V0(_) | TransactionMeta::V1(_) | TransactionMeta::V2(_) => {}
    }

    Ok(())
}

fn maybe_push_contract_event(
    ledger: u32,
    event_id: String,
    event: &ContractEvent,
    mixer_contract_id: &[u8; 32],
    out: &mut Vec<LocalLedgerEvent>,
) -> Result<()> {
    if !contract_event_matches_contract(event, mixer_contract_id) {
        return Ok(());
    }

    let Some(parsed) = parse_contract_event_fast(event)? else {
        return Ok(());
    };

    let order = out.len() as u32;

    out.push(LocalLedgerEvent {
        id: event_id,
        ledger,
        order,
        parsed,
    });

    Ok(())
}

fn contract_event_matches_contract(event: &ContractEvent, mixer_contract_id: &[u8; 32]) -> bool {
    let Some(contract_id) = event.contract_id.as_ref() else {
        return false;
    };

    let hash: &stellar_xdr::Hash = contract_id.as_ref();
    hash.0 == *mixer_contract_id
}

fn parse_contract_event_fast(event: &ContractEvent) -> Result<Option<MixerTreeEvent>> {
    let ContractEventBody::V0(v0) = &event.body;

    if !topics_contain_symbol(&v0.topics, "mixer") {
        return Ok(None);
    }

    let kind = if topics_contain_symbol(&v0.topics, "deposit") {
        "deposit"
    } else if topics_contain_symbol(&v0.topics, "withdraw") {
        "withdraw"
    } else if topics_contain_symbol(&v0.topics, "transfer") {
        "transfer"
    } else {
        return Ok(None);
    };

    match kind {
        "deposit" => {
            let index = named_scval_u64(&v0.data, &["index"])?;
            let leaf = named_scval_hash(&v0.data, &["leaf"])?;
            let root = named_scval_hash(&v0.data, &["root"])?;

            Ok(Some(MixerTreeEvent::Deposit { index, leaf, root }))
        }
        "withdraw" => {
            let index = named_scval_u64(&v0.data, &["index", "change_index"])?;
            let leaf = named_scval_hash(&v0.data, &["output_leaf", "change_leaf", "leaf"])?;
            let root = named_scval_hash(&v0.data, &["root"])?;

            Ok(Some(MixerTreeEvent::WithdrawChange { index, leaf, root }))
        }
        "transfer" => {
            let start_index = named_scval_u64(&v0.data, &["start_index"])?;
            let output_leaves = named_scval_hash_vec(&v0.data, &["output_leaves"])?;
            let root = named_scval_hash(&v0.data, &["root"])?;

            Ok(Some(MixerTreeEvent::Transfer {
                start_index,
                output_leaves,
                root,
            }))
        }
        _ => Ok(None),
    }
}

fn topics_contain_symbol(topics: &[ScVal], expected: &str) -> bool {
    topics.iter().any(|topic| scval_symbol_eq(topic, expected))
}

fn scval_symbol_eq(value: &ScVal, expected: &str) -> bool {
    match value {
        ScVal::Symbol(symbol) => symbol.as_slice() == expected.as_bytes(),
        ScVal::String(string) => string.as_slice() == expected.as_bytes(),
        _ => false,
    }
}

fn named_scval_u64(value: &ScVal, names: &[&str]) -> Result<u64> {
    let found =
        find_named_scval(value, names).with_context(|| format!("missing u64 field {names:?}"))?;

    scval_to_u64(found).with_context(|| format!("invalid u64 field {names:?}"))
}

fn named_scval_hash(value: &ScVal, names: &[&str]) -> Result<Hash> {
    let found =
        find_named_scval(value, names).with_context(|| format!("missing hash field {names:?}"))?;

    scval_to_hash(found).with_context(|| format!("invalid hash field {names:?}"))
}

fn named_scval_hash_vec(value: &ScVal, names: &[&str]) -> Result<Vec<Hash>> {
    let found = find_named_scval(value, names)
        .with_context(|| format!("missing hash vec field {names:?}"))?;

    scval_to_hash_vec(found).with_context(|| format!("invalid hash vec field {names:?}"))
}

fn find_named_scval<'a>(value: &'a ScVal, names: &[&str]) -> Option<&'a ScVal> {
    match value {
        ScVal::Map(Some(map)) => {
            for entry in map.iter() {
                if scval_key_matches(&entry.key, names) {
                    return Some(&entry.val);
                }
            }

            for entry in map.iter() {
                if let Some(found) = find_named_scval(&entry.val, names) {
                    return Some(found);
                }
            }

            None
        }
        ScVal::Vec(Some(vec)) => {
            for item in vec.iter() {
                if let Some(found) = find_named_scval(item, names) {
                    return Some(found);
                }
            }

            None
        }
        _ => None,
    }
}

fn scval_key_matches(value: &ScVal, names: &[&str]) -> bool {
    names.iter().any(|name| scval_symbol_eq(value, name))
}

fn scval_to_u64(value: &ScVal) -> Result<u64> {
    match value {
        ScVal::U64(n) => Ok(*n),
        ScVal::U32(n) => Ok(u64::from(*n)),
        ScVal::I64(n) if *n >= 0 => Ok(*n as u64),
        ScVal::I32(n) if *n >= 0 => Ok(*n as u64),
        _ => bail!("not a u64-compatible ScVal"),
    }
}

fn scval_to_hash(value: &ScVal) -> Result<Hash> {
    let ScVal::Bytes(bytes) = value else {
        bail!("not a bytes ScVal");
    };

    if bytes.len() != 32 {
        bail!("expected 32-byte hash, got {} bytes", bytes.len());
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(bytes.as_slice());

    Ok(out)
}

fn scval_to_hash_vec(value: &ScVal) -> Result<Vec<Hash>> {
    match value {
        ScVal::Vec(Some(vec)) => vec.iter().map(scval_to_hash).collect(),
        ScVal::Vec(None) => Ok(Vec::new()),
        _ => bail!("not a vec ScVal"),
    }
}

fn parse_mixer_tree_event(event: &StellarEvent) -> Result<Option<MixerTreeEvent>> {
    let symbols = topic_symbols(&event.topic)?;
    let value = scval_json_from_base64(&event.value)
        .with_context(|| format!("failed to decode event value XDR for {}", event.id))?;

    parse_mixer_tree_event_from_xdr_json(&symbols, &value)
}

fn parse_mixer_tree_event_from_xdr_json(
    symbols: &[String],
    value: &Value,
) -> Result<Option<MixerTreeEvent>> {
    if symbols.iter().any(|symbol| symbol == "deposit") {
        let index = named_u64(value, &["index"])?;
        let leaf = named_hash(value, &["leaf"])?;
        let root = named_hash(value, &["root"])?;

        return Ok(Some(MixerTreeEvent::Deposit { index, leaf, root }));
    }

    if symbols.iter().any(|symbol| symbol == "withdraw") {
        let index = named_u64(value, &["index", "change_index"])?;
        let leaf = named_hash(value, &["output_leaf", "change_leaf", "leaf"])?;
        let root = named_hash(value, &["root"])?;

        return Ok(Some(MixerTreeEvent::WithdrawChange { index, leaf, root }));
    }

    if symbols.iter().any(|symbol| symbol == "transfer") {
        let start_index = named_u64(value, &["start_index"])?;
        let output_leaves = named_hash_vec(value, &["output_leaves"])?;
        let root = named_hash(value, &["root"])?;

        return Ok(Some(MixerTreeEvent::Transfer {
            start_index,
            output_leaves,
            root,
        }));
    }

    Ok(None)
}

fn topic_symbols(topics: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();

    for topic in topics {
        let value = scval_json_from_base64(topic).context("failed to decode event topic XDR")?;
        collect_symbols(&value, &mut out);
    }

    Ok(out)
}

fn scval_json_from_base64(value: &str) -> Result<Value> {
    let scval = ScVal::from_xdr_base64(value, Limits::none())
        .context("failed to decode Stellar ScVal base64 XDR")?;

    serde_json::to_value(scval).context("failed to convert Stellar ScVal to XDR-JSON")
}

fn collect_symbols(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            if matches!(
                s.as_str(),
                "mixer" | "init" | "deposit" | "withdraw" | "transfer"
            ) {
                out.push(s.clone());
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_symbols(value, out);
            }
        }
        Value::Object(map) => {
            for key in ["symbol", "sym"] {
                if let Some(Value::String(s)) = map.get(key) {
                    out.push(s.clone());
                }
            }

            for value in map.values() {
                collect_symbols(value, out);
            }
        }
        _ => {}
    }
}

fn named_u64(value: &Value, names: &[&str]) -> Result<u64> {
    let v =
        find_named_value(value, names).with_context(|| format!("missing u64 field {names:?}"))?;

    json_to_u64(v).with_context(|| format!("invalid u64 field {names:?}: {v}"))
}

fn named_hash(value: &Value, names: &[&str]) -> Result<Hash> {
    let v =
        find_named_value(value, names).with_context(|| format!("missing hash field {names:?}"))?;

    json_to_hash(v).with_context(|| format!("invalid hash field {names:?}: {v}"))
}

fn named_hash_vec(value: &Value, names: &[&str]) -> Result<Vec<Hash>> {
    let v = find_named_value(value, names)
        .with_context(|| format!("missing hash vec field {names:?}"))?;

    json_to_hash_vec(v).with_context(|| format!("invalid hash vec field {names:?}: {v}"))
}

fn find_named_value<'a>(value: &'a Value, names: &[&str]) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            for name in names {
                if let Some(v) = map.get(*name) {
                    return Some(v);
                }
            }

            if let Some(key) = map.get("key") {
                if value_matches_name(key, names) {
                    if let Some(v) = map.get("val").or_else(|| map.get("value")) {
                        return Some(v);
                    }
                }
            }

            for v in map.values() {
                if let Some(found) = find_named_value(v, names) {
                    return Some(found);
                }
            }

            None
        }
        Value::Array(values) => {
            for v in values {
                if let Some(found) = find_named_value(v, names) {
                    return Some(found);
                }
            }

            None
        }
        _ => None,
    }
}

fn value_matches_name(value: &Value, names: &[&str]) -> bool {
    match value {
        Value::String(s) => names.iter().any(|name| s == name),
        Value::Object(map) => {
            for key in ["symbol", "sym"] {
                if let Some(Value::String(s)) = map.get(key) {
                    if names.iter().any(|name| s == name) {
                        return true;
                    }
                }
            }

            map.values().any(|v| value_matches_name(v, names))
        }
        Value::Array(values) => values.iter().any(|v| value_matches_name(v, names)),
        _ => false,
    }
}

fn json_to_u64(value: &Value) -> Result<u64> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }

    if let Some(s) = value.as_str() {
        return Ok(s.parse()?);
    }

    if let Value::Object(map) = value {
        for key in ["u64", "u32", "i64", "i32"] {
            if let Some(v) = map.get(key) {
                return json_to_u64(v);
            }
        }

        if map.len() == 1 {
            if let Some(v) = map.values().next() {
                return json_to_u64(v);
            }
        }
    }

    bail!("not a u64-compatible JSON value")
}

fn json_to_hash_vec(value: &Value) -> Result<Vec<Hash>> {
    match value {
        Value::Array(values) => values.iter().map(json_to_hash).collect(),
        Value::Object(map) => {
            for key in ["vec", "Vec", "values", "output_leaves"] {
                if let Some(v) = map.get(key) {
                    return json_to_hash_vec(v);
                }
            }

            if map.len() == 1 {
                if let Some(v) = map.values().next() {
                    return json_to_hash_vec(v);
                }
            }

            bail!("not a hash vec JSON object")
        }
        _ => bail!("not a hash vec JSON value"),
    }
}

fn json_to_hash(value: &Value) -> Result<Hash> {
    if let Some(s) = value.as_str() {
        return parse_hash_string(s);
    }

    if let Some(values) = value.as_array() {
        if values.len() == 32 {
            let mut out = [0u8; 32];

            for (i, v) in values.iter().enumerate() {
                let n = v.as_u64().context("hash byte is not a number")?;
                if n > u8::MAX as u64 {
                    bail!("hash byte is out of range");
                }
                out[i] = n as u8;
            }

            return Ok(out);
        }
    }

    if let Value::Object(map) = value {
        for key in ["bytes", "bytesN", "bytesn", "bytesN32", "BytesN", "hash"] {
            if let Some(v) = map.get(key) {
                return json_to_hash(v);
            }
        }

        if map.len() == 1 {
            if let Some(v) = map.values().next() {
                return json_to_hash(v);
            }
        }
    }

    bail!("not a hash JSON value")
}

fn parse_hash_string(value: &str) -> Result<Hash> {
    let trimmed = value.trim();
    let hex_value = trimmed.strip_prefix("0x").unwrap_or(trimmed);

    let bytes = if hex_value.len() == 64 && hex_value.chars().all(|c| c.is_ascii_hexdigit()) {
        hex::decode(hex_value)?
    } else {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
        BASE64.decode(trimmed.as_bytes())?
    };

    if bytes.len() != 32 {
        bail!("expected 32-byte hash, got {} bytes", bytes.len());
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hash_hex(byte: u8) -> String {
        hex::encode([byte; 32])
    }

    fn symbols(topic_name: &str) -> Vec<String> {
        vec!["mixer".to_string(), topic_name.to_string()]
    }

    fn event_value(fields: &[(&str, Value)]) -> Value {
        Value::Object(
            fields
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect(),
        )
    }

    #[test]
    fn parses_deposit_event_from_xdr_json_shape() {
        let value = event_value(&[
            ("index", json!(7)),
            ("leaf", json!(hash_hex(1))),
            ("root", json!(hash_hex(2))),
        ]);

        let parsed = parse_mixer_tree_event_from_xdr_json(&symbols("deposit"), &value).unwrap();

        match parsed {
            Some(MixerTreeEvent::Deposit { index, leaf, root }) => {
                assert_eq!(index, 7);
                assert_eq!(leaf, [1u8; 32]);
                assert_eq!(root, [2u8; 32]);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_deposit_event_from_named_scval_map_shape() {
        let value = json!({
            "map": [
                { "key": { "symbol": "index" }, "val": { "u64": 7 } },
                { "key": { "symbol": "leaf" }, "val": { "bytes": hash_hex(1) } },
                { "key": { "symbol": "root" }, "val": { "bytes": hash_hex(2) } }
            ]
        });

        let parsed = parse_mixer_tree_event_from_xdr_json(&symbols("deposit"), &value).unwrap();

        match parsed {
            Some(MixerTreeEvent::Deposit { index, leaf, root }) => {
                assert_eq!(index, 7);
                assert_eq!(leaf, [1u8; 32]);
                assert_eq!(root, [2u8; 32]);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_withdraw_change_event_from_xdr_json_shape() {
        let value = event_value(&[
            ("index", json!(8)),
            ("output_leaf", json!(hash_hex(3))),
            ("root", json!(hash_hex(4))),
        ]);

        let parsed = parse_mixer_tree_event_from_xdr_json(&symbols("withdraw"), &value).unwrap();

        match parsed {
            Some(MixerTreeEvent::WithdrawChange { index, leaf, root }) => {
                assert_eq!(index, 8);
                assert_eq!(leaf, [3u8; 32]);
                assert_eq!(root, [4u8; 32]);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn parses_transfer_event_from_xdr_json_shape() {
        let value = event_value(&[
            ("start_index", json!(9)),
            ("output_leaves", json!([hash_hex(5), hash_hex(6)])),
            ("root", json!(hash_hex(7))),
        ]);

        let parsed = parse_mixer_tree_event_from_xdr_json(&symbols("transfer"), &value).unwrap();

        match parsed {
            Some(MixerTreeEvent::Transfer {
                start_index,
                output_leaves,
                root,
            }) => {
                assert_eq!(start_index, 9);
                assert_eq!(output_leaves, vec![[5u8; 32], [6u8; 32]]);
                assert_eq!(root, [7u8; 32]);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn ignores_unknown_event_topic() {
        let parsed = parse_mixer_tree_event_from_xdr_json(&symbols("unknown"), &json!({})).unwrap();

        assert!(parsed.is_none());
    }
}
