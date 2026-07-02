use anyhow::{bail, Context, Result};
use rocksdb::{Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use treepir_core::{Hash, LevelMerkleTree};

const META_KEY: &[u8] = b"meta";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreePirPersistentMetadata {
    pub version: u32,
    pub contract_id: String,
    pub start_ledger: u64,
    pub last_indexed_ledger: u64,
    pub leaf_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredLeaf {
    pub index: u64,
    pub leaf_hex: String,
    pub event_id: String,
    pub ledger: u64,
    pub source: String,
}

pub struct PersistentTreeStore {
    path: PathBuf,
    db: DB,
    metadata: TreePirPersistentMetadata,
}

impl std::fmt::Debug for PersistentTreeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentTreeStore")
            .field("path", &self.path)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl PersistentTreeStore {
    pub fn load_or_create(
        path: impl Into<PathBuf>,
        contract_id: String,
        start_ledger: Option<u64>,
    ) -> Result<Self> {
        let path = path.into();

        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(256);
        options.set_keep_log_file_num(8);

        let db = DB::open(&options, &path)
            .with_context(|| format!("failed to open RocksDB at {}", path.display()))?;

        if let Some(bytes) = db.get(META_KEY)? {
            let metadata: TreePirPersistentMetadata =
                serde_json::from_slice(&bytes).with_context(|| {
                    format!("failed to parse RocksDB metadata at {}", path.display())
                })?;

            if metadata.version != 1 {
                bail!("unsupported state version {}", metadata.version);
            }

            if metadata.contract_id != contract_id {
                bail!(
                    "state DB belongs to contract {}, config points to {}",
                    metadata.contract_id,
                    contract_id
                );
            }

            return Ok(Self { path, db, metadata });
        }

        let start_ledger = start_ledger
            .context("TREEPIR_START_LEDGER is required when state DB does not exist")?;

        let metadata = TreePirPersistentMetadata {
            version: 1,
            contract_id,
            start_ledger,
            last_indexed_ledger: start_ledger.saturating_sub(1),
            leaf_count: 0,
        };

        let store = Self { path, db, metadata };
        store.save()?;

        Ok(store)
    }


    pub fn last_indexed_ledger(&self) -> u64 {
        self.metadata.last_indexed_ledger
    }

    pub fn leaf_count(&self) -> u64 {
        self.metadata.leaf_count
    }

    pub fn set_last_indexed_ledger(&mut self, ledger: u64) {
        self.metadata.last_indexed_ledger = ledger;
    }

    pub fn has_event_id(&self, event_id: &str) -> bool {
        self.db
            .get(event_key(event_id))
            .map(|value| value.is_some())
            .unwrap_or(false)
    }

    pub fn append_leaf_record(
        &mut self,
        index: u64,
        leaf: Hash,
        event_id: &str,
        ledger: u64,
        source: &str,
    ) -> Result<()> {
        if self.has_event_id(event_id) {
            return Ok(());
        }

        if index != self.metadata.leaf_count {
            bail!(
                "persistent leaf index mismatch: next={}, event={}",
                self.metadata.leaf_count,
                index
            );
        }

        let stored = StoredLeaf {
            index,
            leaf_hex: hex::encode(leaf),
            event_id: event_id.to_string(),
            ledger,
            source: source.to_string(),
        };

        let mut next_metadata = self.metadata.clone();
        next_metadata.leaf_count = next_metadata.leaf_count.saturating_add(1);

        let mut batch = WriteBatch::default();
        batch.put(leaf_key(index), serde_json::to_vec(&stored)?);
        batch.put(event_key(event_id), index.to_be_bytes());
        batch.put(META_KEY, serde_json::to_vec(&next_metadata)?);

        self.db.write(batch)?;

        self.metadata = next_metadata;

        Ok(())
    }

    pub fn save(&self) -> Result<()> {
        self.db.put(META_KEY, serde_json::to_vec(&self.metadata)?)?;
        self.db.flush()?;
        Ok(())
    }

    pub fn build_tree<const DEPTH: usize>(&self) -> Result<LevelMerkleTree<DEPTH>> {
        let mut tree = LevelMerkleTree::<DEPTH>::new()?;

        let mut index = 0u64;
        while index < self.metadata.leaf_count {
            let key = leaf_key(index);
            let Some(bytes) = self.db.get(&key)? else {
                bail!("missing stored leaf at index {index}");
            };

            let stored: StoredLeaf = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed to parse stored leaf at index {index}"))?;

            if stored.index != index {
                bail!(
                    "stored leaf key/index mismatch: key={}, value={}",
                    index,
                    stored.index
                );
            }

            if stored.index != tree.leaf_count() as u64 {
                bail!(
                    "stored leaves are not contiguous: next={}, got={}",
                    tree.leaf_count(),
                    stored.index
                );
            }

            let leaf = decode_hash_hex(&stored.leaf_hex)?;
            tree.append_leaf(leaf)?;

            index += 1;
        }

        Ok(tree)
    }
}

fn leaf_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(13);
    key.extend_from_slice(b"leaf/");
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn event_key(event_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + event_id.len());
    key.extend_from_slice(b"event/");
    key.extend_from_slice(event_id.as_bytes());
    key
}

fn decode_hash_hex(value: &str) -> Result<Hash> {
    let bytes = hex::decode(value.trim_start_matches("0x"))?;

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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_state_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        std::env::temp_dir().join(format!(
            "treepir-server-rocksdb-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn store_requires_start_ledger_when_creating_new_state() {
        let path = temp_state_path("missing-start-ledger");

        let error = PersistentTreeStore::load_or_create(path.clone(), "CONTRACT".to_string(), None)
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("TREEPIR_START_LEDGER"),
            "unexpected error: {error}"
        );

        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn store_roundtrips_leaves_and_rebuilds_tree() {
        const DEPTH: usize = 4;

        let path = temp_state_path("roundtrip");

        {
            let mut store = PersistentTreeStore::load_or_create(
                path.clone(),
                "CONTRACT".to_string(),
                Some(123),
            )
            .unwrap();

            assert_eq!(store.last_indexed_ledger(), 122);
            assert_eq!(store.leaf_count(), 0);

            store
                .append_leaf_record(0, [1u8; 32], "event-1", 123, "deposit")
                .unwrap();
            store
                .append_leaf_record(1, [2u8; 32], "event-2", 124, "withdraw")
                .unwrap();

            store.set_last_indexed_ledger(124);
            store.save().unwrap();
        }

        {
            let reloaded =
                PersistentTreeStore::load_or_create(path.clone(), "CONTRACT".to_string(), None)
                    .unwrap();

            assert_eq!(reloaded.last_indexed_ledger(), 124);
            assert_eq!(reloaded.leaf_count(), 2);
            assert!(reloaded.has_event_id("event-1"));
            assert!(reloaded.has_event_id("event-2"));

            let tree = reloaded.build_tree::<DEPTH>().unwrap();

            assert_eq!(tree.leaf_count(), 2);
        }

        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn store_rejects_non_contiguous_leaf_indices() {
        let path = temp_state_path("gap");

        let mut store =
            PersistentTreeStore::load_or_create(path.clone(), "CONTRACT".to_string(), Some(123))
                .unwrap();

        let error = store
            .append_leaf_record(1, [1u8; 32], "event-1", 123, "deposit")
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("persistent leaf index mismatch"),
            "unexpected error: {error}"
        );

        drop(store);
        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn store_rejects_wrong_contract_id_on_reload() {
        let path = temp_state_path("wrong-contract");

        {
            let store = PersistentTreeStore::load_or_create(
                path.clone(),
                "CONTRACT_A".to_string(),
                Some(123),
            )
            .unwrap();

            store.save().unwrap();
        }

        let error =
            PersistentTreeStore::load_or_create(path.clone(), "CONTRACT_B".to_string(), None)
                .unwrap_err()
                .to_string();

        assert!(
            error.contains("state DB belongs to contract"),
            "unexpected error: {error}"
        );

        let _ = DB::destroy(&Options::default(), path);
    }

    #[test]
    fn duplicate_event_id_is_idempotent() {
        let path = temp_state_path("duplicate-event");

        let mut store =
            PersistentTreeStore::load_or_create(path.clone(), "CONTRACT".to_string(), Some(123))
                .unwrap();

        store
            .append_leaf_record(0, [1u8; 32], "event-1", 123, "deposit")
            .unwrap();

        store
            .append_leaf_record(1, [2u8; 32], "event-1", 123, "deposit")
            .unwrap();

        assert_eq!(store.leaf_count(), 1);

        drop(store);
        let _ = DB::destroy(&Options::default(), path);
    }
}
