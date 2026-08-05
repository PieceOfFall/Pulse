use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use rs_netty::codec::{Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS};
use serde::Deserialize;

use super::{
    BrokerStorage,
    delta::{
        CLIENT_PATCH_VERSION, ClientPatch, ClientPatchMode, PendingSnapshot, PersistentProjection,
        QueuedSnapshot, RetainedPatch, SessionSnapshot, StoragePatch, SubscriptionSnapshot,
        prepare_patches,
    },
};
use crate::broker::runtime::{
    retained_store::RetainedMessage,
    session_registry::{BrokerState, QueuedPublish, SessionEntry},
};

const V1_MAGIC: &[u8] = b"PBIN1\n";
const V2_MAGIC: &[u8] = b"PBIN2\n";
const LEGACY_LOG_FILE_NAME: &str = "broker.binlog";
const LEGACY_CHECKPOINT_FILE_NAME: &str = "broker.checkpoint";
const MANIFEST_FILE_NAME: &str = "broker.manifest";
const V2_FORMAT: &str = "pbin2";
const V2_CHECKPOINT_PREFIX: &str = "broker.pbin2.checkpoint.";
const V2_WAL_PREFIX: &str = "broker.pbin2.wal.";
const TMP_SUFFIX: &str = ".tmp";
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_WAL_COMPACT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_WAL_COMPACT_INTERVAL_MS: u64 = 10 * 60 * 1000;

const CLIENT_PATCH: u8 = 1;
const RETAINED_PATCH: u8 = 2;
const CHECKPOINT_END_PAYLOAD: &[u8] = b"\x03PEND2";

const V1_SESSION_UPSERT: u8 = 1;
const V1_SESSION_DELETE: u8 = 2;
const V1_SUBSCRIPTION_UPSERT: u8 = 3;
const V1_SUBSCRIPTION_DELETE: u8 = 4;
const V1_RETAINED_UPSERT: u8 = 5;
const V1_RETAINED_DELETE: u8 = 6;
const V1_OFFLINE_REPLACE: u8 = 7;
const V1_OUTBOUND_REPLACE: u8 = 8;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CommitPolicy {
    Strict,
    #[default]
    Balanced,
    Fast,
}

#[derive(Debug)]
pub(crate) struct ParseCommitPolicyError {
    value: String,
}

impl fmt::Display for ParseCommitPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "storage.commit_policy must be one of strict, balanced, fast; got `{}`",
            self.value
        )
    }
}

impl Error for ParseCommitPolicyError {}

impl FromStr for CommitPolicy {
    type Err = ParseCommitPolicyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "strict" => Ok(Self::Strict),
            "balanced" => Ok(Self::Balanced),
            "fast" => Ok(Self::Fast),
            _ => Err(ParseCommitPolicyError {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalCompactConfig {
    pub(crate) max_bytes: u64,
    pub(crate) interval_ms: u64,
}

impl Default for WalCompactConfig {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_WAL_COMPACT_MAX_BYTES,
            interval_ms: DEFAULT_WAL_COMPACT_INTERVAL_MS,
        }
    }
}

pub(crate) struct BinaryStorage {
    inner: Mutex<BinaryStorageInner>,
    log: Mutex<BinaryLog>,
}

struct BinaryStorageInner {
    state: BrokerState,
    projection: PersistentProjection,
}

impl BinaryStorage {
    #[cfg(test)]
    pub(crate) fn open(dir: impl AsRef<Path>, commit_policy: CommitPolicy) -> io::Result<Self> {
        Self::open_with_options(dir, commit_policy, WalCompactConfig::default())
    }

    pub(crate) fn open_with_options(
        dir: impl AsRef<Path>,
        commit_policy: CommitPolicy,
        compact: WalCompactConfig,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let recovered = recover_storage(dir)?;
        let state = recovered.projection.clone().into_state();
        let log = BinaryLog::open(
            dir.to_path_buf(),
            recovered.active_path,
            recovered.active_epoch,
            commit_policy,
            compact,
        )?;
        Ok(Self {
            inner: Mutex::new(BinaryStorageInner {
                state,
                projection: recovered.projection,
            }),
            log: Mutex::new(log),
        })
    }
}

impl BrokerStorage for BinaryStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut inner = self.inner.lock().expect("broker state lock poisoned");
        operation(&mut inner.state);
        let changes = inner.state.persistence_changes();
        if changes.is_empty() {
            return;
        }

        let patches = prepare_patches(&inner.projection, &inner.state, &changes);
        if patches.is_empty() {
            inner.state.take_persistence_changes();
            return;
        }

        let mut log = self.log.lock().expect("binary log lock poisoned");
        log.append_many(&patches)
            .expect("persist broker state to PBIN2 log");
        for patch in &patches {
            inner
                .projection
                .apply_patch(patch)
                .expect("apply generated PBIN2 patch");
        }
        inner.state.take_persistence_changes();
        log.compact_if_needed(&inner.projection)
            .expect("compact PBIN2 checkpoint");
    }

    fn read_state(&self, operation: &mut dyn FnMut(&BrokerState)) {
        let inner = self.inner.lock().expect("broker state lock poisoned");
        operation(&inner.state);
    }
}

struct BinaryLog {
    dir: PathBuf,
    active_path: PathBuf,
    active_epoch: u64,
    file: Option<File>,
    commit_policy: CommitPolicy,
    compact: WalCompactConfig,
    current_bytes: u64,
    last_compacted_at: Instant,
    records_since_checkpoint: usize,
    pending_balanced_records: usize,
    fast_tx: Option<mpsc::Sender<Vec<StoragePatch>>>,
    fast_thread: Option<JoinHandle<io::Result<()>>>,
}

impl BinaryLog {
    fn open(
        dir: PathBuf,
        active_path: PathBuf,
        active_epoch: u64,
        commit_policy: CommitPolicy,
        compact: WalCompactConfig,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&active_path)?;
        let current_bytes = file.metadata()?.len();
        let mut log = Self {
            dir,
            active_path,
            active_epoch,
            file: None,
            commit_policy,
            compact,
            current_bytes,
            last_compacted_at: Instant::now(),
            records_since_checkpoint: 0,
            pending_balanced_records: 0,
            fast_tx: None,
            fast_thread: None,
        };
        log.start_writer(file);
        Ok(log)
    }

    fn append_many(&mut self, patches: &[StoragePatch]) -> io::Result<()> {
        let encoded_bytes = encoded_patches_len(patches)?;
        if let Some(tx) = &self.fast_tx {
            tx.send(patches.to_vec())
                .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "PBIN2 writer stopped"))?;
            self.current_bytes += encoded_bytes;
            self.records_since_checkpoint += patches.len();
            return Ok(());
        }

        let file = self.file.as_mut().expect("sync PBIN2 log file");
        self.current_bytes += write_patches(file, patches)?;
        self.records_since_checkpoint += patches.len();
        self.pending_balanced_records += patches.len();
        match self.commit_policy {
            CommitPolicy::Strict => {
                file.flush()?;
                file.sync_data()?;
                self.pending_balanced_records = 0;
            }
            CommitPolicy::Balanced if self.pending_balanced_records >= 64 => {
                file.flush()?;
                file.sync_data()?;
                self.pending_balanced_records = 0;
            }
            CommitPolicy::Balanced | CommitPolicy::Fast => {}
        }
        Ok(())
    }

    fn compact_if_needed(&mut self, projection: &PersistentProjection) -> io::Result<()> {
        let size_triggered =
            self.compact.max_bytes != 0 && self.current_bytes > self.compact.max_bytes;
        let time_triggered = self.compact.interval_ms != 0
            && self.records_since_checkpoint > 0
            && self.last_compacted_at.elapsed() >= Duration::from_millis(self.compact.interval_ms);
        if size_triggered || time_triggered {
            self.compact(projection)?;
        }
        Ok(())
    }

    fn compact(&mut self, projection: &PersistentProjection) -> io::Result<()> {
        self.close_writer()?;
        let next_epoch = next_v2_epoch(&self.dir)?.max(self.active_epoch.saturating_add(1));
        let checkpoint_path = v2_checkpoint_path(&self.dir, next_epoch);
        let wal_path = v2_wal_path(&self.dir, next_epoch);
        write_checkpoint(&checkpoint_path, projection)?;
        let new_file = create_v2_wal(&wal_path)?;
        sync_dir(&self.dir)?;

        let manifest = WalManifest::v2(next_epoch);
        write_manifest(&self.dir, &manifest)?;
        sync_dir(&self.dir)?;

        self.active_path = wal_path;
        self.active_epoch = next_epoch;
        self.current_bytes = V2_MAGIC.len() as u64;
        self.records_since_checkpoint = 0;
        self.pending_balanced_records = 0;
        self.last_compacted_at = Instant::now();
        self.start_writer(new_file);
        cleanup_after_v2_commit(&self.dir, &manifest)?;
        Ok(())
    }

    fn start_writer(&mut self, file: File) {
        if matches!(self.commit_policy, CommitPolicy::Fast) {
            let (tx, rx) = mpsc::channel::<Vec<StoragePatch>>();
            let fast_thread = thread::spawn(move || {
                let mut file = file;
                while let Ok(patches) = rx.recv() {
                    write_patches(&mut file, &patches)?;
                }
                file.flush()?;
                file.sync_data()
            });
            self.file = None;
            self.fast_tx = Some(tx);
            self.fast_thread = Some(fast_thread);
        } else {
            self.file = Some(file);
            self.fast_tx = None;
            self.fast_thread = None;
        }
    }

    fn close_writer(&mut self) -> io::Result<()> {
        self.fast_tx.take();
        if let Some(thread) = self.fast_thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "PBIN2 writer panicked"))??;
        }
        if let Some(mut file) = self.file.take() {
            file.flush()?;
            file.sync_data()?;
        }
        Ok(())
    }
}

impl Drop for BinaryLog {
    fn drop(&mut self) {
        let _ = self.close_writer();
    }
}

fn write_patches(file: &mut File, patches: &[StoragePatch]) -> io::Result<u64> {
    let mut written = 0;
    for patch in patches {
        validate_storage_patch(patch)?;
        let payload = encode_patch(patch);
        written += write_frame(file, &payload)?;
    }
    Ok(written)
}

fn write_frame(file: &mut File, payload: &[u8]) -> io::Result<u64> {
    if payload.len() > MAX_FRAME_BYTES || payload.len() > u32::MAX as usize {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "PBIN2 frame exceeds the maximum size",
        ));
    }
    let checksum = crc32(payload);
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(payload)?;
    file.write_all(&checksum.to_le_bytes())?;
    Ok(frame_encoded_len(payload.len()))
}

fn encoded_patches_len(patches: &[StoragePatch]) -> io::Result<u64> {
    patches.iter().try_fold(0u64, |total, patch| {
        validate_storage_patch(patch)?;
        let payload_len = encode_patch(patch).len();
        if payload_len > MAX_FRAME_BYTES || payload_len > u32::MAX as usize {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "PBIN2 frame exceeds the maximum size",
            ));
        }
        Ok(total + frame_encoded_len(payload_len))
    })
}

fn validate_storage_patch(patch: &StoragePatch) -> io::Result<()> {
    patch.validate().map_err(invalid_data)
}

fn frame_encoded_len(payload_len: usize) -> u64 {
    (4 + payload_len + 4) as u64
}

struct RecoveredStorage {
    projection: PersistentProjection,
    active_path: PathBuf,
    active_epoch: u64,
}

#[derive(Clone, Debug)]
struct WalManifest {
    version: u32,
    format: Option<String>,
    checkpoint: String,
    active_log: String,
    active_epoch: u64,
}

impl WalManifest {
    fn v2(active_epoch: u64) -> Self {
        Self {
            version: 2,
            format: Some(V2_FORMAT.to_string()),
            checkpoint: v2_checkpoint_file_name(active_epoch),
            active_log: v2_wal_file_name(active_epoch),
            active_epoch,
        }
    }

    fn encode(&self) -> String {
        let format = self
            .format
            .as_deref()
            .map(|format| format!("format={format}\n"))
            .unwrap_or_default();
        format!(
            "version={}\n{format}checkpoint={}\nactive_log={}\nactive_epoch={}\n",
            self.version, self.checkpoint, self.active_log, self.active_epoch
        )
    }
}

fn recover_storage(dir: &Path) -> io::Result<RecoveredStorage> {
    let manifest_path = dir.join(MANIFEST_FILE_NAME);
    if manifest_path.exists() {
        let manifest = read_manifest(&manifest_path)?;
        return match manifest.version {
            2 => recover_v2_manifest(dir, manifest),
            1 => {
                let state = recover_v1_manifest(dir, &manifest)?;
                migrate_v1_state(dir, state)
            }
            _ => Err(io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "unsupported Pulse binary manifest version {}",
                    manifest.version
                ),
            )),
        };
    }

    let legacy_path = dir.join(LEGACY_LOG_FILE_NAME);
    let state = if legacy_path.exists() {
        replay_v1_log(&legacy_path, ReplayMode::AllowPhysicalTail)?
    } else {
        BrokerState::default()
    };
    migrate_v1_state(dir, state)
}

fn recover_v2_manifest(dir: &Path, manifest: WalManifest) -> io::Result<RecoveredStorage> {
    if manifest.format.as_deref() != Some(V2_FORMAT) {
        return Err(invalid_data("PBIN2 manifest has an unsupported format"));
    }
    if manifest.checkpoint != v2_checkpoint_file_name(manifest.active_epoch)
        || manifest.active_log != v2_wal_file_name(manifest.active_epoch)
    {
        return Err(invalid_data(
            "PBIN2 manifest files do not match active_epoch",
        ));
    }
    let checkpoint_path = manifest_file_path(dir, &manifest.checkpoint)?;
    let active_path = manifest_file_path(dir, &manifest.active_log)?;
    require_file(&checkpoint_path, "PBIN2 checkpoint")?;
    require_file(&active_path, "PBIN2 active WAL")?;

    let mut projection = replay_v2(&checkpoint_path, ReplayMode::Strict)?;
    let replay = replay_v2_into(&active_path, &mut projection, ReplayMode::AllowPhysicalTail)?;
    if replay.truncated_tail {
        truncate_wal(&active_path, replay.valid_len)?;
    }
    if projection.canonicalize_for_offline_recovery() {
        return install_canonical_v2_generation(dir, projection);
    }
    cleanup_after_v2_commit(dir, &manifest)?;
    Ok(RecoveredStorage {
        projection,
        active_path,
        active_epoch: manifest.active_epoch,
    })
}

fn install_canonical_v2_generation(
    dir: &Path,
    projection: PersistentProjection,
) -> io::Result<RecoveredStorage> {
    let active_epoch = next_v2_epoch(dir)?;
    let checkpoint_path = v2_checkpoint_path(dir, active_epoch);
    let active_path = v2_wal_path(dir, active_epoch);
    write_checkpoint(&checkpoint_path, &projection)?;
    drop(create_v2_wal(&active_path)?);
    sync_dir(dir)?;

    let manifest = WalManifest::v2(active_epoch);
    write_manifest(dir, &manifest)?;
    sync_dir(dir)?;
    cleanup_after_v2_commit(dir, &manifest)?;
    Ok(RecoveredStorage {
        projection,
        active_path,
        active_epoch,
    })
}

fn recover_v1_manifest(dir: &Path, manifest: &WalManifest) -> io::Result<BrokerState> {
    if manifest.format.is_some() {
        return Err(invalid_data(
            "PBIN1 manifest unexpectedly declares a format",
        ));
    }
    if manifest.active_log != legacy_wal_file_name(manifest.active_epoch) {
        return Err(invalid_data(
            "PBIN1 manifest active_log does not match active_epoch",
        ));
    }
    let checkpoint_path = manifest_file_path(dir, &manifest.checkpoint)?;
    let active_path = manifest_file_path(dir, &manifest.active_log)?;
    require_file(&checkpoint_path, "PBIN1 checkpoint")?;
    require_file(&active_path, "PBIN1 active WAL")?;
    let mut state = replay_v1_log(&checkpoint_path, ReplayMode::Strict)?;
    replay_v1_into(&active_path, &mut state, ReplayMode::AllowPhysicalTail)?;
    Ok(state)
}

fn migrate_v1_state(dir: &Path, state: BrokerState) -> io::Result<RecoveredStorage> {
    let mut projection = PersistentProjection::from_state(&state);
    projection.canonicalize_for_offline_recovery();
    let active_epoch = next_v2_epoch(dir)?;
    let checkpoint_path = v2_checkpoint_path(dir, active_epoch);
    let active_path = v2_wal_path(dir, active_epoch);

    write_checkpoint(&checkpoint_path, &projection)?;
    create_v2_wal(&active_path)?;
    sync_dir(dir)?;
    let manifest = WalManifest::v2(active_epoch);
    write_manifest(dir, &manifest)?;
    sync_dir(dir)?;
    cleanup_after_v2_commit(dir, &manifest)?;
    Ok(RecoveredStorage {
        projection,
        active_path,
        active_epoch,
    })
}

fn read_manifest(path: &Path) -> io::Result<WalManifest> {
    let contents = fs::read_to_string(path)?;
    let mut version = None;
    let mut format = None;
    let mut checkpoint = None;
    let mut active_log = None;
    let mut active_epoch = None;
    for line in contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| invalid_data(format!("invalid manifest line: {line}")))?;
        match key {
            "version" => set_once(
                &mut version,
                value
                    .parse::<u32>()
                    .map_err(|error| invalid_data(format!("invalid manifest version: {error}")))?,
                "version",
            )?,
            "format" => set_once(&mut format, value.to_string(), "format")?,
            "checkpoint" => set_once(&mut checkpoint, value.to_string(), "checkpoint")?,
            "active_log" => set_once(&mut active_log, value.to_string(), "active_log")?,
            "active_epoch" => set_once(
                &mut active_epoch,
                value.parse::<u64>().map_err(|error| {
                    invalid_data(format!("invalid manifest active_epoch: {error}"))
                })?,
                "active_epoch",
            )?,
            _ => return Err(invalid_data(format!("unknown manifest field: {key}"))),
        }
    }
    Ok(WalManifest {
        version: version.ok_or_else(|| invalid_data("manifest missing version"))?,
        format,
        checkpoint: checkpoint.ok_or_else(|| invalid_data("manifest missing checkpoint"))?,
        active_log: active_log.ok_or_else(|| invalid_data("manifest missing active_log"))?,
        active_epoch: active_epoch.ok_or_else(|| invalid_data("manifest missing active_epoch"))?,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, field: &str) -> io::Result<()> {
    if slot.replace(value).is_some() {
        return Err(invalid_data(format!("duplicate manifest field: {field}")));
    }
    Ok(())
}

fn write_checkpoint(path: &Path, projection: &PersistentProjection) -> io::Result<()> {
    let tmp = tmp_path(path)?;
    remove_file_if_exists(&tmp)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    file.write_all(V2_MAGIC)?;
    write_patches(&mut file, &checkpoint_patches(projection))?;
    write_frame(&mut file, CHECKPOINT_END_PAYLOAD)?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    Ok(())
}

fn checkpoint_patches(projection: &PersistentProjection) -> Vec<StoragePatch> {
    let mut patches = Vec::with_capacity(projection.clients.len() + projection.retained.len());
    patches.extend(projection.clients.iter().map(|(client_id, client)| {
        StoragePatch::Client(ClientPatch::reset(client_id.clone(), client))
    }));
    patches.extend(projection.retained.iter().map(|(topic_name, message)| {
        StoragePatch::Retained(RetainedPatch {
            topic_name: topic_name.clone(),
            message: Some(message.clone()),
        })
    }));
    patches
}

fn write_manifest(dir: &Path, manifest: &WalManifest) -> io::Result<()> {
    let path = dir.join(MANIFEST_FILE_NAME);
    let tmp = tmp_path(&path)?;
    remove_file_if_exists(&tmp)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    file.write_all(manifest.encode().as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn create_v2_wal(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .append(true)
        .read(true)
        .create_new(true)
        .open(path)?;
    file.write_all(V2_MAGIC)?;
    file.flush()?;
    file.sync_data()?;
    Ok(file)
}

fn cleanup_after_v2_commit(dir: &Path, manifest: &WalManifest) -> io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let stale_tmp = name.ends_with(TMP_SUFFIX);
        let stale_v2_checkpoint =
            v2_epoch_from_name(name, V2_CHECKPOINT_PREFIX).is_some() && name != manifest.checkpoint;
        let stale_v2_wal =
            v2_epoch_from_name(name, V2_WAL_PREFIX).is_some() && name != manifest.active_log;
        let legacy = name == LEGACY_LOG_FILE_NAME
            || name == LEGACY_CHECKPOINT_FILE_NAME
            || legacy_wal_epoch_from_file_name(name).is_some();
        if stale_tmp || stale_v2_checkpoint || stale_v2_wal || legacy {
            remove_file_if_exists(&path)?;
        }
    }
    sync_dir(dir)
}

fn next_v2_epoch(dir: &Path) -> io::Result<u64> {
    let mut maximum = 0u64;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(epoch) = v2_epoch_from_name(&name, V2_CHECKPOINT_PREFIX)
                .or_else(|| v2_epoch_from_name(&name, V2_WAL_PREFIX))
            {
                maximum = maximum.max(epoch);
            }
        }
    }
    maximum
        .checked_add(1)
        .ok_or_else(|| invalid_data("PBIN2 epoch exhausted"))
}

fn manifest_file_path(dir: &Path, file_name: &str) -> io::Result<PathBuf> {
    if Path::new(file_name)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(file_name)
    {
        return Err(invalid_data(format!(
            "manifest path must be a file name: {file_name}"
        )));
    }
    Ok(dir.join(file_name))
}

fn require_file(path: &Path, label: &str) -> io::Result<()> {
    if !path.is_file() {
        return Err(invalid_data(format!(
            "{label} is missing: {}",
            path.display()
        )));
    }
    Ok(())
}

fn v2_checkpoint_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(v2_checkpoint_file_name(epoch))
}

fn v2_checkpoint_file_name(epoch: u64) -> String {
    format!("{V2_CHECKPOINT_PREFIX}{epoch}")
}

fn v2_wal_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(v2_wal_file_name(epoch))
}

fn v2_wal_file_name(epoch: u64) -> String {
    format!("{V2_WAL_PREFIX}{epoch}")
}

fn v2_epoch_from_name(name: &str, prefix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?.parse().ok()
}

fn legacy_wal_file_name(epoch: u64) -> String {
    format!("{LEGACY_LOG_FILE_NAME}.{epoch}")
}

fn legacy_wal_epoch_from_file_name(name: &str) -> Option<u64> {
    name.strip_prefix(&format!("{LEGACY_LOG_FILE_NAME}."))?
        .parse()
        .ok()
}

fn tmp_path(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_data("invalid PBIN2 file name"))?;
    Ok(path.with_file_name(format!("{name}{TMP_SUFFIX}")))
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    match File::open(dir) {
        Ok(file) => file.sync_all(),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::PermissionDenied | ErrorKind::IsADirectory
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[derive(Clone, Copy)]
enum ReplayMode {
    AllowPhysicalTail,
    Strict,
}

fn replay_v2(path: &Path, mode: ReplayMode) -> io::Result<PersistentProjection> {
    let mut projection = PersistentProjection::default();
    replay_v2_into(path, &mut projection, mode)?;
    Ok(projection)
}

struct ReplayOutcome {
    valid_len: u64,
    truncated_tail: bool,
}

fn replay_v2_into(
    path: &Path,
    projection: &mut PersistentProjection,
    mode: ReplayMode,
) -> io::Result<ReplayOutcome> {
    let mut reader = BufReader::new(File::open(path)?);
    read_header(&mut reader, V2_MAGIC, "PBIN2")?;
    let mut valid_len = V2_MAGIC.len() as u64;
    loop {
        let frame = match read_frame(&mut reader, mode, "PBIN2")? {
            FrameRead::Complete(frame) => frame,
            FrameRead::End => {
                if matches!(mode, ReplayMode::Strict) {
                    return Err(invalid_data("PBIN2 checkpoint end marker is missing"));
                }
                return Ok(ReplayOutcome {
                    valid_len,
                    truncated_tail: false,
                });
            }
            FrameRead::Truncated => {
                return Ok(ReplayOutcome {
                    valid_len,
                    truncated_tail: true,
                });
            }
        };
        if crc32(&frame.payload) != frame.checksum {
            return Err(invalid_data("PBIN2 frame checksum mismatch"));
        }
        if frame.payload == CHECKPOINT_END_PAYLOAD {
            if !matches!(mode, ReplayMode::Strict) {
                return Err(invalid_data("PBIN2 WAL contains a checkpoint end marker"));
            }
            valid_len += frame_encoded_len(frame.payload.len());
            return match read_frame(&mut reader, ReplayMode::Strict, "PBIN2")? {
                FrameRead::End => Ok(ReplayOutcome {
                    valid_len,
                    truncated_tail: false,
                }),
                FrameRead::Complete(_) => Err(invalid_data(
                    "PBIN2 checkpoint contains data after its end marker",
                )),
                FrameRead::Truncated => unreachable!("strict replay rejects truncated frames"),
            };
        }
        let patch = decode_patch(&frame.payload).map_err(invalid_data)?;
        projection.apply_patch(&patch).map_err(invalid_data)?;
        valid_len += frame_encoded_len(frame.payload.len());
    }
}

fn read_header(reader: &mut impl Read, magic: &[u8], label: &str) -> io::Result<()> {
    let mut actual = vec![0; magic.len()];
    reader
        .read_exact(&mut actual)
        .map_err(|error| match error.kind() {
            ErrorKind::UnexpectedEof => invalid_data(format!("incomplete {label} header")),
            _ => error,
        })?;
    if actual != magic {
        return Err(invalid_data(format!("invalid {label} header")));
    }
    Ok(())
}

struct Frame {
    payload: Vec<u8>,
    checksum: u32,
}

enum FrameRead {
    Complete(Frame),
    End,
    Truncated,
}

fn read_frame(reader: &mut impl Read, mode: ReplayMode, label: &str) -> io::Result<FrameRead> {
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length[..1]) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(FrameRead::End),
        Err(error) => return Err(error),
    }
    if !read_tail_part(reader, &mut length[1..], mode, label, "frame length")? {
        return Ok(FrameRead::Truncated);
    }
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(invalid_data(format!(
            "{label} frame exceeds the maximum size"
        )));
    }
    let mut payload = vec![0; length];
    if !read_tail_part(reader, &mut payload, mode, label, "frame payload")? {
        return Ok(FrameRead::Truncated);
    }
    let mut checksum = [0u8; 4];
    if !read_tail_part(reader, &mut checksum, mode, label, "frame checksum")? {
        return Ok(FrameRead::Truncated);
    }
    Ok(FrameRead::Complete(Frame {
        payload,
        checksum: u32::from_le_bytes(checksum),
    }))
}

fn read_tail_part(
    reader: &mut impl Read,
    buffer: &mut [u8],
    mode: ReplayMode,
    label: &str,
    part: &str,
) -> io::Result<bool> {
    match reader.read_exact(buffer) {
        Ok(()) => Ok(true),
        Err(error)
            if error.kind() == ErrorKind::UnexpectedEof
                && matches!(mode, ReplayMode::AllowPhysicalTail) =>
        {
            Ok(false)
        }
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            Err(invalid_data(format!("incomplete {label} {part}")))
        }
        Err(error) => Err(error),
    }
}

fn truncate_wal(path: &Path, valid_len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_len)?;
    file.sync_data()
}

fn encode_patch(patch: &StoragePatch) -> Vec<u8> {
    let mut writer = Writer::default();
    match patch {
        StoragePatch::Client(patch) => encode_client_patch(&mut writer, patch),
        StoragePatch::Retained(patch) => {
            writer.u8(RETAINED_PATCH);
            writer.string(&patch.topic_name);
            writer.bool(patch.message.is_some());
            if let Some(message) = &patch.message {
                writer.opt_u64(message.expires_at_ms);
                writer.bytes(&encode_retained(message));
            }
        }
    }
    writer.into_inner()
}

fn encode_client_patch(writer: &mut Writer, patch: &ClientPatch) {
    writer.u8(CLIENT_PATCH);
    writer.u8(patch.version);
    writer.u8(match patch.mode {
        ClientPatchMode::Merge => 1,
        ClientPatchMode::Reset => 2,
        ClientPatchMode::Delete => 3,
    });
    writer.string(&patch.client_id);
    writer.bool(patch.session.is_some());
    if let Some(session) = &patch.session {
        encode_session(writer, session);
    }
    writer.len(patch.subscription_upserts.len());
    for subscription in &patch.subscription_upserts {
        encode_subscription(writer, subscription);
    }
    writer.len(patch.subscription_deletes.len());
    for filter in &patch.subscription_deletes {
        writer.string(filter);
    }
    writer.opt_u64(patch.offline_remove_through);
    writer.len(patch.offline_append.len());
    for queued in &patch.offline_append {
        writer.u64(queued.sequence);
        encode_pending(writer, &queued.pending);
    }
    encode_pending_map(writer, &patch.qos1_upserts);
    encode_u16_set(writer, &patch.qos1_deletes);
    encode_pending_map(writer, &patch.qos2_publish_upserts);
    encode_u16_set(writer, &patch.qos2_publish_deletes);
    encode_u16_set(writer, &patch.pubrel_add);
    encode_u16_set(writer, &patch.pubrel_remove);
}

fn decode_patch(payload: &[u8]) -> Result<StoragePatch, String> {
    let mut reader = Reader::new(payload);
    let patch = match reader.u8()? {
        CLIENT_PATCH => StoragePatch::Client(decode_client_patch(&mut reader)?),
        RETAINED_PATCH => {
            let topic_name = reader.string()?;
            let message = if reader.bool()? {
                let expires_at_ms = reader.opt_u64()?;
                let mut message = decode_retained(reader.bytes()?)
                    .ok_or_else(|| "invalid retained MQTT packet".to_string())?;
                if message.topic_name != topic_name {
                    return Err("retained patch topic does not match packet".to_string());
                }
                message.expires_at_ms = expires_at_ms;
                Some(message)
            } else {
                None
            };
            StoragePatch::Retained(RetainedPatch {
                topic_name,
                message,
            })
        }
        _ => return Err("unknown PBIN2 frame tag".to_string()),
    };
    reader.finish()?;
    patch.validate().map_err(str::to_string)?;
    Ok(patch)
}

fn decode_client_patch(reader: &mut Reader<'_>) -> Result<ClientPatch, String> {
    let version = reader.u8()?;
    if version != CLIENT_PATCH_VERSION {
        return Err("unsupported client patch version".to_string());
    }
    let mode = match reader.u8()? {
        1 => ClientPatchMode::Merge,
        2 => ClientPatchMode::Reset,
        3 => ClientPatchMode::Delete,
        _ => return Err("unknown client patch mode".to_string()),
    };
    let client_id = reader.string()?;
    let session = reader.bool()?.then(|| decode_session(reader)).transpose()?;

    let mut subscription_upserts = Vec::new();
    let mut upsert_filters = BTreeSet::new();
    for _ in 0..reader.count()? {
        let subscription = decode_subscription(reader, &client_id)?;
        if !upsert_filters.insert(subscription.filter.clone()) {
            return Err("duplicate subscription upsert".to_string());
        }
        subscription_upserts.push(subscription);
    }
    let mut subscription_deletes = Vec::new();
    let mut delete_filters = BTreeSet::new();
    for _ in 0..reader.count()? {
        let filter = reader.string()?;
        if !delete_filters.insert(filter.clone()) {
            return Err("duplicate subscription delete".to_string());
        }
        subscription_deletes.push(filter);
    }
    if !upsert_filters.is_disjoint(&delete_filters) {
        return Err("subscription is both upserted and deleted".to_string());
    }

    let offline_remove_through = reader.opt_u64()?;
    let mut offline_append = Vec::new();
    let mut offline_sequences = BTreeSet::new();
    for _ in 0..reader.count()? {
        let sequence = reader.u64()?;
        if !offline_sequences.insert(sequence) {
            return Err("duplicate offline sequence".to_string());
        }
        offline_append.push(QueuedSnapshot {
            sequence,
            pending: decode_offline_pending(reader)?,
        });
    }
    let qos1_upserts = decode_pending_map(reader)?;
    let qos1_deletes = decode_u16_set(reader)?;
    let qos2_publish_upserts = decode_pending_map(reader)?;
    let qos2_publish_deletes = decode_u16_set(reader)?;
    let pubrel_add = decode_u16_set(reader)?;
    let pubrel_remove = decode_u16_set(reader)?;
    if !qos1_upserts.keys().all(|key| !qos1_deletes.contains(key))
        || !qos2_publish_upserts
            .keys()
            .all(|key| !qos2_publish_deletes.contains(key))
        || !pubrel_add.is_disjoint(&pubrel_remove)
    {
        return Err("client patch has conflicting operations".to_string());
    }

    let patch = ClientPatch {
        version,
        client_id,
        mode,
        session,
        subscription_upserts,
        subscription_deletes,
        offline_remove_through,
        offline_append,
        qos1_upserts,
        qos1_deletes,
        qos2_publish_upserts,
        qos2_publish_deletes,
        pubrel_add,
        pubrel_remove,
    };
    patch.validate().map_err(str::to_string)?;
    Ok(patch)
}

fn encode_session(writer: &mut Writer, session: &SessionSnapshot) {
    writer.u32(session.session_expiry_interval);
    writer.opt_u64(session.expires_at_ms);
    writer.u16(session.next_packet_id);
    writer.u64(session.next_offline_sequence);
}

fn decode_session(reader: &mut Reader<'_>) -> Result<SessionSnapshot, String> {
    Ok(SessionSnapshot {
        session_expiry_interval: reader.u32()?,
        expires_at_ms: reader.opt_u64()?,
        next_packet_id: reader.u16()?,
        next_offline_sequence: reader.u64()?,
    })
}

fn encode_subscription(writer: &mut Writer, subscription: &SubscriptionSnapshot) {
    writer.string(&subscription.filter);
    writer.string(&subscription.match_filter);
    writer.opt_string(subscription.shared_group.as_deref());
    writer.u8(qos_to_u8(subscription.maximum_qos));
    writer.bool(subscription.no_local);
    writer.bool(subscription.retain_as_published);
    writer.u8(subscription.retain_handling);
    writer.opt_u32(subscription.subscription_identifier);
}

fn decode_subscription(
    reader: &mut Reader<'_>,
    client_id: &str,
) -> Result<SubscriptionSnapshot, String> {
    Ok(SubscriptionSnapshot {
        client_id: client_id.to_string(),
        filter: reader.string()?,
        match_filter: reader.string()?,
        shared_group: reader.opt_string()?,
        maximum_qos: qos_from_u8(reader.u8()?)?,
        no_local: reader.bool()?,
        retain_as_published: reader.bool()?,
        retain_handling: reader.u8()?,
        subscription_identifier: reader.opt_u32()?,
    })
}

fn encode_pending(writer: &mut Writer, pending: &PendingSnapshot) {
    writer.opt_u64(pending.expires_at_ms);
    writer.bytes(&encode_publish(&pending.packet));
}

fn decode_pending(reader: &mut Reader<'_>) -> Result<PendingSnapshot, String> {
    let expires_at_ms = reader.opt_u64()?;
    let packet = decode_publish(reader.bytes()?)
        .ok_or_else(|| "invalid outbound MQTT packet".to_string())?;
    Ok(PendingSnapshot {
        packet,
        expires_at_ms,
    })
}

fn decode_offline_pending(reader: &mut Reader<'_>) -> Result<PendingSnapshot, String> {
    let mut pending = decode_pending(reader)?;
    pending.packet.packet_id = None;
    Ok(pending)
}

fn encode_pending_map(writer: &mut Writer, values: &BTreeMap<u16, PendingSnapshot>) {
    writer.len(values.len());
    for (packet_id, pending) in values {
        writer.u16(*packet_id);
        encode_pending(writer, pending);
    }
}

fn decode_pending_map(reader: &mut Reader<'_>) -> Result<BTreeMap<u16, PendingSnapshot>, String> {
    let mut values = BTreeMap::new();
    for _ in 0..reader.count()? {
        let packet_id = reader.u16()?;
        if values.insert(packet_id, decode_pending(reader)?).is_some() {
            return Err("duplicate packet identifier".to_string());
        }
    }
    Ok(values)
}

fn encode_u16_set(writer: &mut Writer, values: &BTreeSet<u16>) {
    writer.len(values.len());
    for value in values {
        writer.u16(*value);
    }
}

fn decode_u16_set(reader: &mut Reader<'_>) -> Result<BTreeSet<u16>, String> {
    let mut values = BTreeSet::new();
    for _ in 0..reader.count()? {
        if !values.insert(reader.u16()?) {
            return Err("duplicate packet identifier".to_string());
        }
    }
    Ok(values)
}

#[derive(Clone)]
enum V1Record {
    SessionUpsert {
        client_id: String,
        session: SessionSnapshot,
    },
    SessionDelete {
        client_id: String,
    },
    SubscriptionUpsert(SubscriptionSnapshot),
    SubscriptionDelete {
        client_id: String,
        filter: String,
    },
    RetainedUpsert {
        topic_name: String,
        message: RetainedMessage,
    },
    RetainedDelete {
        topic_name: String,
    },
    OfflineReplace {
        client_id: String,
        queue: Vec<PendingSnapshot>,
    },
    OutboundReplace {
        client_id: String,
        outbound: V1OutboundSnapshot,
    },
}

#[derive(Clone, Default)]
struct V1OutboundSnapshot {
    qos1: HashMap<u16, PendingSnapshot>,
    qos2_publish: HashMap<u16, PendingSnapshot>,
    qos2_pubrel: HashSet<u16>,
}

fn replay_v1_log(path: &Path, mode: ReplayMode) -> io::Result<BrokerState> {
    let mut state = BrokerState::default();
    replay_v1_into(path, &mut state, mode)?;
    Ok(state)
}

fn replay_v1_into(path: &Path, state: &mut BrokerState, mode: ReplayMode) -> io::Result<()> {
    let mut reader = BufReader::new(File::open(path)?);
    read_header(&mut reader, V1_MAGIC, "PBIN1")?;
    loop {
        let frame = match read_frame(&mut reader, mode, "PBIN1")? {
            FrameRead::Complete(frame) => frame,
            FrameRead::End | FrameRead::Truncated => return Ok(()),
        };
        if crc32(&frame.payload) != frame.checksum {
            return Err(invalid_data("PBIN1 frame checksum mismatch"));
        }
        let record = decode_v1_record(&frame.payload).map_err(invalid_data)?;
        apply_v1_record(state, record);
    }
}

fn apply_v1_record(state: &mut BrokerState, record: V1Record) {
    match record {
        V1Record::SessionUpsert { client_id, session } => {
            let entry = state
                .sessions_by_client_id
                .entry(client_id)
                .or_insert_with(|| {
                    SessionEntry::disconnected(
                        session.session_expiry_interval,
                        session.expires_at_ms,
                    )
                });
            entry.session_expiry_interval = session.session_expiry_interval;
            entry.expires_at_ms = session.expires_at_ms;
            entry.next_packet_id = session.next_packet_id;
        }
        V1Record::SessionDelete { client_id } => {
            state.sessions_by_client_id.remove(&client_id);
            state
                .subscriptions
                .retain(|subscription| subscription.client_id != client_id);
        }
        V1Record::SubscriptionUpsert(subscription) => {
            let subscription = subscription.into_subscription();
            if let Some(existing) = state.subscriptions.iter_mut().find(|existing| {
                existing.client_id == subscription.client_id
                    && existing.filter == subscription.filter
            }) {
                *existing = subscription;
            } else {
                state.subscriptions.push(subscription);
            }
        }
        V1Record::SubscriptionDelete { client_id, filter } => {
            state.subscriptions.retain(|subscription| {
                !(subscription.client_id == client_id && subscription.filter == filter)
            });
        }
        V1Record::RetainedUpsert {
            topic_name,
            message,
        } => {
            state.retained.insert(topic_name, message);
        }
        V1Record::RetainedDelete { topic_name } => {
            state.retained.remove(&topic_name);
        }
        V1Record::OfflineReplace { client_id, queue } => {
            if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
                session.offline_queue = queue
                    .into_iter()
                    .enumerate()
                    .map(|(sequence, pending)| QueuedPublish {
                        sequence: sequence as u64,
                        pending: pending.into_pending(),
                    })
                    .collect::<VecDeque<_>>();
                session.next_offline_sequence = session.offline_queue.len() as u64;
            }
        }
        V1Record::OutboundReplace {
            client_id,
            outbound,
        } => {
            if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
                session.outbound_qos1 = outbound
                    .qos1
                    .into_iter()
                    .map(|(packet_id, pending)| (packet_id, pending.into_pending()))
                    .collect();
                session.outbound_qos2_publish = outbound
                    .qos2_publish
                    .into_iter()
                    .map(|(packet_id, pending)| (packet_id, pending.into_pending()))
                    .collect();
                session.outbound_qos2_pubrel = outbound.qos2_pubrel;
            }
        }
    }
}

fn decode_v1_record(payload: &[u8]) -> Result<V1Record, String> {
    let mut reader = Reader::new(payload);
    let record = match reader.u8()? {
        V1_SESSION_UPSERT => V1Record::SessionUpsert {
            client_id: reader.string()?,
            session: SessionSnapshot {
                session_expiry_interval: reader.u32()?,
                expires_at_ms: reader.opt_u64()?,
                next_packet_id: reader.u16()?,
                next_offline_sequence: 0,
            },
        },
        V1_SESSION_DELETE => V1Record::SessionDelete {
            client_id: reader.string()?,
        },
        V1_SUBSCRIPTION_UPSERT => V1Record::SubscriptionUpsert(SubscriptionSnapshot {
            client_id: reader.string()?,
            filter: reader.string()?,
            match_filter: reader.string()?,
            shared_group: reader.opt_string()?,
            maximum_qos: qos_from_u8(reader.u8()?)?,
            no_local: reader.bool()?,
            retain_as_published: reader.bool()?,
            retain_handling: reader.u8()?,
            subscription_identifier: reader.opt_u32()?,
        }),
        V1_SUBSCRIPTION_DELETE => V1Record::SubscriptionDelete {
            client_id: reader.string()?,
            filter: reader.string()?,
        },
        V1_RETAINED_UPSERT => {
            let topic_name = reader.string()?;
            let expires_at_ms = reader.opt_u64()?;
            let mut message = decode_retained(reader.bytes()?)
                .ok_or_else(|| "invalid PBIN1 retained packet".to_string())?;
            message.expires_at_ms = expires_at_ms;
            V1Record::RetainedUpsert {
                topic_name,
                message,
            }
        }
        V1_RETAINED_DELETE => V1Record::RetainedDelete {
            topic_name: reader.string()?,
        },
        V1_OFFLINE_REPLACE => {
            let client_id = reader.string()?;
            let mut queue = Vec::new();
            for _ in 0..reader.count()? {
                queue.push(decode_offline_pending(&mut reader)?);
            }
            V1Record::OfflineReplace { client_id, queue }
        }
        V1_OUTBOUND_REPLACE => {
            let client_id = reader.string()?;
            let mut outbound = V1OutboundSnapshot::default();
            for _ in 0..reader.count()? {
                let packet_id = reader.u16()?;
                if outbound
                    .qos1
                    .insert(packet_id, decode_pending(&mut reader)?)
                    .is_some()
                {
                    return Err("duplicate PBIN1 QoS1 packet identifier".to_string());
                }
            }
            for _ in 0..reader.count()? {
                let packet_id = reader.u16()?;
                if outbound
                    .qos2_publish
                    .insert(packet_id, decode_pending(&mut reader)?)
                    .is_some()
                {
                    return Err("duplicate PBIN1 QoS2 packet identifier".to_string());
                }
            }
            for _ in 0..reader.count()? {
                if !outbound.qos2_pubrel.insert(reader.u16()?) {
                    return Err("duplicate PBIN1 PUBREL packet identifier".to_string());
                }
            }
            V1Record::OutboundReplace {
                client_id,
                outbound,
            }
        }
        _ => return Err("unknown PBIN1 record tag".to_string()),
    };
    reader.finish()?;
    Ok(record)
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn len(&mut self, value: usize) {
        self.u32(u32::try_from(value).expect("PBIN2 collection length"));
    }

    fn opt_u32(&mut self, value: Option<u32>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.u32(value);
        }
    }

    fn opt_u64(&mut self, value: Option<u64>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.u64(value);
        }
    }

    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn opt_string(&mut self, value: Option<&str>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.string(value);
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        self.len(value.len());
        self.bytes.extend_from_slice(value);
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }

    fn finish(&self) -> Result<(), String> {
        if self.index == self.bytes.len() {
            Ok(())
        } else {
            Err("PBIN record contains trailing bytes".to_string())
        }
    }

    fn count(&mut self) -> Result<usize, String> {
        let count = self.u32()? as usize;
        if count > self.bytes.len().saturating_sub(self.index) {
            return Err("PBIN collection count exceeds frame".to_string());
        }
        Ok(count)
    }

    fn u8(&mut self) -> Result<u8, String> {
        let value = *self
            .bytes
            .get(self.index)
            .ok_or_else(|| "unexpected end of PBIN record".to_string())?;
        self.index += 1;
        Ok(value)
    }

    fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid PBIN boolean".to_string()),
        }
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    fn opt_u32(&mut self) -> Result<Option<u32>, String> {
        self.bool()?.then(|| self.u32()).transpose()
    }

    fn opt_u64(&mut self) -> Result<Option<u64>, String> {
        self.bool()?.then(|| self.u64()).transpose()
    }

    fn string(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| "PBIN string is not UTF-8".to_string())
    }

    fn opt_string(&mut self) -> Result<Option<String>, String> {
        self.bool()?.then(|| self.string()).transpose()
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "unexpected end of PBIN record".to_string())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .index
            .checked_add(len)
            .ok_or_else(|| "PBIN record length overflow".to_string())?;
        let bytes = self
            .bytes
            .get(self.index..end)
            .ok_or_else(|| "unexpected end of PBIN record".to_string())?;
        self.index = end;
        Ok(bytes)
    }
}

fn encode_retained(message: &RetainedMessage) -> Vec<u8> {
    let packet_id = (message.qos != QoS::AtMostOnce).then_some(1);
    encode_publish(&PublishPacket {
        dup: false,
        qos: message.qos,
        retain: true,
        topic_name: message.topic_name.clone(),
        packet_id,
        properties: message.properties.clone(),
        payload: message.payload.clone(),
    })
}

fn decode_retained(packet: &[u8]) -> Option<RetainedMessage> {
    let packet = decode_publish(packet)?;
    Some(RetainedMessage::new(
        packet.qos,
        packet.topic_name,
        packet.properties,
        Bytes::copy_from_slice(&packet.payload),
        None,
    ))
}

pub(crate) fn encode_publish(packet: &PublishPacket) -> Vec<u8> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    let mut packet = packet.clone();
    if packet.qos != QoS::AtMostOnce && packet.packet_id.is_none() {
        packet.packet_id = Some(1);
    }
    codec
        .encode(MqttPacket::Publish(packet), &mut buffer)
        .expect("encode publish");
    buffer.to_vec()
}

pub(crate) fn decode_publish(packet: &[u8]) -> Option<PublishPacket> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::from(packet);
    let packet = codec.decode(&mut buffer).ok().flatten()?;
    if !buffer.is_empty() {
        return None;
    }
    let MqttPacket::Publish(packet) = packet else {
        return None;
    };
    Some(packet)
}

fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn qos_from_u8(value: u8) -> Result<QoS, String> {
    match value {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        _ => Err("invalid PBIN QoS".to_string()),
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::runtime::{message::PendingPublish, subscription_tree::SubscriptionEntry};
    use rs_netty::codec::SubscriptionOptions;

    const V1_LEGACY_FIXTURE_HEX: &str = concat!(
        "5042494e310a11000000010500000076616c69643c000000000700a5ae0521",
        "26000000030500000076616c69640700000076616c69642f23070000007661",
        "6c69642f230001000000006e0adb4326000000030500000067686f73740700",
        "000067686f73742f230700000067686f73742f230001000000004127e9dc33",
        "000000080500000076616c696401000000040000160000003214000b76616c",
        "69642f746f7069630004006c6976650000000000000000ff9f0f4735000000",
        "080500000067686f737401000000010000180000003216000b67686f73742f",
        "746f7069630001006f727068616e000000000000000030ba648f2b00000007",
        "0500000067686f73740100000000180000003216000b67686f73742f746f70",
        "69630001006f727068616e6844bc0e",
    );

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("pulse-pbin2-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn publish(topic_name: &str, payload: &'static [u8], qos: QoS) -> PublishPacket {
        PublishPacket {
            dup: false,
            qos,
            retain: false,
            topic_name: topic_name.to_string(),
            packet_id: None,
            properties: Vec::new(),
            payload: Bytes::from_static(payload),
        }
    }

    fn fixture_bytes() -> Vec<u8> {
        V1_LEGACY_FIXTURE_HEX
            .as_bytes()
            .chunks_exact(2)
            .map(|digits| u8::from_str_radix(std::str::from_utf8(digits).unwrap(), 16).unwrap())
            .collect()
    }

    fn state_with_qos1(count: u16) -> BrokerState {
        let mut state = BrokerState::default();
        let mut session = SessionEntry::disconnected(60, Some(123));
        for packet_id in 1..=count {
            session.outbound_qos1.insert(
                packet_id,
                PendingPublish {
                    packet: PublishPacket {
                        packet_id: Some(packet_id),
                        ..publish("devices/qos1", b"payload", QoS::AtLeastOnce)
                    },
                    expires_at_ms: None,
                },
            );
        }
        state
            .sessions_by_client_id
            .insert("client".to_string(), session);
        state
    }

    fn seed_persistent_state(state: &mut BrokerState) {
        state.sessions_by_client_id.insert(
            "client".to_string(),
            SessionEntry::disconnected(60, Some(u64::MAX)),
        );
        let session = state.sessions_by_client_id.get_mut("client").unwrap();
        session.next_packet_id = 7;
        session.next_offline_sequence = 4;
        session.offline_queue.push_back(QueuedPublish {
            sequence: 3,
            pending: PendingPublish {
                packet: publish("devices/offline", b"offline", QoS::AtLeastOnce),
                expires_at_ms: Some(456),
            },
        });
        session.outbound_qos1.insert(
            4,
            PendingPublish {
                packet: PublishPacket {
                    packet_id: Some(4),
                    ..publish("devices/inflight", b"inflight", QoS::AtLeastOnce)
                },
                expires_at_ms: Some(789),
            },
        );
        session.outbound_qos2_pubrel.insert(9);
        state.subscriptions.push(SubscriptionEntry {
            client_id: "client".to_string(),
            filter: "devices/#".to_string(),
            match_filter: "devices/#".to_string(),
            shared_group: None,
            options: SubscriptionOptions {
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            subscription_identifier: Some(11),
        });
        state.retained.insert(
            "devices/retained".to_string(),
            RetainedMessage::new(
                QoS::AtMostOnce,
                "devices/retained".to_string(),
                Vec::new(),
                Bytes::from_static(b"retained"),
                Some(999),
            ),
        );
        state.mark_client_reset("client");
        state.mark_retained_changed("devices/retained");
    }

    #[test]
    fn recovers_pbin2_state() {
        let dir = temp_dir("recover");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            storage.with_state(&mut seed_persistent_state);
        }
        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        storage.read_state(&mut |state| {
            let session = state.sessions_by_client_id.get("client").unwrap();
            assert_eq!(session.next_packet_id, 7);
            assert_eq!(session.next_offline_sequence, 4);
            assert_eq!(session.offline_queue.front().unwrap().sequence, 3);
            assert!(session.outbound_qos1.contains_key(&4));
            assert!(session.outbound_qos2_pubrel.contains(&9));
            assert_eq!(state.subscriptions.len(), 1);
            assert!(state.retained.contains_key("devices/retained"));
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn v2_unknown_tag_and_complete_crc_failure_are_rejected() {
        let dir = temp_dir("strict-corruption");
        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        drop(storage);
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let wal = dir.join(manifest.active_log);

        let payload = [99u8];
        let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
        file.write_all(&(payload.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&payload).unwrap();
        file.write_all(&crc32(&payload).to_le_bytes()).unwrap();
        drop(file);
        assert!(BinaryStorage::open(&dir, CommitPolicy::Strict).is_err());

        let _ = fs::remove_dir_all(&dir);
        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        drop(storage);
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let wal = dir.join(manifest.active_log);
        let patch = StoragePatch::Retained(RetainedPatch {
            topic_name: "missing".to_string(),
            message: None,
        });
        let payload = encode_patch(&patch);
        let mut file = OpenOptions::new().append(true).open(&wal).unwrap();
        file.write_all(&(payload.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&payload).unwrap();
        file.write_all(&0u32.to_le_bytes()).unwrap();
        drop(file);
        assert!(BinaryStorage::open(&dir, CommitPolicy::Strict).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pbin2_rejects_trailing_bytes_in_nested_mqtt_packet() {
        let message = RetainedMessage::new(
            QoS::AtMostOnce,
            "devices/trailing".to_string(),
            Vec::new(),
            Bytes::from_static(b"payload"),
            None,
        );
        let mut mqtt_packet = encode_retained(&message);
        mqtt_packet.push(0);

        let mut writer = Writer::default();
        writer.u8(RETAINED_PATCH);
        writer.string("devices/trailing");
        writer.bool(true);
        writer.opt_u64(None);
        writer.bytes(&mqtt_packet);

        assert!(decode_patch(&writer.into_inner()).is_err());
    }

    #[test]
    fn active_wal_allows_only_physical_tail() {
        let dir = temp_dir("physical-tail");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            storage.with_state(&mut seed_persistent_state);
        }
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let wal = dir.join(manifest.active_log);
        OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(&[12, 0, 0])
            .unwrap();
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            storage.with_state(&mut |state| {
                state.retained.insert(
                    "after/tail".to_string(),
                    RetainedMessage::new(
                        QoS::AtMostOnce,
                        "after/tail".to_string(),
                        Vec::new(),
                        Bytes::from_static(b"ok"),
                        None,
                    ),
                );
                state.mark_retained_changed("after/tail");
            });
        }
        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        storage.read_state(&mut |state| {
            assert!(state.retained.contains_key("after/tail"));
            assert!(state.sessions_by_client_id.contains_key("client"));
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn active_wal_accepts_every_final_frame_truncation_but_rejects_middle_corruption() {
        let dir = temp_dir("active-truncation-matrix");
        fs::create_dir_all(&dir).unwrap();
        let complete_path = dir.join("complete.wal");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&complete_path)
            .unwrap();
        file.write_all(V2_MAGIC).unwrap();
        let first = StoragePatch::Retained(RetainedPatch {
            topic_name: "matrix/first".to_string(),
            message: Some(RetainedMessage::new(
                QoS::AtMostOnce,
                "matrix/first".to_string(),
                Vec::new(),
                Bytes::from_static(b"first"),
                None,
            )),
        });
        let second = StoragePatch::Retained(RetainedPatch {
            topic_name: "matrix/second".to_string(),
            message: Some(RetainedMessage::new(
                QoS::AtMostOnce,
                "matrix/second".to_string(),
                Vec::new(),
                Bytes::from_static(b"second"),
                None,
            )),
        });
        let first_len = write_patches(&mut file, std::slice::from_ref(&first)).unwrap();
        write_patches(&mut file, std::slice::from_ref(&second)).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let complete = fs::read(&complete_path).unwrap();
        let first_end = V2_MAGIC.len() + first_len as usize;
        let truncated_path = dir.join("truncated.wal");
        for cut in 0..V2_MAGIC.len() {
            fs::write(&truncated_path, &complete[..cut]).unwrap();
            assert!(
                replay_v2(&truncated_path, ReplayMode::AllowPhysicalTail).is_err(),
                "active WAL accepted an incomplete header at byte {cut}"
            );
        }
        for cut in V2_MAGIC.len()..complete.len() {
            fs::write(&truncated_path, &complete[..cut]).unwrap();
            let projection = replay_v2(&truncated_path, ReplayMode::AllowPhysicalTail)
                .unwrap_or_else(|error| panic!("active WAL truncation at {cut}: {error}"));
            assert_eq!(
                projection.retained.contains_key("matrix/first"),
                cut >= first_end
            );
            assert!(!projection.retained.contains_key("matrix/second"));
        }

        let mut corrupted = complete;
        corrupted[V2_MAGIC.len() + 4] ^= 0x7f;
        fs::write(&truncated_path, corrupted).unwrap();
        assert!(replay_v2(&truncated_path, ReplayMode::AllowPhysicalTail).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pbin2_rejects_unknown_client_patch_version_with_valid_crc() {
        let dir = temp_dir("unknown-patch-version");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("unknown-version.wal");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(V2_MAGIC).unwrap();
        let mut payload = encode_patch(&StoragePatch::Client(ClientPatch::delete(
            "client".to_string(),
        )));
        payload[1] = CLIENT_PATCH_VERSION + 1;
        write_frame(&mut file, &payload).unwrap();
        file.sync_all().unwrap();
        drop(file);

        assert!(replay_v2(&path, ReplayMode::AllowPhysicalTail).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_uses_generation_scoped_files() {
        let dir = temp_dir("compact");
        let storage = BinaryStorage::open_with_options(
            &dir,
            CommitPolicy::Strict,
            WalCompactConfig {
                max_bytes: V2_MAGIC.len() as u64,
                interval_ms: 0,
            },
        )
        .unwrap();
        storage.with_state(&mut seed_persistent_state);
        drop(storage);
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(manifest.version, 2);
        assert!(manifest.checkpoint.starts_with(V2_CHECKPOINT_PREFIX));
        assert!(manifest.active_log.starts_with(V2_WAL_PREFIX));
        assert!(dir.join(&manifest.checkpoint).is_file());
        assert!(dir.join(&manifest.active_log).is_file());
        assert!(!dir.join(LEGACY_LOG_FILE_NAME).exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn qos1_ack_patch_size_does_not_depend_on_remaining_inflight() {
        let patch_len = |count: u16| {
            let mut state = state_with_qos1(count);
            let projection = PersistentProjection::from_state(&state);
            state
                .sessions_by_client_id
                .get_mut("client")
                .unwrap()
                .outbound_qos1
                .remove(&1);
            let patches = prepare_patches(
                &projection,
                &state,
                &[
                    crate::broker::runtime::session_registry::PersistenceChange::ClientChanged(
                        "client".to_string(),
                    ),
                ],
            );
            assert_eq!(patches.len(), 1);
            encode_patch(&patches[0]).len()
        };
        let one_remaining = patch_len(2);
        let thousand_remaining = patch_len(1_001);
        assert_eq!(one_remaining, thousand_remaining);
        assert!(one_remaining < 64);
    }

    #[test]
    fn offline_queue_drain_writes_linear_patch_bytes() {
        let drain_bytes =
            |count: u64| {
                let mut state = BrokerState::default();
                let mut session = SessionEntry::disconnected(60, Some(u64::MAX));
                session.next_offline_sequence = count;
                for sequence in 0..count {
                    session.offline_queue.push_back(QueuedPublish {
                        sequence,
                        pending: PendingPublish {
                            packet: publish("devices/offline", b"payload", QoS::AtLeastOnce),
                            expires_at_ms: None,
                        },
                    });
                }
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), session);
                let mut projection = PersistentProjection::from_state(&state);
                let mut total_bytes = 0;
                let mut patch_bytes = None;

                for _ in 0..count {
                    state
                        .sessions_by_client_id
                        .get_mut("client")
                        .unwrap()
                        .offline_queue
                        .pop_front()
                        .unwrap();
                    let patches = prepare_patches(
                    &projection,
                    &state,
                    &[crate::broker::runtime::session_registry::PersistenceChange::ClientChanged(
                        "client".to_string(),
                    )],
                );
                    assert_eq!(patches.len(), 1);
                    let encoded_len = encode_patch(&patches[0]).len();
                    assert_eq!(*patch_bytes.get_or_insert(encoded_len), encoded_len);
                    total_bytes += encoded_len;
                    projection.apply_patch(&patches[0]).unwrap();
                }
                (total_bytes, patch_bytes.unwrap())
            };

        let (one_hundred_total, patch_bytes) = drain_bytes(100);
        let (one_thousand_total, _) = drain_bytes(1_000);
        assert_eq!(one_hundred_total, patch_bytes * 100);
        assert_eq!(one_thousand_total, patch_bytes * 1_000);
        assert!(patch_bytes < 64);
    }

    #[test]
    fn client_frames_are_sorted_by_client_id() {
        let mut state = BrokerState::default();
        state
            .sessions_by_client_id
            .insert("zeta".to_string(), SessionEntry::disconnected(60, Some(1)));
        state
            .sessions_by_client_id
            .insert("alpha".to_string(), SessionEntry::disconnected(60, Some(1)));
        let patches = prepare_patches(
            &PersistentProjection::default(),
            &state,
            &[
                crate::broker::runtime::session_registry::PersistenceChange::ClientReset(
                    "zeta".to_string(),
                ),
                crate::broker::runtime::session_registry::PersistenceChange::ClientReset(
                    "alpha".to_string(),
                ),
            ],
        );
        let client_ids = patches
            .iter()
            .map(|patch| match patch {
                StoragePatch::Client(patch) => patch.client_id.as_str(),
                StoragePatch::Retained(_) => panic!("unexpected retained patch"),
            })
            .collect::<Vec<_>>();
        assert_eq!(client_ids, ["alpha", "zeta"]);
    }

    #[test]
    fn checkpoint_rejects_every_truncation() {
        let dir = temp_dir("checkpoint-truncation");
        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: V2_MAGIC.len() as u64,
                    interval_ms: 0,
                },
            )
            .unwrap();
            storage.with_state(&mut seed_persistent_state);
        }
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let checkpoint = fs::read(dir.join(manifest.checkpoint)).unwrap();
        let cut_path = dir.join("truncated-checkpoint");
        for cut in 0..checkpoint.len() {
            fs::write(&cut_path, &checkpoint[..cut]).unwrap();
            assert!(
                replay_v2(&cut_path, ReplayMode::Strict).is_err(),
                "checkpoint truncation at byte {cut} was accepted"
            );
        }
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn migrates_fixed_pbin1_legacy_and_drops_orphans() {
        let dir = temp_dir("v1-legacy-migration");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(LEGACY_LOG_FILE_NAME), fixture_bytes()).unwrap();

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        storage.read_state(&mut |state| {
            let session = state.sessions_by_client_id.get("valid").unwrap();
            assert!(session.outbound_qos1.contains_key(&4));
            assert_eq!(state.subscriptions.len(), 1);
            assert_eq!(state.subscriptions[0].client_id, "valid");
            assert!(!state.sessions_by_client_id.contains_key("ghost"));
        });
        drop(storage);

        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        assert_eq!(manifest.version, 2);
        assert!(!dir.join(LEGACY_LOG_FILE_NAME).exists());
        let projection = replay_v2(&dir.join(manifest.checkpoint), ReplayMode::Strict).unwrap();
        assert_eq!(projection.clients.len(), 1);
        assert!(projection.clients.contains_key("valid"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn migrates_large_pbin1_orphan_outbound_without_checkpointing_payload() {
        const ORPHAN_QOS1_COUNT: u16 = 22_566;

        let dir = temp_dir("v1-large-orphan-migration");
        fs::create_dir_all(&dir).unwrap();
        let legacy_path = dir.join(LEGACY_LOG_FILE_NAME);
        let encoded_packet = encode_publish(&PublishPacket {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic_name: "orphan/qos1".to_string(),
            packet_id: Some(1),
            properties: Vec::new(),
            payload: Bytes::from(vec![b'x'; 905]),
        });
        let mut payload = Writer::default();
        payload.u8(V1_OUTBOUND_REPLACE);
        payload.string("orphan");
        payload.len(usize::from(ORPHAN_QOS1_COUNT));
        for packet_id in 1..=ORPHAN_QOS1_COUNT {
            payload.u16(packet_id);
            payload.opt_u64(None);
            payload.bytes(&encoded_packet);
        }
        payload.len(0);
        payload.len(0);
        let payload = payload.into_inner();
        let mut legacy = File::create(&legacy_path).unwrap();
        legacy.write_all(V1_MAGIC).unwrap();
        write_frame(&mut legacy, &payload).unwrap();
        legacy.sync_all().unwrap();
        drop(legacy);
        assert!(fs::metadata(&legacy_path).unwrap().len() > 20_000_000);

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.is_empty());
        });
        drop(storage);

        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let checkpoint_path = dir.join(manifest.checkpoint);
        let projection = replay_v2(&checkpoint_path, ReplayMode::Strict).unwrap();
        assert!(projection.clients.is_empty());
        assert!(fs::metadata(checkpoint_path).unwrap().len() < 64);
        assert!(!legacy_path.exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn migrates_fixed_pbin1_manifest_layout() {
        let dir = temp_dir("v1-manifest-migration");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(LEGACY_CHECKPOINT_FILE_NAME), fixture_bytes()).unwrap();
        fs::write(dir.join(legacy_wal_file_name(5)), V1_MAGIC).unwrap();
        fs::write(
            dir.join(MANIFEST_FILE_NAME),
            concat!(
                "version=1\n",
                "checkpoint=broker.checkpoint\n",
                "active_log=broker.binlog.5\n",
                "active_epoch=5\n"
            ),
        )
        .unwrap();

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.contains_key("valid"));
            assert!(!state.sessions_by_client_id.contains_key("ghost"));
        });
        drop(storage);
        assert_eq!(
            read_manifest(&dir.join(MANIFEST_FILE_NAME))
                .unwrap()
                .version,
            2
        );
        assert!(!dir.join(LEGACY_CHECKPOINT_FILE_NAME).exists());
        assert!(!dir.join(legacy_wal_file_name(5)).exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn transient_client_is_absent_from_compacted_checkpoint() {
        let dir = temp_dir("transient-checkpoint");
        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: V2_MAGIC.len() as u64,
                    interval_ms: 0,
                },
            )
            .unwrap();
            storage.with_state(&mut |state| {
                let mut session = SessionEntry::disconnected(0, None);
                for packet_id in 1..=1_000 {
                    session.outbound_qos1.insert(
                        packet_id,
                        PendingPublish {
                            packet: PublishPacket {
                                packet_id: Some(packet_id),
                                ..publish("transient", b"payload", QoS::AtLeastOnce)
                            },
                            expires_at_ms: None,
                        },
                    );
                }
                state
                    .sessions_by_client_id
                    .insert("transient".to_string(), session);
                state.mark_client_reset("transient");
            });
        }
        let manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();
        let projection = replay_v2(&dir.join(manifest.checkpoint), ReplayMode::Strict).unwrap();
        assert!(!projection.clients.contains_key("transient"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn recovered_session_deadline_is_persisted_once() {
        let dir = temp_dir("persist-recovered-deadline");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("finite".to_string(), SessionEntry::disconnected(60, None));
                state.mark_client_reset("finite");
            });
        }

        let first_deadline = {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            let mut deadline = None;
            storage.read_state(&mut |state| {
                deadline = state.sessions_by_client_id["finite"].expires_at_ms;
            });
            deadline.expect("recovered deadline")
        };
        let first_manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();

        std::thread::sleep(Duration::from_millis(2));
        let second_deadline = {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).unwrap();
            let mut deadline = None;
            storage.read_state(&mut |state| {
                deadline = state.sessions_by_client_id["finite"].expires_at_ms;
            });
            deadline.expect("stable recovered deadline")
        };
        let second_manifest = read_manifest(&dir.join(MANIFEST_FILE_NAME)).unwrap();

        assert_eq!(second_deadline, first_deadline);
        assert_eq!(second_manifest.active_epoch, first_manifest.active_epoch);
        let _ = fs::remove_dir_all(dir);
    }
}
