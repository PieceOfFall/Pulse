use std::{
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use rs_netty::codec::{
    Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS, SubscriptionOptions,
};
use serde::Deserialize;

use super::BrokerStorage;
use crate::broker::runtime::{
    message::PendingPublish,
    retained_store::RetainedMessage,
    session_registry::{BrokerState, PersistenceChange, SessionEntry},
    subscription_tree::SubscriptionEntry,
};
use tracing::warn;

const MAGIC: &[u8] = b"PBIN1\n";
const LOG_FILE_NAME: &str = "broker.binlog";
const MANIFEST_FILE_NAME: &str = "broker.manifest";
const CHECKPOINT_FILE_NAME: &str = "broker.checkpoint";
const TMP_SUFFIX: &str = ".tmp";
const DEFAULT_WAL_COMPACT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_WAL_COMPACT_INTERVAL_MS: u64 = 10 * 60 * 1000;

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

const SESSION_UPSERT: u8 = 1;
const SESSION_DELETE: u8 = 2;
const SUBSCRIPTION_UPSERT: u8 = 3;
const SUBSCRIPTION_DELETE: u8 = 4;
const RETAINED_UPSERT: u8 = 5;
const RETAINED_DELETE: u8 = 6;
const OFFLINE_REPLACE: u8 = 7;
const OUTBOUND_REPLACE: u8 = 8;

pub(crate) struct BinaryStorage {
    inner: Mutex<BinaryStorageInner>,
    log: Mutex<BinaryLog>,
}

struct BinaryStorageInner {
    state: BrokerState,
    snapshot: PersistentSnapshot,
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
        let state = recovered.state;
        let snapshot = PersistentSnapshot::from_state(&state);
        let log = BinaryLog::open(
            dir.to_path_buf(),
            recovered.active_path,
            recovered.active_epoch,
            commit_policy,
            compact,
        )?;

        Ok(Self {
            inner: Mutex::new(BinaryStorageInner { state, snapshot }),
            log: Mutex::new(log),
        })
    }
}

impl BrokerStorage for BinaryStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut inner = self.inner.lock().expect("broker state lock poisoned");
        operation(&mut inner.state);

        let changes = inner.state.take_persistence_changes();
        if changes.is_empty() {
            return;
        }

        let BinaryStorageInner { state, snapshot } = &mut *inner;
        let records = diff_records_for_changes(snapshot, state, changes);
        if !records.is_empty() {
            let mut log = self.log.lock().expect("binary log lock poisoned");
            log.append_many(&records)
                .expect("persist broker state to binary log");
            log.compact_if_needed(snapshot)
                .expect("compact binary log checkpoint");
        }
    }

    fn with_transient_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut inner = self.inner.lock().expect("broker state lock poisoned");
        operation(&mut inner.state);
    }

    fn read_state(&self, operation: &mut dyn FnMut(&BrokerState)) {
        let inner = self.inner.lock().expect("broker state lock poisoned");
        operation(&inner.state);
    }
}

struct BinaryLog {
    dir: PathBuf,
    active_path: PathBuf,
    active_epoch: Option<u64>,
    file: Option<File>,
    commit_policy: CommitPolicy,
    compact: WalCompactConfig,
    current_bytes: u64,
    last_compacted_at: Instant,
    records_since_checkpoint: usize,
    pending_balanced_records: usize,
    fast_tx: Option<mpsc::Sender<Vec<Record>>>,
    fast_thread: Option<JoinHandle<io::Result<()>>>,
}

impl BinaryLog {
    fn open(
        dir: PathBuf,
        active_path: PathBuf,
        active_epoch: Option<u64>,
        commit_policy: CommitPolicy,
        compact: WalCompactConfig,
    ) -> io::Result<Self> {
        let file = open_wal_file(&active_path)?;
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

    fn append_many(&mut self, records: &[Record]) -> io::Result<()> {
        let encoded_bytes = encoded_records_len(records);
        if let Some(tx) = &self.fast_tx {
            tx.send(records.to_vec())
                .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "binary log writer stopped"))?;
            self.current_bytes += encoded_bytes;
            self.records_since_checkpoint += records.len();
            return Ok(());
        }

        let file = self.file.as_mut().expect("sync binary log file");
        self.current_bytes += write_records(file, records)?;
        self.records_since_checkpoint += records.len();
        self.pending_balanced_records += records.len();
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

    fn compact_if_needed(&mut self, snapshot: &PersistentSnapshot) -> io::Result<()> {
        let size_triggered =
            self.compact.max_bytes != 0 && self.current_bytes > self.compact.max_bytes;
        let time_triggered = self.compact.interval_ms != 0
            && self.records_since_checkpoint > 0
            && self.last_compacted_at.elapsed() >= Duration::from_millis(self.compact.interval_ms);
        if !size_triggered && !time_triggered {
            return Ok(());
        }

        self.compact(snapshot)
    }

    fn compact(&mut self, snapshot: &PersistentSnapshot) -> io::Result<()> {
        self.close_writer()?;

        let checkpoint_path = self.dir.join(CHECKPOINT_FILE_NAME);
        write_checkpoint(&checkpoint_path, snapshot)?;

        let next_epoch = self.active_epoch.unwrap_or(0) + 1;
        let next_path = wal_epoch_path(&self.dir, next_epoch);
        remove_file_if_exists(&next_path)?;
        let new_file = create_wal_file(&next_path)?;

        let manifest = WalManifest::new(next_epoch);
        write_manifest(&self.dir, &manifest)?;
        sync_dir(&self.dir)?;

        self.active_path = next_path;
        self.active_epoch = Some(next_epoch);
        self.current_bytes = MAGIC.len() as u64;
        self.records_since_checkpoint = 0;
        self.pending_balanced_records = 0;
        self.last_compacted_at = Instant::now();
        self.start_writer(new_file);
        cleanup_unreferenced(&self.dir, self.active_epoch)?;
        Ok(())
    }

    fn start_writer(&mut self, file: File) {
        if matches!(self.commit_policy, CommitPolicy::Fast) {
            let (tx, rx) = mpsc::channel::<Vec<Record>>();
            let fast_thread = thread::spawn(move || {
                let mut file = file;
                while let Ok(records) = rx.recv() {
                    write_records(&mut file, &records)?;
                }
                file.flush()?;
                file.sync_data()
            });
            self.file = None;
            self.fast_tx = Some(tx);
            self.fast_thread = Some(fast_thread);
            return;
        }

        self.file = Some(file);
        self.fast_tx = None;
        self.fast_thread = None;
    }

    fn close_writer(&mut self) -> io::Result<()> {
        self.fast_tx.take();
        if let Some(thread) = self.fast_thread.take() {
            thread.join().map_err(|_| {
                io::Error::new(ErrorKind::BrokenPipe, "binary log writer panicked")
            })??;
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

fn write_records(file: &mut File, records: &[Record]) -> io::Result<u64> {
    let mut written = 0;
    for record in records {
        let payload = encode_record(record);
        let checksum = crc32(&payload);
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&payload)?;
        file.write_all(&checksum.to_le_bytes())?;
        written += record_encoded_len(payload.len());
    }
    Ok(written)
}

fn encoded_records_len(records: &[Record]) -> u64 {
    records
        .iter()
        .map(|record| record_encoded_len(encode_record(record).len()))
        .sum()
}

fn record_encoded_len(payload_len: usize) -> u64 {
    (4 + payload_len + 4) as u64
}

struct RecoveredStorage {
    state: BrokerState,
    active_path: PathBuf,
    active_epoch: Option<u64>,
}

#[derive(Clone)]
struct WalManifest {
    checkpoint: String,
    active_log: String,
    active_epoch: u64,
}

impl WalManifest {
    fn new(active_epoch: u64) -> Self {
        Self {
            checkpoint: CHECKPOINT_FILE_NAME.to_string(),
            active_log: wal_epoch_file_name(active_epoch),
            active_epoch,
        }
    }

    fn encode(&self) -> String {
        format!(
            "version=1\ncheckpoint={}\nactive_log={}\nactive_epoch={}\n",
            self.checkpoint, self.active_log, self.active_epoch
        )
    }
}

fn recover_storage(dir: &Path) -> io::Result<RecoveredStorage> {
    let manifest_path = dir.join(MANIFEST_FILE_NAME);
    if manifest_path.exists() {
        match recover_from_manifest(dir, &manifest_path) {
            Ok(recovered) => {
                cleanup_unreferenced(dir, recovered.active_epoch)?;
                return Ok(recovered);
            }
            Err(error) => {
                let legacy_path = dir.join(LOG_FILE_NAME);
                if legacy_path.exists() {
                    let legacy_state = replay_log(&legacy_path).map_err(|legacy_error| {
                        io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "recover manifest WAL failed: {error}; legacy broker.binlog fallback also failed: {legacy_error}"
                            ),
                        )
                    })?;
                    warn!(
                        error = %error,
                        legacy_path = %legacy_path.display(),
                        "recover manifest WAL failed; falling back to legacy binary log"
                    );
                    cleanup_unreferenced(dir, None)?;
                    return Ok(RecoveredStorage {
                        state: legacy_state,
                        active_path: legacy_path,
                        active_epoch: None,
                    });
                }

                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "recover manifest WAL failed: {error}; no legacy broker.binlog fallback exists"
                    ),
                ));
            }
        }
    }

    cleanup_unreferenced(dir, None)?;
    let active_path = dir.join(LOG_FILE_NAME);
    Ok(RecoveredStorage {
        state: replay_log(&active_path)?,
        active_path,
        active_epoch: None,
    })
}

fn recover_from_manifest(dir: &Path, manifest_path: &Path) -> io::Result<RecoveredStorage> {
    let manifest = read_manifest(manifest_path)?;
    let checkpoint_path = manifest_file_path(dir, &manifest.checkpoint)?;
    let active_path = manifest_file_path(dir, &manifest.active_log)?;

    if !checkpoint_path.exists() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "manifest checkpoint is missing: {}",
                checkpoint_path.display()
            ),
        ));
    }
    if !active_path.exists() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("manifest active WAL is missing: {}", active_path.display()),
        ));
    }

    let expected_active = wal_epoch_file_name(manifest.active_epoch);
    if manifest.active_log != expected_active {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "manifest active_log {} does not match active_epoch {}",
                manifest.active_log, manifest.active_epoch
            ),
        ));
    }

    let mut state = replay_checkpoint(&checkpoint_path)?;
    replay_log_into(&active_path, &mut state, ReplayMode::AllowCorruptTail)?;
    Ok(RecoveredStorage {
        state,
        active_path,
        active_epoch: Some(manifest.active_epoch),
    })
}

fn read_manifest(path: &Path) -> io::Result<WalManifest> {
    let contents = fs::read_to_string(path)?;
    let mut version = None;
    let mut checkpoint = None;
    let mut active_log = None;
    let mut active_epoch = None;

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid manifest line: {line}"),
            ));
        };
        match key {
            "version" => {
                version = Some(value.parse::<u32>().map_err(|error| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        format!("invalid manifest version: {error}"),
                    )
                })?)
            }
            "checkpoint" => checkpoint = Some(value.to_string()),
            "active_log" => active_log = Some(value.to_string()),
            "active_epoch" => {
                active_epoch = Some(value.parse::<u64>().map_err(|error| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        format!("invalid manifest active_epoch: {error}"),
                    )
                })?)
            }
            _ => {}
        }
    }

    if version != Some(1) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "unsupported manifest version",
        ));
    }

    Ok(WalManifest {
        checkpoint: checkpoint
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "manifest missing checkpoint"))?,
        active_log: active_log
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "manifest missing active_log"))?,
        active_epoch: active_epoch.ok_or_else(|| {
            io::Error::new(ErrorKind::InvalidData, "manifest missing active_epoch")
        })?,
    })
}

fn write_checkpoint(path: &Path, snapshot: &PersistentSnapshot) -> io::Result<()> {
    let tmp = tmp_path(path);
    remove_file_if_exists(&tmp)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    file.write_all(MAGIC)?;
    write_records(&mut file, &checkpoint_records(snapshot))?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    Ok(())
}

fn write_manifest(dir: &Path, manifest: &WalManifest) -> io::Result<()> {
    let path = dir.join(MANIFEST_FILE_NAME);
    let tmp = tmp_path(&path);
    remove_file_if_exists(&tmp)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    file.write_all(manifest.encode().as_bytes())?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn checkpoint_records(snapshot: &PersistentSnapshot) -> Vec<Record> {
    let mut records = Vec::new();
    records.extend(
        snapshot
            .sessions
            .iter()
            .map(|(client_id, session)| Record::SessionUpsert {
                client_id: client_id.clone(),
                session: session.clone(),
            }),
    );
    records.extend(
        snapshot
            .subscriptions
            .values()
            .cloned()
            .map(Record::SubscriptionUpsert),
    );
    records.extend(
        snapshot
            .retained
            .iter()
            .map(|(topic_name, message)| Record::RetainedUpsert {
                topic_name: topic_name.clone(),
                message: message.clone(),
            }),
    );
    records.extend(
        snapshot
            .offline
            .iter()
            .map(|(client_id, queue)| Record::OfflineReplace {
                client_id: client_id.clone(),
                queue: queue.clone(),
            }),
    );
    records.extend(
        snapshot
            .outbound
            .iter()
            .map(|(client_id, outbound)| Record::OutboundReplace {
                client_id: client_id.clone(),
                outbound: outbound.clone(),
            }),
    );
    records
}

fn open_wal_file(path: &Path) -> io::Result<File> {
    let new_file = !path.exists() || fs::metadata(path)?.len() == 0;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    if new_file {
        file.write_all(MAGIC)?;
        file.flush()?;
        file.sync_data()?;
    }
    Ok(file)
}

fn create_wal_file(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .append(true)
        .read(true)
        .create_new(true)
        .open(path)?;
    file.write_all(MAGIC)?;
    file.flush()?;
    file.sync_data()?;
    Ok(file)
}

fn cleanup_unreferenced(dir: &Path, active_epoch: Option<u64>) -> io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        if name.ends_with(TMP_SUFFIX) {
            remove_file_if_exists(&path)?;
            continue;
        }

        if name == LOG_FILE_NAME {
            if active_epoch.is_some() {
                remove_file_if_exists(&path)?;
            }
            continue;
        }

        if let Some(epoch) = wal_epoch_from_file_name(name)
            && Some(epoch) != active_epoch
        {
            remove_file_if_exists(&path)?;
        }
    }

    Ok(())
}

fn manifest_file_path(dir: &Path, file_name: &str) -> io::Result<PathBuf> {
    if Path::new(file_name)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(file_name)
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("manifest path must be a file name: {file_name}"),
        ));
    }
    Ok(dir.join(file_name))
}

fn wal_epoch_path(dir: &Path, epoch: u64) -> PathBuf {
    dir.join(wal_epoch_file_name(epoch))
}

fn wal_epoch_file_name(epoch: u64) -> String {
    format!("{LOG_FILE_NAME}.{epoch}")
}

fn wal_epoch_from_file_name(name: &str) -> Option<u64> {
    name.strip_prefix(&format!("{LOG_FILE_NAME}."))?
        .parse()
        .ok()
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("WAL file name")
        .to_string();
    file_name.push_str(TMP_SUFFIX);
    path.with_file_name(file_name)
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

#[derive(Clone, Default, PartialEq)]
struct PersistentSnapshot {
    sessions: HashMap<String, SessionSnapshot>,
    subscriptions: HashMap<(String, String), SubscriptionSnapshot>,
    retained: HashMap<String, RetainedMessage>,
    offline: HashMap<String, Vec<PendingSnapshot>>,
    outbound: HashMap<String, OutboundSnapshot>,
}

impl PersistentSnapshot {
    fn from_state(state: &BrokerState) -> Self {
        let mut snapshot = Self::default();
        for (client_id, session) in &state.sessions_by_client_id {
            snapshot.sessions.insert(
                client_id.clone(),
                SessionSnapshot {
                    session_expiry_interval: session.session_expiry_interval,
                    expires_at_ms: session.expires_at_ms,
                    next_packet_id: session.next_packet_id,
                },
            );

            if !session.offline_queue.is_empty() {
                snapshot.offline.insert(
                    client_id.clone(),
                    session
                        .offline_queue
                        .iter()
                        .map(PendingSnapshot::from_pending)
                        .collect(),
                );
            }

            let outbound = OutboundSnapshot::from_session(session);
            if !outbound.is_empty() {
                snapshot.outbound.insert(client_id.clone(), outbound);
            }
        }

        for subscription in &state.subscriptions {
            snapshot.subscriptions.insert(
                (subscription.client_id.clone(), subscription.filter.clone()),
                SubscriptionSnapshot::from_subscription(subscription),
            );
        }

        for (topic_name, message) in state.retained.iter() {
            snapshot
                .retained
                .insert(topic_name.clone(), message.clone());
        }

        snapshot
    }
}

#[derive(Clone, PartialEq)]
struct SessionSnapshot {
    session_expiry_interval: u32,
    expires_at_ms: Option<u64>,
    next_packet_id: u16,
}

#[derive(Clone, PartialEq)]
struct SubscriptionSnapshot {
    client_id: String,
    filter: String,
    match_filter: String,
    shared_group: Option<String>,
    maximum_qos: QoS,
    no_local: bool,
    retain_as_published: bool,
    retain_handling: u8,
    subscription_identifier: Option<u32>,
}

impl SubscriptionSnapshot {
    fn from_subscription(subscription: &SubscriptionEntry) -> Self {
        Self {
            client_id: subscription.client_id.clone(),
            filter: subscription.filter.clone(),
            match_filter: subscription.match_filter.clone(),
            shared_group: subscription.shared_group.clone(),
            maximum_qos: subscription.options.maximum_qos,
            no_local: subscription.options.no_local,
            retain_as_published: subscription.options.retain_as_published,
            retain_handling: subscription.options.retain_handling,
            subscription_identifier: subscription.subscription_identifier,
        }
    }

    fn into_subscription(self) -> SubscriptionEntry {
        SubscriptionEntry {
            client_id: self.client_id,
            filter: self.filter,
            match_filter: self.match_filter,
            shared_group: self.shared_group,
            options: SubscriptionOptions {
                maximum_qos: self.maximum_qos,
                no_local: self.no_local,
                retain_as_published: self.retain_as_published,
                retain_handling: self.retain_handling,
            },
            subscription_identifier: self.subscription_identifier,
        }
    }
}

#[derive(Clone, PartialEq)]
struct PendingSnapshot {
    packet: PublishPacket,
    expires_at_ms: Option<u64>,
}

impl PendingSnapshot {
    fn from_pending(pending: &PendingPublish) -> Self {
        Self {
            packet: pending.packet.clone(),
            expires_at_ms: pending.expires_at_ms,
        }
    }

    fn into_pending(self) -> PendingPublish {
        PendingPublish {
            packet: self.packet,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

#[derive(Clone, Default, PartialEq)]
struct OutboundSnapshot {
    qos1: HashMap<u16, PendingSnapshot>,
    qos2_publish: HashMap<u16, PendingSnapshot>,
    qos2_pubrel: HashSet<u16>,
}

impl OutboundSnapshot {
    fn from_session(session: &SessionEntry) -> Self {
        Self {
            qos1: session
                .outbound_qos1
                .iter()
                .map(|(packet_id, pending)| (*packet_id, PendingSnapshot::from_pending(pending)))
                .collect(),
            qos2_publish: session
                .outbound_qos2_publish
                .iter()
                .map(|(packet_id, pending)| (*packet_id, PendingSnapshot::from_pending(pending)))
                .collect(),
            qos2_pubrel: session.outbound_qos2_pubrel.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.qos1.is_empty() && self.qos2_publish.is_empty() && self.qos2_pubrel.is_empty()
    }
}

#[derive(Clone)]
enum Record {
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
        outbound: OutboundSnapshot,
    },
}

fn diff_records_for_changes(
    snapshot: &mut PersistentSnapshot,
    state: &BrokerState,
    changes: Vec<PersistenceChange>,
) -> Vec<Record> {
    let mut sync_sessions = false;
    let mut sync_subscriptions = false;
    let mut sync_retained = false;
    let mut offline_clients = HashSet::new();
    let mut outbound_clients = HashSet::new();

    for change in changes {
        match change {
            PersistenceChange::Sessions => sync_sessions = true,
            PersistenceChange::Subscriptions => sync_subscriptions = true,
            PersistenceChange::Retained => sync_retained = true,
            PersistenceChange::Offline(client_id) => {
                offline_clients.insert(client_id);
            }
            PersistenceChange::Outbound(client_id) => {
                outbound_clients.insert(client_id);
            }
        }
    }

    let mut records = Vec::new();
    if sync_sessions {
        records.extend(sync_session_records(snapshot, state));
    }
    if sync_subscriptions {
        records.extend(sync_subscription_records(snapshot, state));
    }
    if sync_retained {
        records.extend(sync_retained_records(snapshot, state));
    }
    for client_id in offline_clients {
        records.extend(sync_offline_records(snapshot, state, &client_id));
    }
    for client_id in outbound_clients {
        records.extend(sync_outbound_records(snapshot, state, &client_id));
    }
    records
}

fn sync_session_records(snapshot: &mut PersistentSnapshot, state: &BrokerState) -> Vec<Record> {
    let next: HashMap<String, SessionSnapshot> = state
        .sessions_by_client_id
        .iter()
        .map(|(client_id, session)| {
            (
                client_id.clone(),
                SessionSnapshot {
                    session_expiry_interval: session.session_expiry_interval,
                    expires_at_ms: session.expires_at_ms,
                    next_packet_id: session.next_packet_id,
                },
            )
        })
        .collect();
    let mut records = Vec::new();
    for (client_id, session) in &next {
        if snapshot.sessions.get(client_id) != Some(session) {
            records.push(Record::SessionUpsert {
                client_id: client_id.clone(),
                session: session.clone(),
            });
        }
    }
    for client_id in snapshot.sessions.keys() {
        if !next.contains_key(client_id) {
            records.push(Record::SessionDelete {
                client_id: client_id.clone(),
            });
        }
    }
    snapshot.sessions = next;
    records
}

fn sync_subscription_records(
    snapshot: &mut PersistentSnapshot,
    state: &BrokerState,
) -> Vec<Record> {
    if state.subscriptions.len() == snapshot.subscriptions.len() + 1
        && let Some(subscription) = state.subscriptions.last()
    {
        let key = (subscription.client_id.clone(), subscription.filter.clone());
        if let std::collections::hash_map::Entry::Vacant(entry) = snapshot.subscriptions.entry(key)
        {
            let subscription = SubscriptionSnapshot::from_subscription(subscription);
            entry.insert(subscription.clone());
            return vec![Record::SubscriptionUpsert(subscription)];
        }
    }

    let next: HashMap<(String, String), SubscriptionSnapshot> = state
        .subscriptions
        .iter()
        .map(|subscription| {
            (
                (subscription.client_id.clone(), subscription.filter.clone()),
                SubscriptionSnapshot::from_subscription(subscription),
            )
        })
        .collect();
    let mut records = Vec::new();
    for (key, subscription) in &next {
        if snapshot.subscriptions.get(key) != Some(subscription) {
            records.push(Record::SubscriptionUpsert(subscription.clone()));
        }
    }
    for (client_id, filter) in snapshot.subscriptions.keys() {
        if !next.contains_key(&(client_id.clone(), filter.clone())) {
            records.push(Record::SubscriptionDelete {
                client_id: client_id.clone(),
                filter: filter.clone(),
            });
        }
    }
    snapshot.subscriptions = next;
    records
}

fn sync_retained_records(snapshot: &mut PersistentSnapshot, state: &BrokerState) -> Vec<Record> {
    let next: HashMap<String, RetainedMessage> = state
        .retained
        .iter()
        .map(|(topic_name, message)| (topic_name.clone(), message.clone()))
        .collect();
    let mut records = Vec::new();
    for (topic_name, message) in &next {
        if snapshot.retained.get(topic_name) != Some(message) {
            records.push(Record::RetainedUpsert {
                topic_name: topic_name.clone(),
                message: message.clone(),
            });
        }
    }
    for topic_name in snapshot.retained.keys() {
        if !next.contains_key(topic_name) {
            records.push(Record::RetainedDelete {
                topic_name: topic_name.clone(),
            });
        }
    }
    snapshot.retained = next;
    records
}

fn sync_offline_records(
    snapshot: &mut PersistentSnapshot,
    state: &BrokerState,
    client_id: &str,
) -> Vec<Record> {
    let next = state
        .sessions_by_client_id
        .get(client_id)
        .map(|session| {
            session
                .offline_queue
                .iter()
                .map(PendingSnapshot::from_pending)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if next.is_empty() {
        if snapshot.offline.remove(client_id).is_some() {
            return vec![Record::OfflineReplace {
                client_id: client_id.to_string(),
                queue: Vec::new(),
            }];
        }
        return Vec::new();
    }

    if snapshot.offline.get(client_id) == Some(&next) {
        return Vec::new();
    }
    snapshot.offline.insert(client_id.to_string(), next.clone());
    vec![Record::OfflineReplace {
        client_id: client_id.to_string(),
        queue: next,
    }]
}

fn sync_outbound_records(
    snapshot: &mut PersistentSnapshot,
    state: &BrokerState,
    client_id: &str,
) -> Vec<Record> {
    let next = state
        .sessions_by_client_id
        .get(client_id)
        .map(OutboundSnapshot::from_session)
        .unwrap_or_default();
    if next.is_empty() {
        if snapshot.outbound.remove(client_id).is_some() {
            return vec![Record::OutboundReplace {
                client_id: client_id.to_string(),
                outbound: OutboundSnapshot::default(),
            }];
        }
        return Vec::new();
    }

    if snapshot.outbound.get(client_id) == Some(&next) {
        return Vec::new();
    }
    snapshot
        .outbound
        .insert(client_id.to_string(), next.clone());
    vec![Record::OutboundReplace {
        client_id: client_id.to_string(),
        outbound: next,
    }]
}

#[derive(Clone, Copy)]
enum ReplayMode {
    AllowCorruptTail,
    Strict,
}

fn replay_log(path: &Path) -> io::Result<BrokerState> {
    replay_log_with_mode(path, ReplayMode::AllowCorruptTail)
}

fn replay_checkpoint(path: &Path) -> io::Result<BrokerState> {
    replay_log_with_mode(path, ReplayMode::Strict)
}

fn replay_log_with_mode(path: &Path, mode: ReplayMode) -> io::Result<BrokerState> {
    let mut state = BrokerState::default();
    if !path.exists() {
        return Ok(state);
    }
    replay_log_into(path, &mut state, mode)?;
    Ok(state)
}

fn replay_log_into(path: &Path, state: &mut BrokerState, mode: ReplayMode) -> io::Result<()> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; MAGIC.len()];
    match file.read_exact(&mut magic) {
        Ok(()) if magic == MAGIC => {}
        Ok(()) => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid Pulse binary log header",
            ));
        }
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
            return match mode {
                ReplayMode::AllowCorruptTail => Ok(()),
                ReplayMode::Strict => Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "incomplete Pulse binary log header",
                )),
            };
        }
        Err(error) => return Err(error),
    }

    loop {
        let mut length = [0u8; 4];
        match file.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }

        let length = u32::from_le_bytes(length) as usize;
        let mut payload = vec![0u8; length];
        if let Err(error) = file.read_exact(&mut payload) {
            if error.kind() == ErrorKind::UnexpectedEof {
                return match mode {
                    ReplayMode::AllowCorruptTail => Ok(()),
                    ReplayMode::Strict => Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "incomplete Pulse binary log payload",
                    )),
                };
            }
            return Err(error);
        }

        let mut checksum = [0u8; 4];
        if let Err(error) = file.read_exact(&mut checksum) {
            if error.kind() == ErrorKind::UnexpectedEof {
                return match mode {
                    ReplayMode::AllowCorruptTail => Ok(()),
                    ReplayMode::Strict => Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "incomplete Pulse binary log checksum",
                    )),
                };
            }
            return Err(error);
        }
        if crc32(&payload) != u32::from_le_bytes(checksum) {
            return match mode {
                ReplayMode::AllowCorruptTail => Ok(()),
                ReplayMode::Strict => Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "Pulse binary log checksum mismatch",
                )),
            };
        }

        let Some(record) = decode_record(&payload) else {
            return match mode {
                ReplayMode::AllowCorruptTail => Ok(()),
                ReplayMode::Strict => Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "invalid Pulse binary log record",
                )),
            };
        };
        apply_record(state, record);
    }

    Ok(())
}

fn apply_record(state: &mut BrokerState, record: Record) {
    match record {
        Record::SessionUpsert { client_id, session } => {
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
        Record::SessionDelete { client_id } => {
            state.sessions_by_client_id.remove(&client_id);
            state
                .subscriptions
                .retain(|subscription| subscription.client_id != client_id);
        }
        Record::SubscriptionUpsert(subscription) => {
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
        Record::SubscriptionDelete { client_id, filter } => {
            state.subscriptions.retain(|subscription| {
                !(subscription.client_id == client_id && subscription.filter == filter)
            });
        }
        Record::RetainedUpsert {
            topic_name,
            message,
        } => {
            state.retained.insert(topic_name, message);
        }
        Record::RetainedDelete { topic_name } => {
            state.retained.remove(&topic_name);
        }
        Record::OfflineReplace { client_id, queue } => {
            if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
                session.offline_queue = queue
                    .into_iter()
                    .map(PendingSnapshot::into_pending)
                    .collect::<VecDeque<_>>();
            }
        }
        Record::OutboundReplace {
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

fn encode_record(record: &Record) -> Vec<u8> {
    let mut writer = Writer::default();
    match record {
        Record::SessionUpsert { client_id, session } => {
            writer.u8(SESSION_UPSERT);
            writer.string(client_id);
            writer.u32(session.session_expiry_interval);
            writer.opt_u64(session.expires_at_ms);
            writer.u16(session.next_packet_id);
        }
        Record::SessionDelete { client_id } => {
            writer.u8(SESSION_DELETE);
            writer.string(client_id);
        }
        Record::SubscriptionUpsert(subscription) => {
            writer.u8(SUBSCRIPTION_UPSERT);
            writer.string(&subscription.client_id);
            writer.string(&subscription.filter);
            writer.string(&subscription.match_filter);
            writer.opt_string(subscription.shared_group.as_deref());
            writer.u8(qos_to_u8(subscription.maximum_qos));
            writer.bool(subscription.no_local);
            writer.bool(subscription.retain_as_published);
            writer.u8(subscription.retain_handling);
            writer.opt_u32(subscription.subscription_identifier);
        }
        Record::SubscriptionDelete { client_id, filter } => {
            writer.u8(SUBSCRIPTION_DELETE);
            writer.string(client_id);
            writer.string(filter);
        }
        Record::RetainedUpsert {
            topic_name,
            message,
        } => {
            writer.u8(RETAINED_UPSERT);
            writer.string(topic_name);
            writer.opt_u64(message.expires_at_ms);
            writer.bytes(&encode_retained(message));
        }
        Record::RetainedDelete { topic_name } => {
            writer.u8(RETAINED_DELETE);
            writer.string(topic_name);
        }
        Record::OfflineReplace { client_id, queue } => {
            writer.u8(OFFLINE_REPLACE);
            writer.string(client_id);
            writer.u32(queue.len() as u32);
            for pending in queue {
                writer.opt_u64(pending.expires_at_ms);
                writer.bytes(&encode_publish(&pending.packet));
            }
        }
        Record::OutboundReplace {
            client_id,
            outbound,
        } => {
            writer.u8(OUTBOUND_REPLACE);
            writer.string(client_id);
            writer.u32(outbound.qos1.len() as u32);
            for (packet_id, pending) in &outbound.qos1 {
                writer.u16(*packet_id);
                writer.opt_u64(pending.expires_at_ms);
                writer.bytes(&encode_publish(&pending.packet));
            }
            writer.u32(outbound.qos2_publish.len() as u32);
            for (packet_id, pending) in &outbound.qos2_publish {
                writer.u16(*packet_id);
                writer.opt_u64(pending.expires_at_ms);
                writer.bytes(&encode_publish(&pending.packet));
            }
            writer.u32(outbound.qos2_pubrel.len() as u32);
            for packet_id in &outbound.qos2_pubrel {
                writer.u16(*packet_id);
            }
        }
    }
    writer.into_inner()
}

fn decode_record(payload: &[u8]) -> Option<Record> {
    let mut reader = Reader::new(payload);
    let tag = reader.u8()?;
    match tag {
        SESSION_UPSERT => Some(Record::SessionUpsert {
            client_id: reader.string()?,
            session: SessionSnapshot {
                session_expiry_interval: reader.u32()?,
                expires_at_ms: reader.opt_u64()?,
                next_packet_id: reader.u16()?,
            },
        }),
        SESSION_DELETE => Some(Record::SessionDelete {
            client_id: reader.string()?,
        }),
        SUBSCRIPTION_UPSERT => Some(Record::SubscriptionUpsert(SubscriptionSnapshot {
            client_id: reader.string()?,
            filter: reader.string()?,
            match_filter: reader.string()?,
            shared_group: reader.opt_string()?,
            maximum_qos: qos_from_u8(reader.u8()?),
            no_local: reader.bool()?,
            retain_as_published: reader.bool()?,
            retain_handling: reader.u8()?,
            subscription_identifier: reader.opt_u32()?,
        })),
        SUBSCRIPTION_DELETE => Some(Record::SubscriptionDelete {
            client_id: reader.string()?,
            filter: reader.string()?,
        }),
        RETAINED_UPSERT => {
            let topic_name = reader.string()?;
            let expires_at_ms = reader.opt_u64()?;
            let mut message = decode_retained(reader.bytes()?)?;
            message.expires_at_ms = expires_at_ms;
            Some(Record::RetainedUpsert {
                topic_name,
                message,
            })
        }
        RETAINED_DELETE => Some(Record::RetainedDelete {
            topic_name: reader.string()?,
        }),
        OFFLINE_REPLACE => {
            let client_id = reader.string()?;
            let count = reader.u32()? as usize;
            let mut queue = Vec::with_capacity(count);
            for _ in 0..count {
                let expires_at_ms = reader.opt_u64()?;
                let packet = decode_publish(reader.bytes()?)?;
                queue.push(PendingSnapshot {
                    packet,
                    expires_at_ms,
                });
            }
            Some(Record::OfflineReplace { client_id, queue })
        }
        OUTBOUND_REPLACE => {
            let client_id = reader.string()?;
            let mut outbound = OutboundSnapshot::default();
            let qos1_count = reader.u32()? as usize;
            for _ in 0..qos1_count {
                let packet_id = reader.u16()?;
                let expires_at_ms = reader.opt_u64()?;
                let packet = decode_publish(reader.bytes()?)?;
                outbound.qos1.insert(
                    packet_id,
                    PendingSnapshot {
                        packet,
                        expires_at_ms,
                    },
                );
            }
            let qos2_count = reader.u32()? as usize;
            for _ in 0..qos2_count {
                let packet_id = reader.u16()?;
                let expires_at_ms = reader.opt_u64()?;
                let packet = decode_publish(reader.bytes()?)?;
                outbound.qos2_publish.insert(
                    packet_id,
                    PendingSnapshot {
                        packet,
                        expires_at_ms,
                    },
                );
            }
            let pubrel_count = reader.u32()? as usize;
            for _ in 0..pubrel_count {
                outbound.qos2_pubrel.insert(reader.u16()?);
            }
            Some(Record::OutboundReplace {
                client_id,
                outbound,
            })
        }
        _ => None,
    }
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

    fn opt_u32(&mut self, value: Option<u32>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.u32(value);
        }
    }

    fn opt_u64(&mut self, value: Option<u64>) {
        self.bool(value.is_some());
        if let Some(value) = value {
            self.bytes.extend_from_slice(&value.to_le_bytes());
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
        self.u32(value.len() as u32);
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

    fn u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.index)?;
        self.index += 1;
        Some(value)
    }

    fn bool(&mut self) -> Option<bool> {
        Some(self.u8()? != 0)
    }

    fn u16(&mut self) -> Option<u16> {
        let bytes = self.take_array::<2>()?;
        Some(u16::from_le_bytes(bytes))
    }

    fn u32(&mut self) -> Option<u32> {
        let bytes = self.take_array::<4>()?;
        Some(u32::from_le_bytes(bytes))
    }

    fn opt_u32(&mut self) -> Option<Option<u32>> {
        if self.bool()? {
            Some(Some(self.u32()?))
        } else {
            Some(None)
        }
    }

    fn opt_u64(&mut self) -> Option<Option<u64>> {
        if self.bool()? {
            let bytes = self.take_array::<8>()?;
            Some(Some(u64::from_le_bytes(bytes)))
        } else {
            Some(None)
        }
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?.to_vec()).ok()
    }

    fn opt_string(&mut self) -> Option<Option<String>> {
        if self.bool()? {
            Some(Some(self.string()?))
        } else {
            Some(None)
        }
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn take_array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let bytes = self.take(N)?;
        bytes.try_into().ok()
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.index.checked_add(len)?;
        let bytes = self.bytes.get(self.index..end)?;
        self.index = end;
        Some(bytes)
    }
}

fn encode_retained(message: &RetainedMessage) -> Vec<u8> {
    let packet_id = if message.qos == QoS::AtMostOnce {
        None
    } else {
        Some(1)
    };
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

fn encode_publish(packet: &PublishPacket) -> Vec<u8> {
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

fn decode_publish(packet: &[u8]) -> Option<PublishPacket> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::from(packet);
    let packet = codec.decode(&mut buffer).ok().flatten()?;
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

fn qos_from_u8(value: u8) -> QoS {
    match value {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::runtime::retained_store::RetainedMessage;

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "pulse-binary-storage-{name}-{}",
            std::process::id()
        ));
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

    fn wal_epoch_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = fs::read_dir(dir)
            .expect("read WAL dir")
            .map(|entry| entry.expect("dir entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(wal_epoch_from_file_name)
                    .is_some()
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[test]
    fn recovers_sessions_subscriptions_retained_offline_and_outbound() {
        let dir = temp_dir("recover");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("open storage");
            storage.with_state(&mut |state| {
                state.sessions_by_client_id.insert(
                    "client".to_string(),
                    SessionEntry::disconnected(60, Some(123)),
                );
                let session = state
                    .sessions_by_client_id
                    .get_mut("client")
                    .expect("session");
                session.next_packet_id = 7;
                session.offline_queue.push_back(PendingPublish {
                    packet: publish("devices/offline", b"offline", QoS::AtLeastOnce),
                    expires_at_ms: Some(456),
                });
                session.outbound_qos1.insert(
                    4,
                    PendingPublish {
                        packet: publish("devices/inflight", b"inflight", QoS::AtLeastOnce),
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
                state.mark_sessions_changed();
                state.mark_subscriptions_changed();
                state.mark_retained_changed();
                state.mark_offline_changed("client");
                state.mark_outbound_changed("client");
            });
        }

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("reopen storage");
        storage.read_state(&mut |state| {
            let session = state.sessions_by_client_id.get("client").expect("session");
            assert_eq!(session.next_packet_id, 7);
            assert_eq!(session.offline_queue.len(), 1);
            assert!(session.outbound_qos1.contains_key(&4));
            assert!(session.outbound_qos2_pubrel.contains(&9));
            assert_eq!(state.subscriptions.len(), 1);
            let retained = state.retained.get("devices/retained").expect("retained");
            assert_eq!(retained.payload, Bytes::from_static(b"retained"));
            assert_eq!(retained.expires_at_ms, Some(999));
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn size_trigger_compacts_checkpoint_and_removes_legacy_wal() {
        let dir = temp_dir("size-compact");
        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: MAGIC.len() as u64,
                    interval_ms: 0,
                },
            )
            .expect("open storage");
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });

            assert!(dir.join(MANIFEST_FILE_NAME).exists());
            assert!(dir.join(CHECKPOINT_FILE_NAME).exists());
            assert!(!dir.join(LOG_FILE_NAME).exists());
            assert_eq!(wal_epoch_files(&dir).len(), 1);
        }

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("reopen storage");
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.contains_key("client"));
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn time_trigger_compacts_only_after_a_new_wal_write() {
        let dir = temp_dir("time-compact");
        let compact = WalCompactConfig {
            max_bytes: 0,
            interval_ms: 1,
        };
        {
            let _storage = BinaryStorage::open_with_options(&dir, CommitPolicy::Strict, compact)
                .expect("open storage");
            thread::sleep(Duration::from_millis(2));
            assert!(!dir.join(MANIFEST_FILE_NAME).exists());
        }

        {
            let storage = BinaryStorage::open_with_options(&dir, CommitPolicy::Strict, compact)
                .expect("reopen storage");
            thread::sleep(Duration::from_millis(2));
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });
            assert!(dir.join(MANIFEST_FILE_NAME).exists());
            assert!(dir.join(CHECKPOINT_FILE_NAME).exists());
        }
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compact_recovery_restores_checkpoint_and_active_wal_records() {
        let dir = temp_dir("checkpoint-active-recover");
        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: MAGIC.len() as u64,
                    interval_ms: 0,
                },
            )
            .expect("open storage");
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });
        }

        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: 0,
                    interval_ms: 0,
                },
            )
            .expect("reopen storage");
            storage.with_state(&mut |state| {
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
                    subscription_identifier: None,
                });
                state.mark_subscriptions_changed();
            });
        }

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("recover storage");
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.contains_key("client"));
            assert_eq!(state.subscriptions.len(), 1);
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn startup_cleans_tmp_and_unreferenced_epoch_files() {
        let dir = temp_dir("cleanup");
        {
            let storage = BinaryStorage::open_with_options(
                &dir,
                CommitPolicy::Strict,
                WalCompactConfig {
                    max_bytes: MAGIC.len() as u64,
                    interval_ms: 0,
                },
            )
            .expect("open storage");
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });
        }
        fs::write(dir.join("broker.checkpoint.tmp"), b"junk").expect("write tmp checkpoint");
        fs::write(dir.join("broker.manifest.tmp"), b"junk").expect("write tmp manifest");
        fs::write(dir.join(LOG_FILE_NAME), MAGIC).expect("write stale legacy WAL");
        fs::write(wal_epoch_path(&dir, 999), MAGIC).expect("write orphan WAL");

        let _storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("reopen storage");
        assert!(!dir.join("broker.checkpoint.tmp").exists());
        assert!(!dir.join("broker.manifest.tmp").exists());
        assert!(!dir.join(LOG_FILE_NAME).exists());
        assert!(!wal_epoch_path(&dir, 999).exists());
        assert_eq!(wal_epoch_files(&dir).len(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn ignores_corrupt_tail() {
        let dir = temp_dir("corrupt-tail");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("open storage");
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });
        }

        let path = dir.join(LOG_FILE_NAME);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("append log");
        file.write_all(&[7, 0, 0, 0, 1, 2]).expect("write junk");

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("reopen storage");
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.contains_key("client"));
        });
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn fast_policy_flushes_background_writer_on_drop() {
        let dir = temp_dir("fast-drop");
        {
            let storage = BinaryStorage::open(&dir, CommitPolicy::Fast).expect("open storage");
            storage.with_state(&mut |state| {
                state
                    .sessions_by_client_id
                    .insert("client".to_string(), SessionEntry::disconnected(60, None));
                state.mark_sessions_changed();
            });
        }

        let storage = BinaryStorage::open(&dir, CommitPolicy::Strict).expect("reopen storage");
        storage.read_state(&mut |state| {
            assert!(state.sessions_by_client_id.contains_key("client"));
        });
        let _ = fs::remove_dir_all(dir);
    }
}
