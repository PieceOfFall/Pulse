use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::Deserialize;

use super::routing::RouteSubscription;

const MAGIC: &[u8] = b"PVWAL1\n";
const EVENT_VERSION: u8 = 1;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DurableLogEvent {
    SessionUpsert {
        client_id: String,
        session_expiry_interval: u32,
        expires_at_ms: Option<u64>,
        next_packet_id: u16,
    },
    SubscriptionUpsert {
        client_id: String,
        filter: String,
        match_filter: String,
        shared_group: Option<String>,
        subscription_identifier: Option<u32>,
    },
    SubscriptionDelete {
        client_id: String,
        filter: String,
    },
    RetainUpsert {
        topic_name: String,
        qos: u8,
        payload_len: u32,
        expires_at_ms: Option<u64>,
    },
    RetainDelete {
        topic_name: String,
    },
    OfflineEnqueue {
        client_id: String,
        sequence: u64,
        packet_id: Option<u16>,
        payload_len: u32,
        expires_at_ms: Option<u64>,
    },
    OfflineAck {
        client_id: String,
        sequence: u64,
    },
    InflightUpsert {
        client_id: String,
        packet_id: u16,
        qos: u8,
        direction: InflightDirection,
        expires_at_ms: Option<u64>,
    },
    InflightAck {
        client_id: String,
        packet_id: u16,
        direction: InflightDirection,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum InflightDirection {
    Inbound,
    Outbound,
}

#[derive(Debug)]
pub(crate) struct DurableLog {
    path: PathBuf,
    file: File,
    commit_policy: CommitPolicy,
    pending_balanced_records: usize,
}

impl DurableLog {
    pub(crate) fn open(path: impl AsRef<Path>, commit_policy: CommitPolicy) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }

        let new_file = !path.exists() || fs::metadata(&path)?.len() == 0;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        if new_file {
            file.write_all(MAGIC)?;
            file.flush()?;
        }

        Ok(Self {
            path,
            file,
            commit_policy,
            pending_balanced_records: 0,
        })
    }

    pub(crate) fn append(&mut self, event: &DurableLogEvent) -> io::Result<()> {
        let payload = encode_event(event);
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&payload)?;
        match self.commit_policy {
            CommitPolicy::Strict => self.file.sync_data()?,
            CommitPolicy::Balanced => {
                self.pending_balanced_records += 1;
                if self.pending_balanced_records >= 64 {
                    self.file.sync_data()?;
                    self.pending_balanced_records = 0;
                }
            }
            CommitPolicy::Fast => {}
        }
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if !matches!(self.commit_policy, CommitPolicy::Fast) {
            self.file.sync_data()?;
        }
        self.pending_balanced_records = 0;
        Ok(())
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn replay(path: impl AsRef<Path>) -> io::Result<RecoveredDurableState> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(RecoveredDurableState::default());
        }

        let mut file = File::open(path)?;
        let mut magic = [0u8; MAGIC.len()];
        if let Err(error) = file.read_exact(&mut magic) {
            if error.kind() == ErrorKind::UnexpectedEof {
                return Ok(RecoveredDurableState::default());
            }
            return Err(error);
        }
        if magic != MAGIC {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "invalid Pulse vNext WAL header",
            ));
        }

        let mut state = RecoveredDurableState::default();
        loop {
            let mut length = [0u8; 4];
            match file.read_exact(&mut length) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
            let length = u32::from_le_bytes(length) as usize;
            let mut payload = vec![0u8; length];
            match file.read_exact(&mut payload) {
                Ok(()) => {
                    let event = decode_event(&payload)?;
                    state.apply(event);
                }
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error),
            }
        }
        Ok(state)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RecoveredDurableState {
    pub(crate) sessions: BTreeMap<String, RecoveredSession>,
    pub(crate) subscriptions: BTreeMap<(String, String), RouteSubscription>,
    pub(crate) retained: BTreeMap<String, RecoveredRetained>,
    pub(crate) offline: BTreeSet<(String, u64)>,
    pub(crate) inflight: BTreeMap<(String, u16, InflightDirection), RecoveredInflight>,
}

impl RecoveredDurableState {
    fn apply(&mut self, event: DurableLogEvent) {
        match event {
            DurableLogEvent::SessionUpsert {
                client_id,
                session_expiry_interval,
                expires_at_ms,
                next_packet_id,
            } => {
                self.sessions.insert(
                    client_id,
                    RecoveredSession {
                        session_expiry_interval,
                        expires_at_ms,
                        next_packet_id,
                    },
                );
            }
            DurableLogEvent::SubscriptionUpsert {
                client_id,
                filter,
                match_filter,
                shared_group,
                subscription_identifier,
            } => {
                self.subscriptions.insert(
                    (client_id.clone(), filter.clone()),
                    RouteSubscription {
                        client_id,
                        filter,
                        match_filter,
                        shared_group,
                        subscription_identifier,
                    },
                );
            }
            DurableLogEvent::SubscriptionDelete { client_id, filter } => {
                self.subscriptions.remove(&(client_id, filter));
            }
            DurableLogEvent::RetainUpsert {
                topic_name,
                qos,
                payload_len,
                expires_at_ms,
            } => {
                self.retained.insert(
                    topic_name,
                    RecoveredRetained {
                        qos,
                        payload_len,
                        expires_at_ms,
                    },
                );
            }
            DurableLogEvent::RetainDelete { topic_name } => {
                self.retained.remove(&topic_name);
            }
            DurableLogEvent::OfflineEnqueue {
                client_id,
                sequence,
                ..
            } => {
                self.offline.insert((client_id, sequence));
            }
            DurableLogEvent::OfflineAck {
                client_id,
                sequence,
            } => {
                self.offline.remove(&(client_id, sequence));
            }
            DurableLogEvent::InflightUpsert {
                client_id,
                packet_id,
                qos,
                direction,
                expires_at_ms,
            } => {
                self.inflight.insert(
                    (client_id, packet_id, direction),
                    RecoveredInflight { qos, expires_at_ms },
                );
            }
            DurableLogEvent::InflightAck {
                client_id,
                packet_id,
                direction,
            } => {
                self.inflight.remove(&(client_id, packet_id, direction));
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredSession {
    pub(crate) session_expiry_interval: u32,
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) next_packet_id: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredRetained {
    pub(crate) qos: u8,
    pub(crate) payload_len: u32,
    pub(crate) expires_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredInflight {
    pub(crate) qos: u8,
    pub(crate) expires_at_ms: Option<u64>,
}

fn encode_event(event: &DurableLogEvent) -> Vec<u8> {
    let mut output = Vec::new();
    output.push(EVENT_VERSION);
    match event {
        DurableLogEvent::SessionUpsert {
            client_id,
            session_expiry_interval,
            expires_at_ms,
            next_packet_id,
        } => {
            output.push(1);
            write_string(&mut output, client_id);
            output.extend_from_slice(&session_expiry_interval.to_le_bytes());
            write_option_u64(&mut output, *expires_at_ms);
            output.extend_from_slice(&next_packet_id.to_le_bytes());
        }
        DurableLogEvent::SubscriptionUpsert {
            client_id,
            filter,
            match_filter,
            shared_group,
            subscription_identifier,
        } => {
            output.push(2);
            write_string(&mut output, client_id);
            write_string(&mut output, filter);
            write_string(&mut output, match_filter);
            write_option_string(&mut output, shared_group.as_deref());
            write_option_u32(&mut output, *subscription_identifier);
        }
        DurableLogEvent::SubscriptionDelete { client_id, filter } => {
            output.push(3);
            write_string(&mut output, client_id);
            write_string(&mut output, filter);
        }
        DurableLogEvent::RetainUpsert {
            topic_name,
            qos,
            payload_len,
            expires_at_ms,
        } => {
            output.push(4);
            write_string(&mut output, topic_name);
            output.push(*qos);
            output.extend_from_slice(&payload_len.to_le_bytes());
            write_option_u64(&mut output, *expires_at_ms);
        }
        DurableLogEvent::RetainDelete { topic_name } => {
            output.push(5);
            write_string(&mut output, topic_name);
        }
        DurableLogEvent::OfflineEnqueue {
            client_id,
            sequence,
            packet_id,
            payload_len,
            expires_at_ms,
        } => {
            output.push(6);
            write_string(&mut output, client_id);
            output.extend_from_slice(&sequence.to_le_bytes());
            write_option_u16(&mut output, *packet_id);
            output.extend_from_slice(&payload_len.to_le_bytes());
            write_option_u64(&mut output, *expires_at_ms);
        }
        DurableLogEvent::OfflineAck {
            client_id,
            sequence,
        } => {
            output.push(7);
            write_string(&mut output, client_id);
            output.extend_from_slice(&sequence.to_le_bytes());
        }
        DurableLogEvent::InflightUpsert {
            client_id,
            packet_id,
            qos,
            direction,
            expires_at_ms,
        } => {
            output.push(8);
            write_string(&mut output, client_id);
            output.extend_from_slice(&packet_id.to_le_bytes());
            output.push(*qos);
            output.push(direction_code(*direction));
            write_option_u64(&mut output, *expires_at_ms);
        }
        DurableLogEvent::InflightAck {
            client_id,
            packet_id,
            direction,
        } => {
            output.push(9);
            write_string(&mut output, client_id);
            output.extend_from_slice(&packet_id.to_le_bytes());
            output.push(direction_code(*direction));
        }
    }
    output
}

fn decode_event(payload: &[u8]) -> io::Result<DurableLogEvent> {
    let mut input = Decoder { payload, offset: 0 };
    let version = input.read_u8()?;
    if version != EVENT_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "unsupported Pulse vNext WAL event version",
        ));
    }
    match input.read_u8()? {
        1 => Ok(DurableLogEvent::SessionUpsert {
            client_id: input.read_string()?,
            session_expiry_interval: input.read_u32()?,
            expires_at_ms: input.read_option_u64()?,
            next_packet_id: input.read_u16()?,
        }),
        2 => Ok(DurableLogEvent::SubscriptionUpsert {
            client_id: input.read_string()?,
            filter: input.read_string()?,
            match_filter: input.read_string()?,
            shared_group: input.read_option_string()?,
            subscription_identifier: input.read_option_u32()?,
        }),
        3 => Ok(DurableLogEvent::SubscriptionDelete {
            client_id: input.read_string()?,
            filter: input.read_string()?,
        }),
        4 => Ok(DurableLogEvent::RetainUpsert {
            topic_name: input.read_string()?,
            qos: input.read_u8()?,
            payload_len: input.read_u32()?,
            expires_at_ms: input.read_option_u64()?,
        }),
        5 => Ok(DurableLogEvent::RetainDelete {
            topic_name: input.read_string()?,
        }),
        6 => Ok(DurableLogEvent::OfflineEnqueue {
            client_id: input.read_string()?,
            sequence: input.read_u64()?,
            packet_id: input.read_option_u16()?,
            payload_len: input.read_u32()?,
            expires_at_ms: input.read_option_u64()?,
        }),
        7 => Ok(DurableLogEvent::OfflineAck {
            client_id: input.read_string()?,
            sequence: input.read_u64()?,
        }),
        8 => Ok(DurableLogEvent::InflightUpsert {
            client_id: input.read_string()?,
            packet_id: input.read_u16()?,
            qos: input.read_u8()?,
            direction: decode_direction(input.read_u8()?)?,
            expires_at_ms: input.read_option_u64()?,
        }),
        9 => Ok(DurableLogEvent::InflightAck {
            client_id: input.read_string()?,
            packet_id: input.read_u16()?,
            direction: decode_direction(input.read_u8()?)?,
        }),
        kind => Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("unknown Pulse vNext WAL event kind {kind}"),
        )),
    }
}

fn write_string(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(&(value.len() as u32).to_le_bytes());
    output.extend_from_slice(value.as_bytes());
}

fn write_option_string(output: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            output.push(1);
            write_string(output, value);
        }
        None => output.push(0),
    }
}

fn write_option_u16(output: &mut Vec<u8>, value: Option<u16>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(0),
    }
}

fn write_option_u32(output: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(0),
    }
}

fn write_option_u64(output: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
        }
        None => output.push(0),
    }
}

fn direction_code(direction: InflightDirection) -> u8 {
    match direction {
        InflightDirection::Inbound => 1,
        InflightDirection::Outbound => 2,
    }
}

fn decode_direction(value: u8) -> io::Result<InflightDirection> {
    match value {
        1 => Ok(InflightDirection::Inbound),
        2 => Ok(InflightDirection::Outbound),
        _ => Err(io::Error::new(
            ErrorKind::InvalidData,
            "invalid inflight direction",
        )),
    }
}

struct Decoder<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn read_u8(&mut self) -> io::Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self) -> io::Result<String> {
        let length = self.read_u32()? as usize;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid WAL string"))
    }

    fn read_option_string(&mut self) -> io::Result<Option<String>> {
        if self.read_u8()? == 0 {
            Ok(None)
        } else {
            self.read_string().map(Some)
        }
    }

    fn read_option_u16(&mut self) -> io::Result<Option<u16>> {
        if self.read_u8()? == 0 {
            Ok(None)
        } else {
            self.read_u16().map(Some)
        }
    }

    fn read_option_u32(&mut self) -> io::Result<Option<u32>> {
        if self.read_u8()? == 0 {
            Ok(None)
        } else {
            self.read_u32().map(Some)
        }
    }

    fn read_option_u64(&mut self) -> io::Result<Option<u64>> {
        if self.read_u8()? == 0 {
            Ok(None)
        } else {
            self.read_u64().map(Some)
        }
    }

    fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "invalid WAL length"))?;
        if end > self.payload.len() {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "truncated WAL event",
            ));
        }
        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitPolicy, DurableLog, DurableLogEvent, InflightDirection, RecoveredDurableState,
    };

    #[test]
    fn wal_replays_latest_state() {
        let path =
            std::env::temp_dir().join(format!("pulse-vnext-wal-{}-latest.wal", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut log = DurableLog::open(&path, CommitPolicy::Balanced).expect("open wal");
            log.append(&DurableLogEvent::SessionUpsert {
                client_id: "client-a".to_string(),
                session_expiry_interval: 60,
                expires_at_ms: Some(123),
                next_packet_id: 7,
            })
            .expect("append session");
            log.append(&DurableLogEvent::SubscriptionUpsert {
                client_id: "client-a".to_string(),
                filter: "devices/+".to_string(),
                match_filter: "devices/+".to_string(),
                shared_group: None,
                subscription_identifier: Some(9),
            })
            .expect("append subscription");
            log.append(&DurableLogEvent::SubscriptionDelete {
                client_id: "client-a".to_string(),
                filter: "devices/+".to_string(),
            })
            .expect("append delete");
            log.append(&DurableLogEvent::InflightUpsert {
                client_id: "client-a".to_string(),
                packet_id: 42,
                qos: 1,
                direction: InflightDirection::Outbound,
                expires_at_ms: None,
            })
            .expect("append inflight");
            log.flush().expect("flush wal");
        }

        let state = DurableLog::replay(&path).expect("replay wal");
        assert_eq!(state.sessions["client-a"].next_packet_id, 7);
        assert!(state.subscriptions.is_empty());
        assert_eq!(state.inflight.len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_missing_wal_returns_empty_state() {
        let path = std::env::temp_dir().join(format!(
            "pulse-vnext-wal-{}-missing.wal",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let state = DurableLog::replay(path).expect("replay missing wal");
        assert_eq!(state, RecoveredDurableState::default());
    }
}
