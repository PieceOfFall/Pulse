use std::{
    env,
    error::Error,
    ffi::OsString,
    fmt, fs, io,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde::Deserialize;

use crate::broker::runtime::config::BrokerConfig;

const CONFIG_FILE_NAME: &str = "Broker.toml";

#[derive(Clone, Debug)]
pub(crate) struct AppConfig {
    pub(crate) server: ServerConfig,
    pub(crate) storage: StorageConfig,
    pub(crate) limits: BrokerConfig,
    pub(crate) observability: ObservabilityConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct ServerConfig {
    pub(crate) bind: String,
    pub(crate) outbound_queue_size: usize,
    pub(crate) tls: ServerTlsConfig,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ServerTlsConfig {
    pub(crate) enabled: bool,
    pub(crate) certificate_chain: Option<PathBuf>,
    pub(crate) private_key: Option<PathBuf>,
    pub(crate) client_auth: TlsClientAuth,
    pub(crate) client_ca: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TlsClientAuth {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Debug)]
pub(crate) struct ParseTlsClientAuthError {
    value: String,
}

impl fmt::Display for ParseTlsClientAuthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "server.tls.client_auth must be one of disabled, optional, required; got `{}`",
            self.value
        )
    }
}

impl Error for ParseTlsClientAuthError {}

impl FromStr for TlsClientAuth {
    type Err = ParseTlsClientAuthError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "optional" => Ok(Self::Optional),
            "required" => Ok(Self::Required),
            _ => Err(ParseTlsClientAuthError {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StorageConfig {
    pub(crate) sqlite: Option<String>,
    pub(crate) mysql: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ObservabilityConfig {
    pub(crate) log: Option<String>,
    pub(crate) metrics_bind: Option<String>,
}

#[derive(Default, Deserialize)]
struct FileConfig {
    server: Option<ServerFileConfig>,
    storage: Option<StorageFileConfig>,
    limits: Option<LimitsFileConfig>,
    observability: Option<ObservabilityFileConfig>,
}

#[derive(Default, Deserialize)]
struct ServerFileConfig {
    bind: Option<String>,
    outbound_queue_size: Option<usize>,
    tls: Option<ServerTlsFileConfig>,
}

#[derive(Default, Deserialize)]
struct ServerTlsFileConfig {
    enabled: Option<bool>,
    certificate_chain: Option<PathBuf>,
    private_key: Option<PathBuf>,
    client_auth: Option<TlsClientAuth>,
    client_ca: Option<PathBuf>,
}

#[derive(Default, Deserialize)]
struct StorageFileConfig {
    sqlite: Option<String>,
    mysql: Option<String>,
}

#[derive(Default, Deserialize)]
struct LimitsFileConfig {
    server_receive_maximum: Option<u16>,
    server_maximum_packet_size: Option<u32>,
    server_topic_alias_maximum: Option<u16>,
    max_subscriptions_per_client: Option<usize>,
    max_offline_queue_len: Option<usize>,
    max_retained_messages: Option<usize>,
    max_retained_payload_bytes: Option<usize>,
}

#[derive(Default, Deserialize)]
struct ObservabilityFileConfig {
    log: Option<String>,
    metrics_bind: Option<String>,
}

#[derive(Default)]
struct CliConfig {
    config_path: Option<PathBuf>,
    server: ServerFileConfig,
    storage: StorageFileConfig,
    limits: LimitsFileConfig,
    observability: ObservabilityFileConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: "0.0.0.0:1883".to_string(),
                outbound_queue_size: 1024,
                tls: ServerTlsConfig::default(),
            },
            storage: default_storage_config(),
            limits: BrokerConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl AppConfig {
    pub(crate) fn load() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::load_from_args(env::args_os())
    }

    fn load_from_args(
        args: impl IntoIterator<Item = OsString>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let cli = parse_cli(args)?;
        let config_path = cli.config_path.clone().unwrap_or_else(default_config_path);
        let config_path_is_explicit = cli.config_path.is_some();

        let mut config = Self::default();
        if let Some(file_config) = read_file_config(&config_path, config_path_is_explicit)? {
            config.apply_file(file_config);
        }
        config.apply_env()?;
        config.apply_cli(cli);
        config.validate()?;
        Ok(config)
    }

    fn apply_file(&mut self, file: FileConfig) {
        if let Some(server) = file.server {
            self.apply_server(server);
        }
        if let Some(storage) = file.storage {
            self.apply_storage(storage);
        }
        if let Some(limits) = file.limits {
            self.apply_limits(limits);
        }
        if let Some(observability) = file.observability {
            self.apply_observability(observability);
        }
    }

    fn apply_env(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.apply_server(ServerFileConfig {
            bind: env_string("MQTT_RS_BIND"),
            outbound_queue_size: env_parse("MQTT_RS_OUTBOUND_QUEUE_SIZE")?,
            tls: Some(ServerTlsFileConfig {
                enabled: env_parse("MQTT_RS_TLS_ENABLED")?,
                certificate_chain: env_path("MQTT_RS_TLS_CERTIFICATE_CHAIN"),
                private_key: env_path("MQTT_RS_TLS_PRIVATE_KEY"),
                client_auth: env_parse("MQTT_RS_TLS_CLIENT_AUTH")?,
                client_ca: env_path("MQTT_RS_TLS_CLIENT_CA"),
            }),
        });
        self.apply_storage(StorageFileConfig {
            sqlite: env_string("MQTT_RS_SQLITE"),
            mysql: env_string("MQTT_RS_MYSQL"),
        });
        self.apply_limits(LimitsFileConfig {
            server_receive_maximum: env_parse("MQTT_RS_SERVER_RECEIVE_MAXIMUM")?,
            server_maximum_packet_size: env_parse("MQTT_RS_SERVER_MAXIMUM_PACKET_SIZE")?,
            server_topic_alias_maximum: env_parse("MQTT_RS_SERVER_TOPIC_ALIAS_MAXIMUM")?,
            max_subscriptions_per_client: env_parse("MQTT_RS_MAX_SUBSCRIPTIONS_PER_CLIENT")?,
            max_offline_queue_len: env_parse("MQTT_RS_MAX_OFFLINE_QUEUE_LEN")?,
            max_retained_messages: env_parse("MQTT_RS_MAX_RETAINED_MESSAGES")?,
            max_retained_payload_bytes: env_parse("MQTT_RS_MAX_RETAINED_PAYLOAD_BYTES")?,
        });
        self.apply_observability(ObservabilityFileConfig {
            log: env_string("MQTT_RS_LOG").or_else(|| env_string("RUST_LOG")),
            metrics_bind: env_string("MQTT_RS_METRICS_BIND"),
        });
        Ok(())
    }

    fn apply_cli(&mut self, cli: CliConfig) {
        self.apply_server(cli.server);
        self.apply_storage(cli.storage);
        self.apply_limits(cli.limits);
        self.apply_observability(cli.observability);
    }

    fn apply_server(&mut self, server: ServerFileConfig) {
        if let Some(bind) = server.bind {
            self.server.bind = bind;
        }
        if let Some(outbound_queue_size) = server.outbound_queue_size {
            self.server.outbound_queue_size = outbound_queue_size;
        }
        if let Some(tls) = server.tls {
            self.apply_server_tls(tls);
        }
    }

    fn apply_server_tls(&mut self, tls: ServerTlsFileConfig) {
        if let Some(enabled) = tls.enabled {
            self.server.tls.enabled = enabled;
        }
        if let Some(certificate_chain) = tls.certificate_chain {
            self.server.tls.certificate_chain = Some(certificate_chain);
        }
        if let Some(private_key) = tls.private_key {
            self.server.tls.private_key = Some(private_key);
        }
        if let Some(client_auth) = tls.client_auth {
            self.server.tls.client_auth = client_auth;
        }
        if let Some(client_ca) = tls.client_ca {
            self.server.tls.client_ca = Some(client_ca);
        }
    }

    fn apply_storage(&mut self, storage: StorageFileConfig) {
        if let Some(sqlite) = storage.sqlite {
            self.storage.sqlite = Some(sqlite);
        }
        if let Some(mysql) = storage.mysql {
            self.storage.mysql = Some(mysql);
        }
    }

    fn apply_limits(&mut self, limits: LimitsFileConfig) {
        if let Some(value) = limits.server_receive_maximum {
            self.limits.server_receive_maximum = value;
        }
        if let Some(value) = limits.server_maximum_packet_size {
            self.limits.server_maximum_packet_size = value;
        }
        if let Some(value) = limits.server_topic_alias_maximum {
            self.limits.server_topic_alias_maximum = value;
        }
        if let Some(value) = limits.max_subscriptions_per_client {
            self.limits.max_subscriptions_per_client = value;
        }
        if let Some(value) = limits.max_offline_queue_len {
            self.limits.max_offline_queue_len = value;
        }
        if let Some(value) = limits.max_retained_messages {
            self.limits.max_retained_messages = value;
        }
        if let Some(value) = limits.max_retained_payload_bytes {
            self.limits.max_retained_payload_bytes = value;
        }
    }

    fn apply_observability(&mut self, observability: ObservabilityFileConfig) {
        if let Some(log) = observability.log {
            self.observability.log = Some(log);
        }
        if let Some(metrics_bind) = observability.metrics_bind {
            self.observability.metrics_bind = Some(metrics_bind);
        }
    }

    fn validate(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.server.outbound_queue_size == 0 {
            return Err("server.outbound_queue_size must be greater than 0".into());
        }
        if self.limits.server_receive_maximum == 0 {
            return Err("limits.server_receive_maximum must be greater than 0".into());
        }
        if self.limits.server_maximum_packet_size == 0 {
            return Err("limits.server_maximum_packet_size must be greater than 0".into());
        }
        if self.storage.sqlite.is_some() && self.storage.mysql.is_some() {
            return Err("storage.sqlite and storage.mysql are mutually exclusive".into());
        }
        if self.server.tls.enabled {
            if self.server.tls.certificate_chain.is_none() {
                return Err(
                    "server.tls.certificate_chain is required when server.tls.enabled is true"
                        .into(),
                );
            }
            if self.server.tls.private_key.is_none() {
                return Err(
                    "server.tls.private_key is required when server.tls.enabled is true".into(),
                );
            }
            if matches!(
                self.server.tls.client_auth,
                TlsClientAuth::Optional | TlsClientAuth::Required
            ) && self.server.tls.client_ca.is_none()
            {
                return Err(
                    "server.tls.client_ca is required when server.tls.client_auth is optional or required"
                        .into(),
                );
            }
        }
        Ok(())
    }
}

fn default_config_path() -> PathBuf {
    env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."))
        .join(CONFIG_FILE_NAME)
}

#[cfg(windows)]
fn default_storage_config() -> StorageConfig {
    StorageConfig {
        sqlite: env::var_os("ProgramData").map(PathBuf::from).map(|path| {
            path.join("Pulse")
                .join("broker.db")
                .to_string_lossy()
                .into_owned()
        }),
        mysql: None,
    }
}

#[cfg(not(windows))]
fn default_storage_config() -> StorageConfig {
    StorageConfig::default()
}

fn read_file_config(
    path: &Path,
    explicit: bool,
) -> Result<Option<FileConfig>, Box<dyn Error + Send + Sync>> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(Some(toml::from_str(&contents)?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound && !explicit => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
}

fn env_string(name: &str) -> Option<String> {
    env::var(name).ok()
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}

fn env_parse<T>(name: &str) -> Result<Option<T>, Box<dyn Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    env::var(name)
        .ok()
        .map(|value| value.parse::<T>().map_err(|error| Box::new(error) as _))
        .transpose()
}

fn parse_cli(
    args: impl IntoIterator<Item = OsString>,
) -> Result<CliConfig, Box<dyn Error + Send + Sync>> {
    let mut config = CliConfig::default();
    let mut args = args.into_iter();
    let _ = args.next();

    while let Some(arg) = args.next() {
        let arg = arg.to_string_lossy();
        match arg.as_ref() {
            "-c" | "--config" => config.config_path = Some(next_path(&mut args, arg.as_ref())?),
            "--bind" => config.server.bind = Some(next_string(&mut args, arg.as_ref())?),
            "--tls" => tls_config_mut(&mut config.server).enabled = Some(true),
            "--no-tls" => tls_config_mut(&mut config.server).enabled = Some(false),
            "--tls-certificate-chain" => {
                tls_config_mut(&mut config.server).certificate_chain =
                    Some(next_path(&mut args, arg.as_ref())?)
            }
            "--tls-private-key" => {
                tls_config_mut(&mut config.server).private_key =
                    Some(next_path(&mut args, arg.as_ref())?)
            }
            "--tls-client-auth" => {
                tls_config_mut(&mut config.server).client_auth =
                    Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--tls-client-ca" => {
                tls_config_mut(&mut config.server).client_ca =
                    Some(next_path(&mut args, arg.as_ref())?)
            }
            "--sqlite" => config.storage.sqlite = Some(next_string(&mut args, arg.as_ref())?),
            "--mysql" => config.storage.mysql = Some(next_string(&mut args, arg.as_ref())?),
            "--outbound-queue-size" => {
                config.server.outbound_queue_size = Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--metrics-bind" => {
                config.observability.metrics_bind = Some(next_string(&mut args, arg.as_ref())?)
            }
            "--log" => config.observability.log = Some(next_string(&mut args, arg.as_ref())?),
            "--server-receive-maximum" => {
                config.limits.server_receive_maximum = Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--server-maximum-packet-size" => {
                config.limits.server_maximum_packet_size =
                    Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--server-topic-alias-maximum" => {
                config.limits.server_topic_alias_maximum =
                    Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--max-subscriptions-per-client" => {
                config.limits.max_subscriptions_per_client =
                    Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--max-offline-queue-len" => {
                config.limits.max_offline_queue_len = Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--max-retained-messages" => {
                config.limits.max_retained_messages = Some(next_parse(&mut args, arg.as_ref())?)
            }
            "--max-retained-payload-bytes" => {
                config.limits.max_retained_payload_bytes =
                    Some(next_parse(&mut args, arg.as_ref())?)
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    Ok(config)
}

fn tls_config_mut(server: &mut ServerFileConfig) -> &mut ServerTlsFileConfig {
    server.tls.get_or_insert_with(ServerTlsFileConfig::default)
}

fn next_path(
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    Ok(PathBuf::from(next_os(args, flag)?))
}

fn next_string(
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    next_os(args, flag)?
        .into_string()
        .map_err(|_| format!("{flag} value must be valid UTF-8").into())
}

fn next_parse<T>(
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    next_string(args, flag)?
        .parse::<T>()
        .map_err(|error| Box::new(error) as _)
}

fn next_os(
    args: &mut impl Iterator<Item = OsString>,
    flag: &str,
) -> Result<OsString, Box<dyn Error + Send + Sync>> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        ffi::OsString,
        path::PathBuf,
        sync::{Mutex, MutexGuard},
    };

    use super::{AppConfig, FileConfig, TlsClientAuth};
    use crate::broker::runtime::config::{
        MAX_OFFLINE_QUEUE_LEN, MAX_RETAINED_MESSAGES, MAX_RETAINED_PAYLOAD_BYTES,
        MAX_SUBSCRIPTIONS_PER_CLIENT, SERVER_MAXIMUM_PACKET_SIZE, SERVER_RECEIVE_MAXIMUM,
        SERVER_TOPIC_ALIAS_MAXIMUM,
    };

    const TLS_ENV_KEYS: &[&str] = &[
        "MQTT_RS_TLS_ENABLED",
        "MQTT_RS_TLS_CERTIFICATE_CHAIN",
        "MQTT_RS_TLS_PRIVATE_KEY",
        "MQTT_RS_TLS_CLIENT_AUTH",
        "MQTT_RS_TLS_CLIENT_CA",
    ];

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvGuard {
        fn clear_tls() -> Self {
            Self::set_tls(&[])
        }

        fn set_tls(vars: &[(&'static str, &str)]) -> Self {
            let lock = ENV_LOCK.lock().expect("environment lock");
            let saved = TLS_ENV_KEYS
                .iter()
                .map(|name| (*name, env::var_os(name)))
                .collect();
            for name in TLS_ENV_KEYS {
                unsafe {
                    env::remove_var(name);
                }
            }
            for (name, value) in vars {
                unsafe {
                    env::set_var(name, value);
                }
            }

            Self { _lock: lock, saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in &self.saved {
                match value {
                    Some(value) => unsafe {
                        env::set_var(name, value);
                    },
                    None => unsafe {
                        env::remove_var(name);
                    },
                }
            }
        }
    }

    #[test]
    fn cli_overrides_config_file_values() {
        let _env = EnvGuard::clear_tls();
        let args = [
            "Pulse",
            "--bind",
            "127.0.0.1:1884",
            "--server-receive-maximum",
            "16",
        ]
        .into_iter()
        .map(OsString::from);
        let config = AppConfig::load_from_args(args).expect("load config");

        assert_eq!(config.server.bind, "127.0.0.1:1884");
        assert_eq!(config.limits.server_receive_maximum, 16);
    }

    #[test]
    fn defaults_match_runtime_constants() {
        let config = AppConfig::default();
        assert!(!config.server.tls.enabled);
        assert!(config.server.tls.certificate_chain.is_none());
        assert!(config.server.tls.private_key.is_none());
        assert_eq!(config.server.tls.client_auth, TlsClientAuth::Disabled);
        assert!(config.server.tls.client_ca.is_none());
        assert_eq!(config.limits.server_receive_maximum, SERVER_RECEIVE_MAXIMUM);
        assert_eq!(
            config.limits.server_maximum_packet_size,
            SERVER_MAXIMUM_PACKET_SIZE
        );
        assert_eq!(
            config.limits.server_topic_alias_maximum,
            SERVER_TOPIC_ALIAS_MAXIMUM
        );
        assert_eq!(
            config.limits.max_subscriptions_per_client,
            MAX_SUBSCRIPTIONS_PER_CLIENT
        );
        assert_eq!(config.limits.max_offline_queue_len, MAX_OFFLINE_QUEUE_LEN);
        assert_eq!(config.limits.max_retained_messages, MAX_RETAINED_MESSAGES);
        assert_eq!(
            config.limits.max_retained_payload_bytes,
            MAX_RETAINED_PAYLOAD_BYTES
        );
    }

    #[test]
    fn file_config_enables_tls_with_required_client_auth() {
        let file: FileConfig = toml::from_str(
            r#"
            [server.tls]
            enabled = true
            certificate_chain = "certs/server-chain.pem"
            private_key = "certs/server-key.pem"
            client_auth = "required"
            client_ca = "certs/client-ca.pem"
            "#,
        )
        .expect("parse config");

        let mut config = AppConfig::default();
        config.apply_file(file);
        config.validate().expect("valid TLS config");

        assert!(config.server.tls.enabled);
        assert_eq!(
            config.server.tls.certificate_chain,
            Some(PathBuf::from("certs/server-chain.pem"))
        );
        assert_eq!(
            config.server.tls.private_key,
            Some(PathBuf::from("certs/server-key.pem"))
        );
        assert_eq!(config.server.tls.client_auth, TlsClientAuth::Required);
        assert_eq!(
            config.server.tls.client_ca,
            Some(PathBuf::from("certs/client-ca.pem"))
        );
    }

    #[test]
    fn env_overrides_tls_config() {
        let _env = EnvGuard::set_tls(&[
            ("MQTT_RS_TLS_ENABLED", "true"),
            ("MQTT_RS_TLS_CERTIFICATE_CHAIN", "env/server-chain.pem"),
            ("MQTT_RS_TLS_PRIVATE_KEY", "env/server-key.pem"),
            ("MQTT_RS_TLS_CLIENT_AUTH", "optional"),
            ("MQTT_RS_TLS_CLIENT_CA", "env/client-ca.pem"),
        ]);
        let args = ["Pulse"].into_iter().map(OsString::from);
        let config = AppConfig::load_from_args(args).expect("load config");

        assert!(config.server.tls.enabled);
        assert_eq!(
            config.server.tls.certificate_chain,
            Some(PathBuf::from("env/server-chain.pem"))
        );
        assert_eq!(
            config.server.tls.private_key,
            Some(PathBuf::from("env/server-key.pem"))
        );
        assert_eq!(config.server.tls.client_auth, TlsClientAuth::Optional);
        assert_eq!(
            config.server.tls.client_ca,
            Some(PathBuf::from("env/client-ca.pem"))
        );
    }

    #[test]
    fn cli_overrides_tls_config() {
        let _env = EnvGuard::clear_tls();
        let args = [
            "Pulse",
            "--tls",
            "--tls-certificate-chain",
            "cli/server-chain.pem",
            "--tls-private-key",
            "cli/server-key.pem",
            "--tls-client-auth",
            "required",
            "--tls-client-ca",
            "cli/client-ca.pem",
        ]
        .into_iter()
        .map(OsString::from);
        let config = AppConfig::load_from_args(args).expect("load config");

        assert!(config.server.tls.enabled);
        assert_eq!(
            config.server.tls.certificate_chain,
            Some(PathBuf::from("cli/server-chain.pem"))
        );
        assert_eq!(
            config.server.tls.private_key,
            Some(PathBuf::from("cli/server-key.pem"))
        );
        assert_eq!(config.server.tls.client_auth, TlsClientAuth::Required);
        assert_eq!(
            config.server.tls.client_ca,
            Some(PathBuf::from("cli/client-ca.pem"))
        );
    }

    #[test]
    fn rejects_invalid_tls_client_auth() {
        let _env = EnvGuard::clear_tls();
        let args = ["Pulse", "--tls-client-auth", "sometimes"]
            .into_iter()
            .map(OsString::from);
        let error = AppConfig::load_from_args(args).expect_err("invalid config");

        assert!(error.to_string().contains("server.tls.client_auth"));
    }

    #[test]
    fn rejects_enabled_tls_without_certificate_or_private_key() {
        let _env = EnvGuard::clear_tls();
        let missing_certificate = [
            "Pulse",
            "--tls",
            "--tls-private-key",
            "certs/server-key.pem",
        ]
        .into_iter()
        .map(OsString::from);
        let error = AppConfig::load_from_args(missing_certificate).expect_err("invalid TLS config");
        assert!(error.to_string().contains("server.tls.certificate_chain"));

        let missing_private_key = [
            "Pulse",
            "--tls",
            "--tls-certificate-chain",
            "certs/server-chain.pem",
        ]
        .into_iter()
        .map(OsString::from);
        let error = AppConfig::load_from_args(missing_private_key).expect_err("invalid TLS config");
        assert!(error.to_string().contains("server.tls.private_key"));
    }

    #[test]
    fn rejects_mtls_without_client_ca() {
        let _env = EnvGuard::clear_tls();
        let args = [
            "Pulse",
            "--tls",
            "--tls-certificate-chain",
            "certs/server-chain.pem",
            "--tls-private-key",
            "certs/server-key.pem",
            "--tls-client-auth",
            "required",
        ]
        .into_iter()
        .map(OsString::from);
        let error = AppConfig::load_from_args(args).expect_err("invalid TLS config");

        assert!(error.to_string().contains("server.tls.client_ca"));
    }

    #[test]
    fn rejects_invalid_zero_receive_maximum() {
        let _env = EnvGuard::clear_tls();
        let args = ["Pulse", "--server-receive-maximum", "0"]
            .into_iter()
            .map(OsString::from);
        let error = AppConfig::load_from_args(args).expect_err("invalid config");

        assert!(error.to_string().contains("limits.server_receive_maximum"));
    }

    #[test]
    fn rejects_multiple_storage_backends() {
        let _env = EnvGuard::clear_tls();
        let args = [
            "Pulse",
            "--sqlite",
            "broker.db",
            "--mysql",
            "mysql://user:password@localhost/mqtt",
        ]
        .into_iter()
        .map(OsString::from);
        let error = AppConfig::load_from_args(args).expect_err("invalid config");

        assert!(
            error
                .to_string()
                .contains("storage.sqlite and storage.mysql")
        );
    }
}
