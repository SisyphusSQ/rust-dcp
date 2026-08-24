use std::{fmt, net::Ipv6Addr, num::NonZeroUsize, path::PathBuf, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{DcpError, Result};

const DEFAULT_KV_PORT: u16 = 11_210;
const DEFAULT_KV_TLS_PORT: u16 = 11_207;
const DEFAULT_CONNECTION_BUFFER_SIZE: usize = 20 * 1024 * 1024;

/// Couchbase seed address.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeedAddress(String);

impl SeedAddress {
    /// Returns the original seed representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Adds a default KV port when the seed has no explicit port.
    #[must_use]
    pub fn with_default_kv_port(&self, tls: bool) -> String {
        if self.has_explicit_port() {
            return self.0.clone();
        }
        let port = if tls {
            DEFAULT_KV_TLS_PORT
        } else {
            DEFAULT_KV_PORT
        };
        if self.0.contains(':') && !self.0.starts_with('[') {
            format!("[{}]:{port}", self.0)
        } else {
            format!("{}:{port}", self.0)
        }
    }

    fn has_explicit_port(&self) -> bool {
        if self.0.starts_with('[') {
            return self
                .0
                .rfind("]:")
                .is_some_and(|index| index + 2 < self.0.len());
        }
        self.0.matches(':').count() == 1
            && self
                .0
                .rsplit_once(':')
                .is_some_and(|(_, port)| port.parse::<u16>().is_ok())
    }
}

impl FromStr for SeedAddress {
    type Err = DcpError;

    fn from_str(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_whitespace) {
            return Err(DcpError::InvalidConfiguration(
                "seed address must be non-empty and contain no whitespace".into(),
            ));
        }

        if value.contains("://") {
            return Err(DcpError::InvalidConfiguration(
                "seed address must not include a URI scheme".into(),
            ));
        }

        if value.starts_with('[') {
            let Some(end) = value.find(']') else {
                return Err(DcpError::InvalidConfiguration(
                    "bracketed IPv6 seed is missing a closing bracket".into(),
                ));
            };
            value[1..end].parse::<Ipv6Addr>().map_err(|error| {
                DcpError::InvalidConfiguration(format!("invalid IPv6 seed: {error}"))
            })?;
            validate_optional_port(&value[end + 1..])?;
        } else {
            match value.matches(':').count() {
                0 => {}
                1 => {
                    let (host, port) = value.rsplit_once(':').expect("one separator exists");
                    if host.is_empty() {
                        return Err(DcpError::InvalidConfiguration(
                            "seed host must not be empty".into(),
                        ));
                    }
                    validate_port(port)?;
                }
                _ => {
                    value.parse::<Ipv6Addr>().map_err(|error| {
                        DcpError::InvalidConfiguration(format!("invalid IPv6 seed: {error}"))
                    })?;
                }
            }
        }
        Ok(Self(value.to_owned()))
    }
}

fn validate_optional_port(suffix: &str) -> Result<()> {
    if suffix.is_empty() {
        return Ok(());
    }
    let Some(port) = suffix.strip_prefix(':') else {
        return Err(DcpError::InvalidConfiguration(
            "unexpected characters after bracketed IPv6 seed".into(),
        ));
    };
    validate_port(port)
}

fn validate_port(port: &str) -> Result<()> {
    if port.parse::<u16>().is_ok_and(|port| port != 0) {
        return Ok(());
    }
    Err(DcpError::InvalidConfiguration(format!(
        "invalid seed port {port:?}"
    )))
}

/// Username/password pair used for SASL authentication.
#[derive(Clone, Deserialize)]
pub struct Credentials {
    username: String,
    password: String,
}

impl Credentials {
    /// Creates credentials without logging or exposing the password.
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Authentication username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Exposes the password only to authentication code.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// TLS connection settings.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// Enables TLS for KV connections.
    pub enabled: bool,
    /// Optional PEM bundle added to the platform trust roots.
    pub root_ca_path: Option<PathBuf>,
    /// Optional DNS name used for certificate validation.
    pub server_name: Option<String>,
}

/// DCP subscription lifetime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DcpMode {
    /// Consume through a captured high sequence number and then finish.
    Finite,
    /// Continue consuming after the initial high sequence number.
    #[default]
    Infinite,
}

/// Initial position used when no durable checkpoint exists.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "checkpoint")]
pub enum StartPosition {
    /// Start at sequence number zero.
    #[default]
    Earliest,
    /// Start at the current high sequence number.
    Latest,
    /// Start from a caller-supplied durable checkpoint.
    Checkpoint(crate::DcpCheckpoint),
}

/// DCP stream priority advertised to the server.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DcpPriority {
    /// Low-priority connection.
    Low,
    /// Medium-priority connection.
    #[default]
    Medium,
    /// High-priority connection.
    High,
}

/// Rollback behavior selected by the integrating application.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RollbackPolicy {
    /// Pause the partition and return `RollbackRequired`.
    #[default]
    StopAndReport,
    /// Rewind to the server-provided point and replay events.
    RewindAndReplay,
    /// Emit the recovery request through the configured handler.
    DelegateToHandler,
}

/// Server-side collection filter.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionFilter {
    /// Scope name. Defaults to `_default`.
    pub scope: String,
    /// Collection names within `scope`.
    pub collections: Vec<String>,
}

impl Default for CollectionFilter {
    fn default() -> Self {
        Self {
            scope: "_default".into(),
            collections: vec!["_default".into()],
        }
    }
}

impl CollectionFilter {
    fn validate(&self) -> Result<()> {
        if self.scope.trim().is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "collection scope must not be empty".into(),
            ));
        }
        if self.collections.is_empty() || self.collections.iter().any(|name| name.trim().is_empty())
        {
            return Err(DcpError::InvalidConfiguration(
                "at least one non-empty collection is required".into(),
            ));
        }

        let unique = self
            .collections
            .iter()
            .collect::<std::collections::HashSet<_>>();
        if unique.len() != self.collections.len() {
            return Err(DcpError::InvalidConfiguration(
                "collection names must be unique".into(),
            ));
        }
        Ok(())
    }
}

/// Connection-level DCP flow-control settings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowControlConfig {
    /// Buffer size advertised using `connection_buffer_size`.
    pub connection_buffer_size: usize,
    /// Fraction of the buffer accumulated before sending `BufferAck`.
    pub ack_ratio: f32,
    /// Maximum decoded events waiting for the application.
    pub event_queue_capacity: NonZeroUsize,
    /// Server NOOP interval requested by the client.
    #[serde(with = "humantime_serde")]
    pub noop_interval: Duration,
    /// Maximum time without any inbound frame before reconnecting.
    #[serde(with = "humantime_serde")]
    pub dead_connection_timeout: Duration,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            connection_buffer_size: DEFAULT_CONNECTION_BUFFER_SIZE,
            ack_ratio: 0.2,
            event_queue_capacity: NonZeroUsize::new(2_048).expect("constant is non-zero"),
            noop_interval: Duration::from_secs(20),
            dead_connection_timeout: Duration::from_secs(60),
        }
    }
}

impl FlowControlConfig {
    fn validate(&self) -> Result<()> {
        if self.connection_buffer_size == 0 {
            return Err(DcpError::InvalidConfiguration(
                "connection buffer size must be greater than zero".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.ack_ratio) || self.ack_ratio == 0.0 {
            return Err(DcpError::InvalidConfiguration(
                "flow-control ack ratio must be in (0, 1]".into(),
            ));
        }
        if self.noop_interval.is_zero() || self.dead_connection_timeout <= self.noop_interval {
            return Err(DcpError::InvalidConfiguration(
                "dead connection timeout must be greater than the NOOP interval".into(),
            ));
        }
        Ok(())
    }
}

/// Checkpoint persistence mode.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum CheckpointMode {
    /// Flush only when requested by the application.
    Manual,
    /// Periodically flush processed contiguous positions.
    Automatic {
        /// Interval between dirty-checkpoint scans.
        #[serde(with = "humantime_serde")]
        flush_interval: Duration,
    },
}

/// Checkpoint initialization and persistence behavior.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointConfig {
    /// Automatic or manual flush behavior.
    pub mode: CheckpointMode,
    /// Maximum time allowed for one store operation.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            mode: CheckpointMode::Automatic {
                flush_interval: Duration::from_secs(60),
            },
            timeout: Duration::from_secs(60),
        }
    }
}

impl CheckpointConfig {
    fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() {
            return Err(DcpError::InvalidConfiguration(
                "checkpoint timeout must be greater than zero".into(),
            ));
        }
        if matches!(
            self.mode,
            CheckpointMode::Automatic { flush_interval } if flush_interval.is_zero()
        ) {
            return Err(DcpError::InvalidConfiguration(
                "automatic checkpoint interval must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Periodic cluster health checking.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckConfig {
    /// Enables health checks.
    pub enabled: bool,
    /// Interval between checks.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Per-check timeout.
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(10),
        }
    }
}

/// Complete configuration for one DCP client.
#[derive(Clone, Debug)]
pub struct DcpConfig {
    /// Seed nodes used for initial bootstrap.
    pub seeds: Vec<SeedAddress>,
    /// SASL credentials.
    pub credentials: Credentials,
    /// Bucket opened for DCP.
    pub bucket: String,
    /// Scope and collection filter.
    pub collections: CollectionFilter,
    /// Finite or infinite stream mode.
    pub mode: DcpMode,
    /// Initial position when no store entry exists.
    pub start_from: StartPosition,
    /// DCP flow-control settings.
    pub flow_control: FlowControlConfig,
    /// Checkpoint behavior.
    pub checkpoint: CheckpointConfig,
    /// Explicit rollback policy.
    pub rollback_policy: RollbackPolicy,
    /// DCP stream priority.
    pub priority: DcpPriority,
    /// Disables Couchbase 7.2+ Change Streams.
    pub disable_change_streams: bool,
    /// TCP/TLS connection timeout.
    pub connect_timeout: Duration,
    /// TLS settings.
    pub tls: TlsConfig,
    /// Health-check settings.
    pub health_check: HealthCheckConfig,
}

impl DcpConfig {
    /// Starts a validated configuration builder.
    #[must_use]
    pub fn builder(credentials: Credentials, bucket: impl Into<String>) -> DcpConfigBuilder {
        DcpConfigBuilder {
            config: Self {
                seeds: Vec::new(),
                credentials,
                bucket: bucket.into(),
                collections: CollectionFilter::default(),
                mode: DcpMode::default(),
                start_from: StartPosition::default(),
                flow_control: FlowControlConfig::default(),
                checkpoint: CheckpointConfig::default(),
                rollback_policy: RollbackPolicy::default(),
                priority: DcpPriority::default(),
                disable_change_streams: false,
                connect_timeout: Duration::from_secs(60),
                tls: TlsConfig::default(),
                health_check: HealthCheckConfig::default(),
            },
        }
    }

    /// Validates all cross-field invariants.
    ///
    /// # Errors
    ///
    /// Returns [`DcpError::InvalidConfiguration`] when a required value is
    /// missing or related timeout and flow-control settings are inconsistent.
    pub fn validate(&self) -> Result<()> {
        if self.seeds.is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "at least one seed is required".into(),
            ));
        }
        if self.credentials.username.trim().is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "username must not be empty".into(),
            ));
        }
        if self.credentials.password.is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "password must not be empty".into(),
            ));
        }
        if self.bucket.trim().is_empty() {
            return Err(DcpError::InvalidConfiguration(
                "bucket must not be empty".into(),
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(DcpError::InvalidConfiguration(
                "connection timeout must be greater than zero".into(),
            ));
        }
        if self.tls.server_name.as_ref().is_some_and(String::is_empty) {
            return Err(DcpError::InvalidConfiguration(
                "TLS server name must not be empty".into(),
            ));
        }
        self.collections.validate()?;
        self.flow_control.validate()?;
        self.checkpoint.validate()?;
        Ok(())
    }
}

/// Builder for [`DcpConfig`].
#[derive(Clone, Debug)]
pub struct DcpConfigBuilder {
    config: DcpConfig,
}

impl DcpConfigBuilder {
    /// Appends a seed node.
    ///
    /// # Errors
    ///
    /// Returns [`DcpError::InvalidConfiguration`] when `seed` is empty or
    /// contains whitespace.
    pub fn seed(mut self, seed: impl AsRef<str>) -> Result<Self> {
        self.config.seeds.push(seed.as_ref().parse()?);
        Ok(self)
    }

    /// Replaces the collection filter.
    #[must_use]
    pub fn collections(mut self, filter: CollectionFilter) -> Self {
        self.config.collections = filter;
        self
    }

    /// Selects finite or infinite mode.
    #[must_use]
    pub const fn mode(mut self, mode: DcpMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Selects the initial position.
    #[must_use]
    pub fn start_from(mut self, position: StartPosition) -> Self {
        self.config.start_from = position;
        self
    }

    /// Replaces flow-control settings.
    #[must_use]
    pub fn flow_control(mut self, flow_control: FlowControlConfig) -> Self {
        self.config.flow_control = flow_control;
        self
    }

    /// Replaces checkpoint settings.
    #[must_use]
    pub fn checkpoint(mut self, checkpoint: CheckpointConfig) -> Self {
        self.config.checkpoint = checkpoint;
        self
    }

    /// Selects rollback handling.
    #[must_use]
    pub const fn rollback_policy(mut self, policy: RollbackPolicy) -> Self {
        self.config.rollback_policy = policy;
        self
    }

    /// Selects DCP priority.
    #[must_use]
    pub const fn priority(mut self, priority: DcpPriority) -> Self {
        self.config.priority = priority;
        self
    }

    /// Enables or disables Couchbase Change Streams.
    #[must_use]
    pub const fn disable_change_streams(mut self, disabled: bool) -> Self {
        self.config.disable_change_streams = disabled;
        self
    }

    /// Replaces TLS settings.
    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.config.tls = tls;
        self
    }

    /// Builds and validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`DcpError::InvalidConfiguration`] when the assembled
    /// configuration violates any required invariant.
    pub fn build(self) -> Result<DcpConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_do_not_leak_password_in_debug() {
        let credentials = Credentials::new("alice", "correct horse battery staple");
        let rendered = format!("{credentials:?}");

        assert!(rendered.contains("alice"));
        assert!(!rendered.contains("correct horse"));
    }

    #[test]
    fn builder_rejects_missing_seed() {
        let error = DcpConfig::builder(Credentials::new("alice", "secret"), "bucket")
            .build()
            .expect_err("missing seed must fail");

        assert!(matches!(error, DcpError::InvalidConfiguration(_)));
    }

    #[test]
    fn builder_produces_go_dcp_compatible_defaults() {
        let config = DcpConfig::builder(Credentials::new("alice", "secret"), "bucket")
            .seed("cb.example.test")
            .expect("seed is valid")
            .build()
            .expect("config is valid");

        assert_eq!(config.mode, DcpMode::Infinite);
        assert_eq!(config.collections, CollectionFilter::default());
        assert_eq!(config.rollback_policy, RollbackPolicy::StopAndReport);
        assert_eq!(
            config.seeds[0].with_default_kv_port(false),
            "cb.example.test:11210"
        );
    }

    #[test]
    fn duplicate_collection_filter_is_rejected() {
        let result = DcpConfig::builder(Credentials::new("alice", "secret"), "bucket")
            .seed("localhost")
            .expect("seed is valid")
            .collections(CollectionFilter {
                scope: "inventory".into(),
                collections: vec!["airline".into(), "airline".into()],
            })
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn ipv6_seed_gets_brackets_and_default_port() {
        let seed: SeedAddress = "2001:db8::1".parse().expect("valid seed");
        assert_eq!(seed.with_default_kv_port(true), "[2001:db8::1]:11207");
    }

    #[test]
    fn invalid_explicit_seed_port_is_rejected() {
        assert!("cb.example.test:70000".parse::<SeedAddress>().is_err());
        assert!("[2001:db8::1]:0".parse::<SeedAddress>().is_err());
    }
}
