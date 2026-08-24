use std::{collections::BTreeSet, time::Duration};

use rust_dcp_protocol::{
    DcpOpenFlags, Frame, HelloFeature, Opcode, ProtocolError, Status, dcp_control, dcp_open, hello,
    select_bucket,
};

use crate::{
    DcpConfig, DcpError, DcpPriority, KvConnection, Result, SaslMechanism, auth::authenticate,
};

const REQUESTED_HELLO_FEATURES: &[HelloFeature] = &[
    HelloFeature::Datatype,
    HelloFeature::Tls,
    HelloFeature::SeqNo,
    HelloFeature::Xattr,
    HelloFeature::ExtendedErrors,
    HelloFeature::SelectBucket,
    HelloFeature::Json,
    HelloFeature::Duplex,
    HelloFeature::ClusterMapNotifications,
    HelloFeature::AltRequest,
    HelloFeature::Collections,
];

/// Features confirmed while bootstrapping one Couchbase KV connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapCapabilities {
    /// HELLO features accepted by the server and understood by this client.
    pub hello_features: BTreeSet<HelloFeature>,
    /// SASL mechanism used to authenticate the connection.
    pub sasl_mechanism: SaslMechanism,
    /// Optional DCP controls accepted by the server.
    pub dcp_controls: BTreeSet<DcpControlFeature>,
}

/// Optional DCP controls accepted during bootstrap.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DcpControlFeature {
    /// Closing a client stream produces a server stream-end event.
    StreamEndOnClose,
    /// Couchbase Change Streams are enabled.
    ChangeStreams,
    /// Expiration events use the dedicated expiration opcode.
    ExpiryOpcode,
    /// DCP stream IDs are enabled.
    StreamId,
}

impl BootstrapCapabilities {
    /// Returns whether a HELLO feature was negotiated.
    #[must_use]
    pub fn supports(&self, feature: HelloFeature) -> bool {
        self.hello_features.contains(&feature)
    }

    /// Returns whether an optional DCP control was accepted.
    #[must_use]
    pub fn supports_control(&self, feature: DcpControlFeature) -> bool {
        self.dcp_controls.contains(&feature)
    }
}

/// Authenticated, bucket-selected, and DCP-configured KV connection.
#[derive(Debug)]
pub struct DcpConnection {
    connection: KvConnection,
    capabilities: BootstrapCapabilities,
}

impl DcpConnection {
    #[cfg(test)]
    pub(crate) fn from_test_parts(
        connection: KvConnection,
        capabilities: BootstrapCapabilities,
    ) -> Self {
        Self {
            connection,
            capabilities,
        }
    }

    /// Negotiated connection capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> &BootstrapCapabilities {
        &self.capabilities
    }

    /// Mutable access used by topology discovery and the stream runtime.
    #[must_use]
    pub fn connection_mut(&mut self) -> &mut KvConnection {
        &mut self.connection
    }

    /// Releases the configured KV connection.
    #[must_use]
    pub fn into_inner(self) -> KvConnection {
        self.connection
    }
}

/// Connects to the configured seeds and performs the complete asynchronous KV
/// and DCP bootstrap sequence on the first usable endpoint.
///
/// # Errors
///
/// Returns a configuration error before dialing, or a summarized bootstrap
/// error after all configured seeds fail.
pub async fn bootstrap_connection(
    config: &DcpConfig,
    client_name: &str,
    connection_name: &str,
) -> Result<DcpConnection> {
    config.validate()?;
    validate_name("HELLO client", client_name)?;
    validate_name("DCP connection", connection_name)?;

    let mut failures = Vec::with_capacity(config.seeds.len());
    for seed in &config.seeds {
        let peer = seed.with_default_kv_port(config.tls.enabled);
        match KvConnection::connect(seed, &config.tls, config.connect_timeout).await {
            Ok(connection) => {
                match bootstrap_on_connection(connection, config, client_name, connection_name)
                    .await
                {
                    Ok(connection) => return Ok(connection),
                    Err(error) => failures.push(format!("{peer}: {error}")),
                }
            }
            Err(error) => failures.push(format!("{peer}: {error}")),
        }
    }

    Err(DcpError::Topology(format!(
        "all KV seeds failed bootstrap: {}",
        failures.join("; ")
    )))
}

/// Performs HELLO, SASL, bucket selection, DCP open, and DCP controls on an
/// already established Tokio connection.
///
/// This entry point supports custom transports and deterministic unit tests.
///
/// # Errors
///
/// Returns a protocol, authentication, server-status, or configuration error
/// when any required bootstrap step fails.
pub async fn bootstrap_on_connection(
    mut connection: KvConnection,
    config: &DcpConfig,
    client_name: &str,
    connection_name: &str,
) -> Result<DcpConnection> {
    config.validate()?;
    validate_name("HELLO client", client_name)?;
    validate_name("DCP connection", connection_name)?;

    let hello_response = connection
        .request(hello(client_name, REQUESTED_HELLO_FEATURES, 0))
        .await?;
    ensure_success(&hello_response, Opcode::HELLO, "HELLO feature negotiation")?;
    let hello_features = parse_hello_features(&hello_response)?;
    connection.set_collections_enabled(hello_features.contains(&HelloFeature::Collections));

    let sasl_mechanism = authenticate(&mut connection, &config.credentials).await?;
    let response = connection.request(select_bucket(&config.bucket, 0)).await?;
    ensure_success(&response, Opcode::SELECT_BUCKET, "bucket selection")?;

    let mut open_flags = DcpOpenFlags::default();
    if hello_features.contains(&HelloFeature::Xattr) {
        open_flags |= DcpOpenFlags::INCLUDE_XATTRS;
    }
    let response = connection
        .request(dcp_open(connection_name, open_flags, 0))
        .await?;
    ensure_success(&response, Opcode::DCP_OPEN, "DCP connection open")?;

    control_required(&mut connection, "enable_noop", "true").await?;
    control_required(
        &mut connection,
        "set_noop_interval",
        &duration_seconds(config.flow_control.noop_interval),
    )
    .await?;
    control_required(
        &mut connection,
        "set_priority",
        priority_name(config.priority),
    )
    .await?;

    let change_streams = if config.disable_change_streams {
        false
    } else {
        control_optional(&mut connection, "change_streams", "true").await?
    };
    let expiry_opcode = control_optional(&mut connection, "enable_expiry_opcode", "true").await?;
    let stream_id = if hello_features.contains(&HelloFeature::Collections) {
        control_optional(&mut connection, "enable_stream_id", "true").await?
    } else {
        false
    };

    control_required(
        &mut connection,
        "connection_buffer_size",
        &config.flow_control.connection_buffer_size.to_string(),
    )
    .await?;
    let stream_end_on_close = control_optional(
        &mut connection,
        "send_stream_end_on_client_close_stream",
        "true",
    )
    .await?;

    let mut dcp_controls = BTreeSet::new();
    for (enabled, feature) in [
        (stream_end_on_close, DcpControlFeature::StreamEndOnClose),
        (change_streams, DcpControlFeature::ChangeStreams),
        (expiry_opcode, DcpControlFeature::ExpiryOpcode),
        (stream_id, DcpControlFeature::StreamId),
    ] {
        if enabled {
            dcp_controls.insert(feature);
        }
    }

    Ok(DcpConnection {
        connection,
        capabilities: BootstrapCapabilities {
            hello_features,
            sasl_mechanism,
            dcp_controls,
        },
    })
}

fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(DcpError::InvalidConfiguration(format!(
            "{kind} name must not be empty"
        )));
    }
    Ok(())
}

fn parse_hello_features(response: &Frame) -> Result<BTreeSet<HelloFeature>> {
    if response.value.len() % 2 != 0 {
        return Err(ProtocolError::MalformedFrame(format!(
            "HELLO response feature payload has odd length {}",
            response.value.len()
        ))
        .into());
    }

    Ok(response
        .value
        .chunks_exact(2)
        .filter_map(|bytes| HelloFeature::from_u16(u16::from_be_bytes([bytes[0], bytes[1]])))
        .collect())
}

async fn control_required(connection: &mut KvConnection, key: &str, value: &str) -> Result<()> {
    let response = connection.request(dcp_control(key, value, 0)).await?;
    ensure_success(
        &response,
        Opcode::DCP_CONTROL,
        &format!("DCP control {key}"),
    )
}

async fn control_optional(connection: &mut KvConnection, key: &str, value: &str) -> Result<bool> {
    let response = connection.request(dcp_control(key, value, 0)).await?;
    ensure_opcode(&response, Opcode::DCP_CONTROL)?;
    if response.status.is_success() {
        return Ok(true);
    }
    if matches!(
        response.status,
        Status::INVALID_ARGUMENTS | Status::UNKNOWN_COMMAND | Status::NOT_SUPPORTED
    ) {
        return Ok(false);
    }
    Err(server_status_error(
        &response,
        &format!("DCP control {key}"),
    ))
}

fn ensure_success(response: &Frame, opcode: Opcode, context: &str) -> Result<()> {
    ensure_opcode(response, opcode)?;
    if response.status.is_success() {
        Ok(())
    } else {
        Err(server_status_error(response, context))
    }
}

fn ensure_opcode(response: &Frame, expected: Opcode) -> Result<()> {
    if !response.magic.is_response() || response.opcode != expected {
        return Err(ProtocolError::MalformedFrame(format!(
            "expected response opcode 0x{:02x}, got magic 0x{:02x} opcode 0x{:02x}",
            expected.as_u8(),
            response.magic.as_u8(),
            response.opcode.as_u8()
        ))
        .into());
    }
    Ok(())
}

fn server_status_error(response: &Frame, context: &str) -> DcpError {
    let detail = String::from_utf8_lossy(&response.value);
    let message = if detail.is_empty() {
        context.to_owned()
    } else {
        format!("{context}: {detail}")
    };
    DcpError::ServerStatus {
        status: response.status.as_u16(),
        opcode: response.opcode.as_u8(),
        message,
    }
}

const fn priority_name(priority: DcpPriority) -> &'static str {
    match priority {
        DcpPriority::Low => "low",
        DcpPriority::Medium => "medium",
        DcpPriority::High => "high",
    }
}

fn duration_seconds(duration: Duration) -> String {
    duration.as_secs().max(1).to_string()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use bytes::{BufMut, Bytes, BytesMut};
    use futures_util::{SinkExt, StreamExt};
    use rust_dcp_protocol::{FrameCodec, Magic};
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use super::*;
    use crate::Credentials;

    fn test_config() -> DcpConfig {
        DcpConfig::builder(Credentials::new("alice", "secret"), "travel")
            .seed("127.0.0.1")
            .unwrap()
            .build()
            .unwrap()
    }

    fn success_response(request: &Frame) -> Frame {
        Frame::success_response_to(request)
    }

    #[tokio::test]
    async fn bootstrap_negotiates_plain_auth_and_dcp_controls() {
        let (client_io, server_io) = duplex(32 * 1024);
        let connection = KvConnection::from_io(client_io, "unit-test", Duration::from_secs(1));
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let mut controls = BTreeMap::new();

            while let Some(request) = framed.next().await.transpose().unwrap() {
                assert!(matches!(request.magic, Magic::Request | Magic::AltRequest));
                let mut response = success_response(&request);
                match request.opcode {
                    Opcode::HELLO => {
                        let mut value = BytesMut::new();
                        value.put_u16(HelloFeature::Xattr.as_u16());
                        value.put_u16(HelloFeature::SelectBucket.as_u16());
                        value.put_u16(HelloFeature::Collections.as_u16());
                        value.put_u16(0xffff);
                        response.value = value.freeze();
                    }
                    Opcode::SASL_LIST_MECHS => response.value = Bytes::from_static(b"PLAIN"),
                    Opcode::SASL_AUTH => {
                        assert_eq!(request.key, Bytes::from_static(b"PLAIN"));
                        assert_eq!(request.value, Bytes::from_static(b"\0alice\0secret"));
                    }
                    Opcode::SELECT_BUCKET => {
                        assert_eq!(request.key, Bytes::from_static(b"travel"));
                    }
                    Opcode::DCP_OPEN => {
                        assert_eq!(request.key, Bytes::from_static(b"consumer-1"));
                        assert_eq!(
                            u32::from_be_bytes(request.extras[4..8].try_into().unwrap()),
                            5
                        );
                    }
                    Opcode::DCP_CONTROL => {
                        let key = String::from_utf8(request.key.to_vec()).unwrap();
                        let value = String::from_utf8(request.value.to_vec()).unwrap();
                        assert_ne!(key, "enable_out_of_order_snapshots");
                        controls.insert(key.clone(), value);
                        if key == "enable_stream_id" {
                            response.status = Status::NOT_SUPPORTED;
                        }
                        if key == "send_stream_end_on_client_close_stream" {
                            framed.send(response).await.unwrap();
                            break;
                        }
                    }
                    opcode => panic!("unexpected bootstrap opcode: {opcode:?}"),
                }
                framed.send(response).await.unwrap();
            }

            controls
        });

        let bootstrapped =
            bootstrap_on_connection(connection, &test_config(), "rust-dcp-test", "consumer-1")
                .await
                .unwrap();
        let controls = server.await.unwrap();

        assert_eq!(
            bootstrapped.capabilities().sasl_mechanism,
            SaslMechanism::Plain
        );
        assert!(
            bootstrapped
                .capabilities()
                .supports(HelloFeature::Collections)
        );
        assert!(
            bootstrapped
                .capabilities()
                .supports_control(DcpControlFeature::ChangeStreams)
        );
        assert!(
            bootstrapped
                .capabilities()
                .supports_control(DcpControlFeature::ExpiryOpcode)
        );
        assert!(
            !bootstrapped
                .capabilities()
                .supports_control(DcpControlFeature::StreamId)
        );
        assert!(
            bootstrapped
                .capabilities()
                .supports_control(DcpControlFeature::StreamEndOnClose)
        );
        assert_eq!(controls.get("set_priority").unwrap(), "medium");
        assert_eq!(controls.get("connection_buffer_size").unwrap(), "20971520");
        assert_eq!(controls.get("set_noop_interval").unwrap(), "20");
    }

    #[tokio::test]
    async fn required_control_failure_aborts_bootstrap() {
        let (client_io, server_io) = duplex(16 * 1024);
        let connection = KvConnection::from_io(client_io, "unit-test", Duration::from_secs(1));
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            while let Some(request) = framed.next().await.transpose().unwrap() {
                let mut response = success_response(&request);
                match request.opcode {
                    Opcode::SASL_LIST_MECHS => response.value = Bytes::from_static(b"PLAIN"),
                    Opcode::DCP_CONTROL if request.key == "enable_noop" => {
                        response.status = Status::TEMPORARY_FAILURE;
                        framed.send(response).await.unwrap();
                        return;
                    }
                    _ => {}
                }
                framed.send(response).await.unwrap();
            }
        });

        let result =
            bootstrap_on_connection(connection, &test_config(), "rust-dcp-test", "consumer-1")
                .await;
        server.await.unwrap();

        assert!(matches!(
            result,
            Err(DcpError::ServerStatus {
                status: 0x86,
                opcode: 0x5e,
                ..
            })
        ));
    }

    #[test]
    fn malformed_hello_feature_list_is_rejected() {
        let mut response = Frame::response(Opcode::HELLO, Status::SUCCESS);
        response.value = Bytes::from_static(&[0x12]);

        assert!(matches!(
            parse_hello_features(&response),
            Err(DcpError::Protocol(_))
        ));
    }
}
