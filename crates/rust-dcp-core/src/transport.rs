use std::{
    collections::VecDeque,
    io::{self, BufReader, Cursor},
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use rust_dcp_protocol::{Frame, FrameCodec};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    task,
    time::{self, Instant},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{ClientConfig, RootCertStore, pki_types::ServerName},
};
use tokio_util::codec::Framed;

use crate::{DcpError, Result, SeedAddress, TlsConfig};

/// Object-safe Tokio stream accepted by the KV protocol transport.
pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Boxed TCP, TLS, or test transport.
pub type BoxedIo = Box<dyn AsyncIo>;

/// Tokio-backed framed connection to one Couchbase KV endpoint.
pub struct KvConnection {
    framed: Framed<BoxedIo, FrameCodec>,
    peer: String,
    request_timeout: Duration,
    next_opaque: u32,
    pending: VecDeque<Frame>,
    last_inbound_activity: Instant,
}

impl std::fmt::Debug for KvConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvConnection")
            .field("peer", &self.peer)
            .field("request_timeout", &self.request_timeout)
            .field("pending_frames", &self.pending.len())
            .field("last_inbound_activity", &self.last_inbound_activity)
            .finish_non_exhaustive()
    }
}

impl KvConnection {
    /// Establishes an asynchronous TCP or TLS connection.
    ///
    /// # Errors
    ///
    /// Returns an I/O, TLS, DNS, certificate, or timeout error when the
    /// endpoint cannot be connected securely within `timeout`.
    pub async fn connect(seed: &SeedAddress, tls: &TlsConfig, timeout: Duration) -> Result<Self> {
        let address = seed.with_default_kv_port(tls.enabled);
        let deadline = Instant::now() + timeout;
        let tcp = time::timeout_at(deadline, TcpStream::connect(&address))
            .await
            .map_err(|_| DcpError::Timeout(timeout))??;
        tcp.set_nodelay(true)?;

        if tls.enabled {
            let client_config = time::timeout_at(deadline, build_tls_config(tls.clone()))
                .await
                .map_err(|_| DcpError::Timeout(timeout))??;
            let server_name = tls
                .server_name
                .clone()
                .unwrap_or_else(|| endpoint_host(seed.as_str()).to_owned());
            let server_name = ServerName::try_from(server_name.clone()).map_err(|error| {
                DcpError::Tls(format!("invalid TLS server name {server_name:?}: {error}"))
            })?;
            let connector = TlsConnector::from(Arc::new(client_config));
            let stream = time::timeout_at(deadline, connector.connect(server_name, tcp))
                .await
                .map_err(|_| DcpError::Timeout(timeout))?
                .map_err(|error| DcpError::Tls(error.to_string()))?;
            Ok(Self::from_boxed_io(Box::new(stream), address, timeout))
        } else {
            Ok(Self::from_boxed_io(Box::new(tcp), address, timeout))
        }
    }

    /// Creates a framed connection from any Tokio I/O object.
    #[must_use]
    pub fn from_io<T>(io: T, peer: impl Into<String>, request_timeout: Duration) -> Self
    where
        T: AsyncIo + 'static,
    {
        Self::from_boxed_io(Box::new(io), peer.into(), request_timeout)
    }

    fn from_boxed_io(io: BoxedIo, peer: String, request_timeout: Duration) -> Self {
        Self {
            framed: Framed::new(io, FrameCodec::default()),
            peer,
            request_timeout,
            next_opaque: 1,
            pending: VecDeque::new(),
            last_inbound_activity: Instant::now(),
        }
    }

    /// Endpoint used by this connection.
    #[must_use]
    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// Time of the last successfully decoded inbound frame.
    #[must_use]
    pub fn last_inbound_activity(&self) -> Instant {
        self.last_inbound_activity
    }

    /// Enables collection-ID key decoding after successful HELLO negotiation.
    pub fn set_collections_enabled(&mut self, enabled: bool) {
        let codec = self.framed.codec().clone().with_collections(enabled);
        *self.framed.codec_mut() = codec;
    }

    /// Sends one frame without waiting for a response.
    ///
    /// # Errors
    ///
    /// Returns a protocol or I/O error from the Tokio framed sink.
    pub async fn send_frame(&mut self, frame: Frame) -> Result<()> {
        self.framed.send(frame).await?;
        Ok(())
    }

    /// Receives one frame, including frames buffered during a request exchange.
    ///
    /// # Errors
    ///
    /// Returns a protocol or I/O error, or unexpected EOF if the peer closed.
    pub async fn receive_frame(&mut self) -> Result<Frame> {
        if let Some(frame) = self.pending.pop_front() {
            return Ok(frame);
        }
        self.read_raw().await
    }

    /// Sends a request and waits asynchronously for its correlated response.
    /// Unsolicited DCP/config frames are retained for later `receive_frame` calls.
    ///
    /// # Errors
    ///
    /// Returns a timeout, protocol, I/O, or unexpected EOF error.
    pub async fn request(&mut self, mut request: Frame) -> Result<Frame> {
        if request.opaque == 0 {
            request.opaque = self.allocate_opaque();
        }
        let opaque = request.opaque;
        let request_timeout = self.request_timeout;
        time::timeout(request_timeout, async {
            self.send_frame(request).await?;
            loop {
                let frame = self.read_raw().await?;
                if frame.magic.is_response() && frame.opaque == opaque {
                    return Ok(frame);
                }
                self.pending.push_back(frame);
            }
        })
        .await
        .map_err(|_| DcpError::Timeout(request_timeout))?
    }

    /// Releases the underlying `Framed` transport for the DCP runtime.
    #[must_use]
    pub fn into_framed(self) -> Framed<BoxedIo, FrameCodec> {
        self.framed
    }

    fn allocate_opaque(&mut self) -> u32 {
        let opaque = self.next_opaque;
        self.next_opaque = self.next_opaque.wrapping_add(1).max(1);
        opaque
    }

    async fn read_raw(&mut self) -> Result<Frame> {
        let frame = self.framed.next().await.transpose()?.ok_or_else(|| {
            DcpError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("KV peer {} closed the connection", self.peer),
            ))
        })?;
        self.last_inbound_activity = Instant::now();
        Ok(frame)
    }
}

async fn build_tls_config(tls: TlsConfig) -> Result<ClientConfig> {
    task::spawn_blocking(move || build_tls_config_sync(&tls))
        .await
        .map_err(|error| DcpError::Tls(format!("TLS root loading task failed: {error}")))?
}

fn build_tls_config_sync(tls: &TlsConfig) -> Result<ClientConfig> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(native.certs);
    if added == 0 && tls.root_ca_path.is_none() {
        return Err(DcpError::Tls(format!(
            "no platform root certificates loaded ({} parse failures, {} loader errors)",
            ignored,
            native.errors.len()
        )));
    }

    if let Some(path) = &tls.root_ca_path {
        let bytes = std::fs::read(path).map_err(|error| {
            DcpError::Tls(format!("cannot read root CA {}: {error}", path.display()))
        })?;
        let mut reader = BufReader::new(Cursor::new(bytes));
        let mut count = 0_usize;
        for certificate in rustls_pemfile::certs(&mut reader) {
            let certificate = certificate.map_err(|error| {
                DcpError::Tls(format!("invalid root CA PEM {}: {error}", path.display()))
            })?;
            roots.add(certificate).map_err(|error| {
                DcpError::Tls(format!("invalid root CA {}: {error}", path.display()))
            })?;
            count += 1;
        }
        if count == 0 {
            return Err(DcpError::Tls(format!(
                "root CA file {} contains no certificates",
                path.display()
            )));
        }
    }

    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn endpoint_host(seed: &str) -> &str {
    if let Some(stripped) = seed.strip_prefix('[')
        && let Some(end) = stripped.find(']')
    {
        return &stripped[..end];
    }
    if seed.matches(':').count() == 1 {
        return seed.rsplit_once(':').map_or(seed, |(host, _)| host);
    }
    seed
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rust_dcp_protocol::{Frame, FrameCodec, Opcode, ProtocolError, Status};
    use tokio::io::duplex;
    use tokio_util::codec::Framed;

    use super::*;

    #[tokio::test]
    async fn request_correlates_response_and_preserves_unsolicited_frame() {
        let (client_io, server_io) = duplex(4_096);
        let mut connection = KvConnection::from_io(client_io, "test-peer", Duration::from_secs(1));
        let server = tokio::spawn(async move {
            let mut framed = Framed::new(server_io, FrameCodec::default());
            let request = framed.next().await.unwrap().unwrap();
            let mut unsolicited = Frame::request(Opcode::DCP_NOOP);
            unsolicited.opaque = 99;
            framed.send(unsolicited).await.unwrap();
            let mut response = Frame::response(request.opcode, Status::SUCCESS);
            response.opaque = request.opaque;
            framed.send(response).await.unwrap();
        });

        let response = connection
            .request(Frame::request(Opcode::HELLO))
            .await
            .expect("response");
        assert_eq!(response.opcode, Opcode::HELLO);
        assert_eq!(connection.receive_frame().await.unwrap().opaque, 99);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn request_times_out_without_response() {
        let (client_io, _server_io) = duplex(64);
        let mut connection =
            KvConnection::from_io(client_io, "test-peer", Duration::from_millis(10));

        assert!(matches!(
            connection.request(Frame::request(Opcode::HELLO)).await,
            Err(DcpError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn outbound_writes_do_not_refresh_inbound_liveness() {
        let (client_io, _server_io) = duplex(4_096);
        let mut connection = KvConnection::from_io(client_io, "test-peer", Duration::from_secs(1));
        let before = connection.last_inbound_activity();

        tokio::time::sleep(Duration::from_millis(2)).await;
        connection
            .send_frame(Frame::request(Opcode::NOOP))
            .await
            .unwrap();

        assert_eq!(connection.last_inbound_activity(), before);
    }

    #[test]
    fn endpoint_host_handles_dns_ipv4_and_ipv6() {
        assert_eq!(endpoint_host("cb.example.test:11207"), "cb.example.test");
        assert_eq!(endpoint_host("127.0.0.1:11207"), "127.0.0.1");
        assert_eq!(endpoint_host("[2001:db8::1]:11207"), "2001:db8::1");
        assert_eq!(endpoint_host("2001:db8::1"), "2001:db8::1");
    }

    #[test]
    fn invalid_custom_root_ca_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "rust-dcp-invalid-ca-{}-{}.pem",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, b"not a certificate").unwrap();
        let result = build_tls_config_sync(&TlsConfig {
            enabled: true,
            root_ca_path: Some(path.clone()),
            server_name: None,
        });
        std::fs::remove_file(path).unwrap();

        assert!(result.is_err());
    }

    #[test]
    fn protocol_error_converts_to_public_error() {
        let error = DcpError::from(ProtocolError::InvalidMagic(0));
        assert!(matches!(error, DcpError::Protocol(_)));
    }
}
