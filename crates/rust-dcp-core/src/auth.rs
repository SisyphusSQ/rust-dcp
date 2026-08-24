use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rust_dcp_protocol::{
    Frame, Opcode, ProtocolError, Status, sasl_auth, sasl_list_mechanisms, sasl_step,
};
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use crate::{Credentials, DcpError, KvConnection, Result};

const MAX_SCRAM_ITERATIONS: u32 = 10_000_000;

/// SCRAM hash algorithm negotiated through SASL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScramAlgorithm {
    /// SCRAM-SHA-1.
    Sha1,
    /// SCRAM-SHA-256.
    Sha256,
    /// SCRAM-SHA-512.
    Sha512,
}

/// Supported SASL authentication mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaslMechanism {
    /// SASL PLAIN (normally protected by TLS).
    Plain,
    /// Salted challenge-response authentication.
    Scram(ScramAlgorithm),
}

impl SaslMechanism {
    /// Wire name sent to the Couchbase KV service.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::Scram(ScramAlgorithm::Sha1) => "SCRAM-SHA1",
            Self::Scram(ScramAlgorithm::Sha256) => "SCRAM-SHA256",
            Self::Scram(ScramAlgorithm::Sha512) => "SCRAM-SHA512",
        }
    }
}

pub(crate) async fn authenticate(
    connection: &mut KvConnection,
    credentials: &Credentials,
) -> Result<SaslMechanism> {
    let mechanisms = connection.request(sasl_list_mechanisms(0)).await?;
    ensure_auth_success(&mechanisms, Opcode::SASL_LIST_MECHS)?;
    let mechanism = select_mechanism(&String::from_utf8_lossy(&mechanisms.value))?;

    match mechanism {
        SaslMechanism::Plain => {
            let mut payload =
                Vec::with_capacity(credentials.username().len() + credentials.password().len() + 2);
            payload.push(0);
            payload.extend_from_slice(credentials.username().as_bytes());
            payload.push(0);
            payload.extend_from_slice(credentials.password().as_bytes());
            let response = connection
                .request(sasl_auth(mechanism.name(), Bytes::from(payload), 0))
                .await?;
            ensure_auth_success(&response, Opcode::SASL_AUTH)?;
        }
        SaslMechanism::Scram(algorithm) => {
            let mut client =
                ScramClient::new(algorithm, credentials.username(), credentials.password())?;
            let response = connection
                .request(sasl_auth(mechanism.name(), client.start(), 0))
                .await?;
            ensure_auth_opcode(&response, Opcode::SASL_AUTH)?;
            if response.status != Status::AUTH_CONTINUE {
                return Err(authentication_status_error(
                    response.status,
                    &response.value,
                ));
            }
            let final_message = client.handle_server_first(&response.value)?;
            let response = connection
                .request(sasl_step(mechanism.name(), final_message, 0))
                .await?;
            ensure_auth_success(&response, Opcode::SASL_STEP)?;
            client.verify_server_final(&response.value)?;
        }
    }
    Ok(mechanism)
}

fn select_mechanism(list: &str) -> Result<SaslMechanism> {
    for (name, mechanism) in [
        ("SCRAM-SHA512", SaslMechanism::Scram(ScramAlgorithm::Sha512)),
        ("SCRAM-SHA256", SaslMechanism::Scram(ScramAlgorithm::Sha256)),
        ("SCRAM-SHA1", SaslMechanism::Scram(ScramAlgorithm::Sha1)),
        ("PLAIN", SaslMechanism::Plain),
    ] {
        if list.split_ascii_whitespace().any(|item| item == name) {
            return Ok(mechanism);
        }
    }
    Err(DcpError::Authentication(format!(
        "server offered no supported SASL mechanism: {list}"
    )))
}

fn ensure_auth_success(response: &Frame, opcode: Opcode) -> Result<()> {
    ensure_auth_opcode(response, opcode)?;
    if response.status.is_success() {
        Ok(())
    } else {
        Err(authentication_status_error(
            response.status,
            &response.value,
        ))
    }
}

fn ensure_auth_opcode(response: &Frame, opcode: Opcode) -> Result<()> {
    if response.magic.is_response() && response.opcode == opcode {
        return Ok(());
    }
    Err(ProtocolError::MalformedFrame(format!(
        "expected SASL response opcode 0x{:02x}, got magic 0x{:02x} opcode 0x{:02x}",
        opcode.as_u8(),
        response.magic.as_u8(),
        response.opcode.as_u8()
    ))
    .into())
}

fn authentication_status_error(status: Status, payload: &[u8]) -> DcpError {
    DcpError::Authentication(format!(
        "server returned status 0x{:04x}: {}",
        status.as_u16(),
        String::from_utf8_lossy(payload)
    ))
}

struct ScramClient {
    algorithm: ScramAlgorithm,
    password: String,
    client_nonce: String,
    client_first_bare: String,
    expected_server_signature: Option<Vec<u8>>,
}

impl Drop for ScramClient {
    fn drop(&mut self) {
        self.password.zeroize();
        if let Some(signature) = &mut self.expected_server_signature {
            signature.zeroize();
        }
    }
}

impl ScramClient {
    fn new(algorithm: ScramAlgorithm, username: &str, password: &str) -> Result<Self> {
        let mut nonce = [0_u8; 18];
        getrandom::fill(&mut nonce).map_err(|error| {
            DcpError::Authentication(format!("cannot create SCRAM nonce: {error}"))
        })?;
        let nonce = general_purpose::STANDARD_NO_PAD.encode(nonce);
        Ok(Self::with_nonce(algorithm, username, password, nonce))
    }

    fn with_nonce(
        algorithm: ScramAlgorithm,
        username: &str,
        password: &str,
        nonce: impl Into<String>,
    ) -> Self {
        let client_nonce = nonce.into();
        let username = username.replace('=', "=3D").replace(',', "=2C");
        Self {
            algorithm,
            password: password.to_owned(),
            client_first_bare: format!("n={username},r={client_nonce}"),
            client_nonce,
            expected_server_signature: None,
        }
    }

    fn start(&self) -> Bytes {
        Bytes::from(format!("n,,{}", self.client_first_bare))
    }

    fn handle_server_first(&mut self, message: &[u8]) -> Result<Bytes> {
        if self.expected_server_signature.is_some() {
            return Err(DcpError::Authentication(
                "SCRAM server-first message was processed twice".into(),
            ));
        }
        let message = std::str::from_utf8(message).map_err(|error| {
            DcpError::Authentication(format!("SCRAM server-first is not UTF-8: {error}"))
        })?;
        let attributes = parse_scram_attributes(message)?;
        if attributes.contains_key(&'m') {
            return Err(DcpError::Authentication(
                "SCRAM mandatory extension is not supported".into(),
            ));
        }
        let nonce = attributes.get(&'r').ok_or_else(|| {
            DcpError::Authentication("SCRAM server-first is missing nonce".into())
        })?;
        if !nonce.starts_with(&self.client_nonce) || nonce.len() == self.client_nonce.len() {
            return Err(DcpError::Authentication(
                "SCRAM server nonce does not extend client nonce".into(),
            ));
        }
        let salt = attributes
            .get(&'s')
            .ok_or_else(|| DcpError::Authentication("SCRAM server-first is missing salt".into()))?;
        let salt = general_purpose::STANDARD.decode(salt).map_err(|error| {
            DcpError::Authentication(format!("SCRAM salt is invalid base64: {error}"))
        })?;
        let iterations = attributes
            .get(&'i')
            .ok_or_else(|| {
                DcpError::Authentication("SCRAM server-first is missing iteration count".into())
            })?
            .parse::<u32>()
            .map_err(|error| {
                DcpError::Authentication(format!("SCRAM iteration count is invalid: {error}"))
            })?;
        if iterations == 0 || iterations > MAX_SCRAM_ITERATIONS {
            return Err(DcpError::Authentication(format!(
                "SCRAM iteration count {iterations} is outside 1..={MAX_SCRAM_ITERATIONS}"
            )));
        }

        let client_final_without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!(
            "{},{message},{client_final_without_proof}",
            self.client_first_bare
        );
        let (proof, server_signature) = calculate_scram(
            self.algorithm,
            self.password.as_bytes(),
            &salt,
            iterations,
            auth_message.as_bytes(),
        )?;
        self.password.zeroize();
        self.expected_server_signature = Some(server_signature);
        Ok(Bytes::from(format!(
            "{client_final_without_proof},p={}",
            general_purpose::STANDARD.encode(proof)
        )))
    }

    fn verify_server_final(&mut self, message: &[u8]) -> Result<()> {
        let message = std::str::from_utf8(message).map_err(|error| {
            DcpError::Authentication(format!("SCRAM server-final is not UTF-8: {error}"))
        })?;
        let attributes = parse_scram_attributes(message)?;
        if let Some(server_error) = attributes.get(&'e') {
            return Err(DcpError::Authentication(format!(
                "SCRAM server rejected authentication: {server_error}"
            )));
        }
        let signature = attributes.get(&'v').ok_or_else(|| {
            DcpError::Authentication("SCRAM server-final is missing verifier".into())
        })?;
        let signature = general_purpose::STANDARD
            .decode(signature)
            .map_err(|error| {
                DcpError::Authentication(format!("SCRAM verifier is invalid base64: {error}"))
            })?;
        let expected = self.expected_server_signature.take().ok_or_else(|| {
            DcpError::Authentication("SCRAM server-final arrived before client-final".into())
        })?;
        if signature.ct_eq(&expected).unwrap_u8() != 1 {
            return Err(DcpError::Authentication(
                "SCRAM server signature mismatch".into(),
            ));
        }
        Ok(())
    }
}

fn parse_scram_attributes(message: &str) -> Result<HashMap<char, &str>> {
    let mut attributes = HashMap::new();
    for attribute in message.split(',') {
        let bytes = attribute.as_bytes();
        if bytes.len() < 3 || bytes[1] != b'=' || !bytes[0].is_ascii_alphabetic() {
            return Err(DcpError::Authentication(format!(
                "invalid SCRAM attribute {attribute:?}"
            )));
        }
        let key = char::from(bytes[0]);
        if attributes.insert(key, &attribute[2..]).is_some() {
            return Err(DcpError::Authentication(format!(
                "duplicate SCRAM attribute {key}"
            )));
        }
    }
    Ok(attributes)
}

fn calculate_scram(
    algorithm: ScramAlgorithm,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    macro_rules! calculate {
        ($digest:ty, $length:expr) => {{
            let mut salted_password = vec![0_u8; $length];
            pbkdf2_hmac::<$digest>(password, salt, iterations, &mut salted_password);

            let mut client_mac = Hmac::<$digest>::new_from_slice(&salted_password)
                .map_err(|error| DcpError::Authentication(error.to_string()))?;
            client_mac.update(b"Client Key");
            let mut client_key = client_mac.finalize().into_bytes().to_vec();
            let stored_key = <$digest>::digest(&client_key);

            let mut signature_mac = Hmac::<$digest>::new_from_slice(&stored_key)
                .map_err(|error| DcpError::Authentication(error.to_string()))?;
            signature_mac.update(auth_message);
            let client_signature = signature_mac.finalize().into_bytes();
            let proof = client_key
                .iter()
                .zip(client_signature.iter())
                .map(|(key, signature)| key ^ signature)
                .collect::<Vec<_>>();

            let mut server_key_mac = Hmac::<$digest>::new_from_slice(&salted_password)
                .map_err(|error| DcpError::Authentication(error.to_string()))?;
            server_key_mac.update(b"Server Key");
            let mut server_key = server_key_mac.finalize().into_bytes().to_vec();
            let mut server_signature_mac = Hmac::<$digest>::new_from_slice(&server_key)
                .map_err(|error| DcpError::Authentication(error.to_string()))?;
            server_signature_mac.update(auth_message);
            let signature = server_signature_mac.finalize().into_bytes().to_vec();

            salted_password.zeroize();
            client_key.zeroize();
            server_key.zeroize();
            (proof, signature)
        }};
    }

    Ok(match algorithm {
        ScramAlgorithm::Sha1 => calculate!(Sha1, 20),
        ScramAlgorithm::Sha256 => calculate!(Sha256, 32),
        ScramAlgorithm::Sha512 => calculate!(Sha512, 64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strongest_supported_mechanism_is_selected() {
        assert_eq!(
            select_mechanism("PLAIN SCRAM-SHA1 SCRAM-SHA256").unwrap(),
            SaslMechanism::Scram(ScramAlgorithm::Sha256)
        );
    }

    #[test]
    fn scram_sha1_matches_rfc_5802_vector() {
        let mut client = ScramClient::with_nonce(
            ScramAlgorithm::Sha1,
            "user",
            "pencil",
            "fyko+d2lbbFgONRv9qkxdawL",
        );
        assert_eq!(
            client.start(),
            Bytes::from_static(b"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL")
        );
        let final_message = client
            .handle_server_first(
                b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
            )
            .unwrap();
        assert_eq!(
            final_message,
            Bytes::from_static(
                b"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
            )
        );
        client
            .verify_server_final(b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=")
            .unwrap();
    }

    #[test]
    fn scram_rejects_nonce_that_does_not_extend_client_nonce() {
        let mut client =
            ScramClient::with_nonce(ScramAlgorithm::Sha256, "user", "secret", "client");

        assert!(
            client
                .handle_server_first(b"r=other,s=QSXCR+Q6sek8bf92,i=4096")
                .is_err()
        );
    }

    #[test]
    fn scram_escapes_username() {
        let client = ScramClient::with_nonce(ScramAlgorithm::Sha256, "a,b=c", "secret", "nonce");
        assert_eq!(
            client.start(),
            Bytes::from_static(b"n,,n=a=2Cb=3Dc,r=nonce")
        );
    }

    #[test]
    fn authentication_rejects_wrong_response_opcode() {
        let response = Frame::response(Opcode::HELLO, Status::SUCCESS);

        assert!(matches!(
            ensure_auth_success(&response, Opcode::SASL_AUTH),
            Err(DcpError::Protocol(_))
        ));
    }
}
