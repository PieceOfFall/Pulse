use std::{fs, path::Path};

use rs_netty::{Error, Result, ServerTlsContext, TlsContextBuilder};

use crate::settings::{ServerTlsConfig, TlsClientAuth};

pub(crate) fn build_server_tls_context(
    config: &ServerTlsConfig,
) -> Result<Option<ServerTlsContext>> {
    if !config.enabled {
        return Ok(None);
    }

    let certificate_chain = read_tls_file(
        required_tls_path(
            config.certificate_chain.as_deref(),
            "server.tls.certificate_chain",
        )?,
        "server.tls.certificate_chain",
    )?;
    let private_key = read_tls_file(
        required_tls_path(config.private_key.as_deref(), "server.tls.private_key")?,
        "server.tls.private_key",
    )?;
    let mut builder = TlsContextBuilder::for_server()
        .certificate_chain_pem(&certificate_chain)
        .private_key_pem(&private_key);

    match config.client_auth {
        TlsClientAuth::Disabled => {}
        TlsClientAuth::Optional => {
            let client_ca = read_tls_file(
                required_tls_path(config.client_ca.as_deref(), "server.tls.client_ca")?,
                "server.tls.client_ca",
            )?;
            builder = builder.client_auth_optional_pem(&client_ca);
        }
        TlsClientAuth::Required => {
            let client_ca = read_tls_file(
                required_tls_path(config.client_ca.as_deref(), "server.tls.client_ca")?,
                "server.tls.client_ca",
            )?;
            builder = builder.client_auth_required_pem(&client_ca);
        }
    }

    builder.build().map(Some)
}

fn required_tls_path<'a>(path: Option<&'a Path>, name: &str) -> Result<&'a Path> {
    path.ok_or_else(|| {
        Error::Tls(format!(
            "{name} is required when server.tls.enabled is true"
        ))
    })
}

fn read_tls_file(path: &Path, name: &str) -> Result<Vec<u8>> {
    fs::read(path).map_err(|error| Error::Tls(format!("read {name} `{}`: {error}", path.display())))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::build_server_tls_context;
    use crate::settings::{ServerTlsConfig, TlsClientAuth};

    #[test]
    fn builds_server_tls_context_from_pem_files() {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                .expect("generate certificate");
        let dir = std::env::temp_dir().join(format!("pulse-tls-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        let cert_path = dir.join("server-chain.pem");
        let key_path = dir.join("server-key.pem");
        fs::write(&cert_path, cert.pem()).expect("write certificate");
        fs::write(&key_path, signing_key.serialize_pem()).expect("write private key");

        let config = ServerTlsConfig {
            enabled: true,
            certificate_chain: Some(cert_path.clone()),
            private_key: Some(key_path.clone()),
            client_auth: TlsClientAuth::Disabled,
            client_ca: None,
        };

        let tls = build_server_tls_context(&config).expect("build TLS context");

        assert!(tls.is_some());
        cleanup(&[cert_path, key_path]);
        let _ = fs::remove_dir(dir);
    }

    fn cleanup(paths: &[PathBuf]) {
        for path in paths {
            let _ = fs::remove_file(path);
        }
    }
}
