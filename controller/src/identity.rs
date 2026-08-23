use anyhow::{Context, Result};
use localsend::crypto::cert::fingerprint_from_cert_der;
use localsend::http::dto_v2::RegisterDtoV2;
use localsend::http::server::TlsConfig;
use localsend::http::state::ClientInfo;
use localsend::model::discovery::{DeviceType, ProtocolType, PROTOCOL_VERSION_V2};
use localsend::multicast::MulticastDevice;
use serde::Deserialize;
use std::fs;
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MAX_IDENTITY_BYTES: u64 = 128 * 1024;
const MAX_PREFERENCES_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Clone)]
pub struct Identity {
    pub alias: String,
    pub port: u16,
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
}

impl Identity {
    pub fn load_or_generate(dir: &Path, alias: String, port: u16) -> Result<Self> {
        fs::create_dir_all(dir)
            .with_context(|| format!("Could not create state directory {}", dir.display()))?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("Could not secure state directory {}", dir.display()))?;

        let path = dir.join("identity.pem");
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                anyhow::ensure!(
                    metadata.file_type().is_file(),
                    "Identity path is not a file"
                );
                anyhow::ensure!(
                    metadata.len() <= MAX_IDENTITY_BYTES,
                    "Identity file is too large"
                );
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                    .context("Could not secure identity file")?;
                let text = fs::read_to_string(&path).context("Could not read identity file")?;
                Self::from_pem(&text, alias, port).context(
                    "Invalid identity file; remove it to generate a new LocalSend identity",
                )
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let identity = Self::generate(alias, port)?;
                identity.save(&path)?;
                Ok(identity)
            }
            Err(error) => Err(error).context("Could not inspect identity file"),
        }
    }

    fn from_pem(text: &str, alias: String, port: u16) -> Result<Self> {
        let blocks = pem::parse_many(text)?;
        let cert = blocks
            .iter()
            .find(|block| block.tag() == "CERTIFICATE")
            .context("missing certificate")?;
        let key = blocks
            .iter()
            .find(|block| block.tag().ends_with("PRIVATE KEY"))
            .context("missing private key")?;
        let key_pem = pem::encode(key);
        rcgen::KeyPair::from_pem(&key_pem).context("unusable private key")?;

        Ok(Self {
            alias,
            port,
            cert_pem: pem::encode(cert),
            key_pem,
            fingerprint: fingerprint_from_cert_der(cert.contents()),
        })
    }

    fn generate(alias: String, port: u16) -> Result<Self> {
        let generated = localsend::crypto::cert::generate_self_signed()?;
        Ok(Self {
            alias,
            port,
            cert_pem: generated.certificate_pem,
            key_pem: generated.private_key_pem,
            fingerprint: generated.fingerprint,
        })
    }

    fn save(&self, path: &Path) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .context("Could not create identity file")?;
        file.write_all(self.cert_pem.as_bytes())?;
        file.write_all(self.key_pem.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    pub fn tls_config(&self) -> TlsConfig {
        TlsConfig {
            cert: self.cert_pem.clone(),
            private_key: self.key_pem.clone(),
        }
    }

    pub fn client_info(&self) -> ClientInfo {
        ClientInfo {
            alias: self.alias.clone(),
            version: PROTOCOL_VERSION_V2.to_string(),
            device_model: Some("Omarchy".to_string()),
            device_type: Some(DeviceType::Headless),
            token: self.fingerprint.clone(),
        }
    }

    pub fn register_dto(&self) -> RegisterDtoV2 {
        RegisterDtoV2 {
            alias: self.alias.clone(),
            version: PROTOCOL_VERSION_V2.to_string(),
            device_model: Some("Omarchy".to_string()),
            device_type: Some(DeviceType::Headless),
            fingerprint: self.fingerprint.clone(),
            port: self.port,
            protocol: ProtocolType::Https,
            download: false,
        }
    }

    pub fn multicast_device(&self) -> MulticastDevice {
        MulticastDevice {
            alias: self.alias.clone(),
            version: PROTOCOL_VERSION_V2.to_string(),
            device_model: Some("Omarchy".to_string()),
            device_type: Some(DeviceType::Headless),
            fingerprint: self.fingerprint.clone(),
            port: self.port,
            protocol: ProtocolType::Https,
            download: false,
        }
    }
}

pub fn state_directory() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| home_directory().map(|home| home.join(".local/state")))
        .context("Neither XDG_STATE_HOME nor HOME provides an absolute state directory")?;
    Ok(base.join("omarchy/localsend-controller"))
}

pub fn default_socket_path() -> Result<PathBuf> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .context("XDG_RUNTIME_DIR is missing or not absolute")?;
    Ok(runtime.join("omarchy-localsend.sock"))
}

pub fn default_destination() -> Result<PathBuf> {
    let destination = dirs::download_dir()
        .or_else(|| home_directory().map(|home| home.join("Downloads")))
        .context("Could not determine the XDG Downloads directory")?;
    Ok(destination)
}

pub fn resolve_alias(override_alias: Option<String>) -> Result<String> {
    if let Some(alias) = override_alias {
        validate_alias(&alias)?;
        return Ok(alias);
    }

    if let Some(alias) = gui_alias() {
        return Ok(alias);
    }

    let hostname = gethostname::gethostname()
        .to_string_lossy()
        .trim_end_matches(".local")
        .to_string();
    Ok(if hostname.is_empty() {
        "Omarchy".to_string()
    } else {
        hostname
    })
}

fn validate_alias(alias: &str) -> Result<()> {
    anyhow::ensure!(!alias.trim().is_empty(), "Alias must not be empty");
    anyhow::ensure!(alias.chars().count() <= 128, "Alias is too long");
    anyhow::ensure!(
        !alias.chars().any(char::is_control),
        "Alias contains control characters"
    );
    Ok(())
}

#[derive(Deserialize)]
struct GuiPreferences {
    #[serde(rename = "flutter.ls_alias")]
    alias: Option<String>,
}

fn gui_alias() -> Option<String> {
    let path =
        home_directory()?.join(".local/share/org.localsend.localsend_app/shared_preferences.json");
    let metadata = fs::symlink_metadata(&path).ok()?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_PREFERENCES_BYTES {
        return None;
    }
    let text = fs::read_to_string(path).ok()?;
    let preferences: GuiPreferences = serde_json::from_str(&text).ok()?;
    let alias = preferences.alias?;
    validate_alias(&alias).ok()?;
    Some(alias)
}

fn home_directory() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(dirs::home_dir)
}
