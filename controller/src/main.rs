mod daemon;
mod files;
mod identity;
mod rpc;

use clap::{error::ErrorKind, Parser, Subcommand};
use rpc::{ApiError, RpcRequest, RpcResponse};
use serde_json::json;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const MAX_RPC_BYTES: u64 = 8 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(35);

#[derive(Parser)]
#[command(
    name = "localsend-controller",
    version,
    about = "Headless LocalSend controller"
)]
struct Cli {
    #[arg(long, global = true, value_name = "PATH")]
    socket: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Daemon {
        #[arg(long, default_value_t = 53317)]
        port: u16,
        #[arg(long, value_name = "PATH")]
        destination: Option<PathBuf>,
        #[arg(long)]
        alias: Option<String>,
        #[arg(long, value_name = "PATH")]
        state_directory: Option<PathBuf>,
    },
    Ping,
    Snapshot,
    Refresh,
    SendFiles {
        #[arg(long)]
        device: String,
        #[arg(long = "path", required = true, value_name = "PATH")]
        paths: Vec<PathBuf>,
    },
    SendText {
        #[arg(long)]
        device: String,
        #[arg(long)]
        text: String,
    },
    SendClipboard {
        #[arg(long)]
        device: String,
    },
    Accept {
        #[arg(long)]
        request: String,
    },
    Decline {
        #[arg(long)]
        request: String,
    },
    Cancel {
        #[arg(long)]
        transfer: String,
    },
    Shutdown,
}

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|_| {
        eprintln!(
            "{}",
            json!({
                "ok": false,
                "error": {
                    "code": "panic",
                    "message": "The controller encountered an internal panic"
                }
            })
        );
    }));

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            emit(&RpcResponse::success(json!({"help": error.to_string()})));
            std::process::exit(0);
        }
        Err(error) => {
            emit(&RpcResponse::failure(ApiError::new(
                "invalid_arguments",
                error.to_string(),
            )));
            std::process::exit(2);
        }
    };

    let socket = match cli.socket {
        Some(path) => path,
        None => match identity::default_socket_path() {
            Ok(path) => path,
            Err(error) => {
                emit(&RpcResponse::failure(ApiError::new(
                    "configuration_error",
                    error.to_string(),
                )));
                std::process::exit(1);
            }
        },
    };

    let exit_code = match cli.command {
        Command::Daemon {
            port,
            destination,
            alias,
            state_directory,
        } => {
            let result = async {
                let alias = identity::resolve_alias(alias)?;
                let destination = match destination {
                    Some(destination) => destination,
                    None => identity::default_destination()?,
                };
                let state_directory = match state_directory {
                    Some(state_directory) => state_directory,
                    None => identity::state_directory()?,
                };
                daemon::run(daemon::DaemonConfig {
                    socket,
                    port,
                    destination,
                    alias,
                    state_directory,
                })
                .await
            }
            .await;
            match result {
                Ok(()) => 0,
                Err(error) => {
                    emit(&RpcResponse::failure(ApiError::new(
                        "daemon_start_failed",
                        error.to_string(),
                    )));
                    1
                }
            }
        }
        command => {
            let request = match command_to_request(command).await {
                Ok(request) => request,
                Err(error) => {
                    emit(&RpcResponse::failure(error));
                    std::process::exit(1);
                }
            };
            match call_daemon(&socket, &request).await {
                Ok(response) => {
                    let success = response.ok;
                    emit(&response);
                    if success {
                        0
                    } else {
                        1
                    }
                }
                Err(error) => {
                    emit(&RpcResponse::failure(error));
                    1
                }
            }
        }
    };
    std::process::exit(exit_code);
}

async fn command_to_request(command: Command) -> Result<RpcRequest, ApiError> {
    Ok(match command {
        Command::Ping => RpcRequest::Ping {},
        Command::Snapshot => RpcRequest::Snapshot {},
        Command::Refresh => RpcRequest::Refresh {},
        Command::SendFiles { device, paths } => RpcRequest::SendFiles {
            device,
            paths: absolute_paths(paths)?,
        },
        Command::SendText { device, text } => RpcRequest::SendText { device, text },
        Command::SendClipboard { device } => {
            let text = read_clipboard().await?;
            RpcRequest::SendText { device, text }
        }
        Command::Accept { request } => RpcRequest::Accept { request },
        Command::Decline { request } => RpcRequest::Decline { request },
        Command::Cancel { transfer } => RpcRequest::Cancel { transfer },
        Command::Shutdown => RpcRequest::Shutdown {},
        Command::Daemon { .. } => {
            return Err(ApiError::new(
                "invalid_arguments",
                "Daemon command cannot be sent as an RPC request",
            ));
        }
    })
}

fn absolute_paths(paths: Vec<PathBuf>) -> Result<Vec<String>, ApiError> {
    let current = std::env::current_dir()
        .map_err(|_| ApiError::new("invalid_path", "Could not determine the current directory"))?;
    paths
        .into_iter()
        .map(|path| {
            let path = if path.is_absolute() {
                path
            } else {
                current.join(path)
            };
            path.into_os_string()
                .into_string()
                .map_err(|_| ApiError::new("invalid_path", "Selected path is not valid UTF-8"))
        })
        .collect()
}

async fn call_daemon(socket: &PathBuf, request: &RpcRequest) -> Result<RpcResponse, ApiError> {
    let mut stream = tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(socket))
        .await
        .map_err(|_| ApiError::new("daemon_unavailable", "Controller connection timed out"))?
        .map_err(|_| {
            ApiError::new(
                "daemon_unavailable",
                format!("Could not connect to {}", socket.display()),
            )
        })?;
    let expected_uid = unsafe { libc::geteuid() };
    let daemon_uid = stream
        .peer_cred()
        .map_err(|_| ApiError::new("daemon_unavailable", "Could not verify controller identity"))?
        .uid();
    if daemon_uid != expected_uid {
        return Err(ApiError::new(
            "daemon_unavailable",
            "Controller socket belongs to another user",
        ));
    }
    let mut request_bytes = serde_json::to_vec(request)
        .map_err(|_| ApiError::new("serialization_error", "Could not serialize request"))?;
    if request_bytes.len() as u64 > MAX_RPC_BYTES {
        return Err(ApiError::new(
            "request_too_large",
            "Request exceeds the management API limit",
        ));
    }
    request_bytes.push(b'\n');
    tokio::time::timeout(RPC_TIMEOUT, async move {
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|_| ApiError::new("daemon_unavailable", "Could not write to daemon"))?;
        stream
            .shutdown()
            .await
            .map_err(|_| ApiError::new("daemon_unavailable", "Could not finish daemon request"))?;

        let mut reader = BufReader::new(stream.take(MAX_RPC_BYTES + 1));
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .map_err(|_| ApiError::new("protocol_error", "Could not read daemon response"))?;
        if read == 0 {
            return Err(ApiError::new(
                "protocol_error",
                "Daemon closed the socket without a response",
            ));
        }
        if read as u64 > MAX_RPC_BYTES || !line.ends_with('\n') {
            return Err(ApiError::new(
                "protocol_error",
                "Daemon response is invalid or too large",
            ));
        }
        serde_json::from_str(line.trim_end_matches(['\r', '\n']))
            .map_err(|_| ApiError::new("protocol_error", "Daemon returned invalid JSON"))
    })
    .await
    .map_err(|_| ApiError::new("daemon_timeout", "Controller request timed out"))?
}

async fn read_clipboard() -> Result<String, ApiError> {
    let mut child = tokio::process::Command::new("wl-paste")
        .arg("--no-newline")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ApiError::new("clipboard_unavailable", "Could not execute wl-paste"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ApiError::new("clipboard_unavailable", "Could not read the clipboard"))?;
    let mut output = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(5), async {
        stdout
            .take(files::MAX_TEXT_BYTES as u64 + 1)
            .read_to_end(&mut output)
            .await
    })
    .await;
    if read.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(ApiError::new(
            "clipboard_unavailable",
            "Reading the clipboard timed out",
        ));
    }
    if read.is_ok_and(|result| result.is_err()) {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(ApiError::new(
            "clipboard_unavailable",
            "Could not read the clipboard",
        ));
    }
    if output.len() > files::MAX_TEXT_BYTES {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(ApiError::new(
            "text_too_large",
            format!(
                "Clipboard text is limited to {} UTF-8 bytes",
                files::MAX_TEXT_BYTES
            ),
        ));
    }
    let status = match tokio::time::timeout(Duration::from_secs(1), child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            return Err(ApiError::new("clipboard_unavailable", "wl-paste failed"));
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(ApiError::new(
                "clipboard_unavailable",
                "wl-paste did not exit",
            ));
        }
    };
    if !status.success() {
        return Err(ApiError::new(
            "clipboard_unavailable",
            "wl-paste could not read the clipboard",
        ));
    }
    String::from_utf8(output).map_err(|_| {
        ApiError::new(
            "clipboard_not_text",
            "Clipboard contents are not valid UTF-8 text",
        )
    })
}

fn emit(response: &RpcResponse) {
    let mut stdout = std::io::stdout().lock();
    if serde_json::to_writer(&mut stdout, response).is_err() {
        let _ = stdout.write_all(
            br#"{"ok":false,"error":{"code":"serialization_error","message":"Could not serialize output"}}"#,
        );
    }
    let _ = stdout.write_all(b"\n");
    let _ = stdout.flush();
}
