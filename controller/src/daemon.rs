use crate::files::{
    collect_selection, remove_reservations, reserve_unique_destination, OutgoingItem,
    OutgoingSource,
};
use crate::identity::Identity;
use crate::rpc::{
    AddressSnapshot, ApiError, ApiResult, ControllerError, DaemonSnapshot, DeviceSnapshot,
    IncomingRequestSnapshot, OfferedFileSnapshot, PeerSnapshot, RpcRequest, RpcResponse, Snapshot,
    TransferDirection, TransferFileSnapshot, TransferFileState, TransferSnapshot, TransferState,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use localsend::discovery::{
    ChannelStatus, DeviceChannel, DeviceIdentity, DiscoveredDevice, DiscoveryConfig,
    DiscoveryEvent, DiscoveryHandle, HttpChannel, StatefulDevice, DEFAULT_DISCOVERY_TIMEOUT,
};
use localsend::http::client::{ClientError, LsHttpClientV2};
use localsend::http::dto_v2::{PrepareUploadRequestDtoV2, RegisterDtoV2};
use localsend::http::server::common::save::FileUploadTarget;
use localsend::http::server::v2::{PrepareUploadDecisionV2, ServerEventV2, SessionEndReasonV2};
use localsend::http::server::{start_with_port, ServerConfigV2, ServerHandle};
use localsend::model::discovery::{DeviceType, ProtocolType, PROTOCOL_VERSION_V2};
use localsend::model::transfer::FileDto;
use localsend::multicast::{DEFAULT_MULTICAST_GROUP, DEFAULT_MULTICAST_GROUP_V6};
use localsend::reqwest;
use localsend::util::interface::{local_interface_addresses, InterfaceFilter};
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::ErrorKind;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

const MAX_RPC_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RECENT_TRANSFERS: usize = 8;
const MAX_RECENT_FILES: usize = 32;
const MAX_INCOMING_FILES: usize = 512;
const MAX_INCOMING_NAME_BYTES: usize = 1024;
const MAX_INCOMING_METADATA_BYTES: usize = 512 * 1024;
const MAX_REMOTE_FIELD_BYTES: usize = 256;
const MAX_MESSAGE_PREVIEW_CHARS: usize = 4096;
const MAX_DEVICES: usize = 128;
const MAX_MANAGEMENT_CONNECTIONS: usize = 32;
const MANAGEMENT_READ_TIMEOUT: Duration = Duration::from_secs(10);
const ONLINE_TTL: Duration = Duration::from_secs(90);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);

pub struct DaemonConfig {
    pub socket: PathBuf,
    pub port: u16,
    pub destination: PathBuf,
    pub alias: String,
    pub state_directory: PathBuf,
}

struct RpcCall {
    request: RpcRequest,
    reply: oneshot::Sender<RpcResponse>,
}

enum InternalEvent {
    SelectionReady {
        device: String,
        result: ApiResult<Vec<OutgoingItem>>,
        reply: oneshot::Sender<RpcResponse>,
    },
    RefreshFinished {
        id: String,
        result: Result<(), String>,
    },
    OutgoingPrepared {
        transfer_id: String,
        session_id: String,
        accepted: HashSet<String>,
    },
    OutgoingFileStarted {
        transfer_id: String,
        file_id: String,
    },
    OutgoingFileCompleted {
        transfer_id: String,
        file_id: String,
    },
    OutgoingFileFailed {
        transfer_id: String,
        file_id: String,
        error: ApiError,
    },
    OutgoingFinished {
        transfer_id: String,
        state: TransferState,
        error: Option<ApiError>,
    },
    IncomingFileResult {
        transfer_id: String,
        file_id: String,
        result: Result<(), String>,
    },
}

struct PendingIncoming {
    snapshot: IncomingRequestSnapshot,
    sender: SenderTarget,
    files: HashMap<String, FileDto>,
    decision_tx: oneshot::Sender<PrepareUploadDecisionV2>,
}

#[derive(Clone)]
struct SenderTarget {
    host: String,
    port: u16,
    protocol: ProtocolType,
    fingerprint: String,
}

struct IncomingActive {
    record: TransferSnapshot,
    sender: SenderTarget,
    progress: HashMap<String, Arc<AtomicU64>>,
    in_flight: HashSet<String>,
    end: Option<IncomingEnd>,
}

#[derive(Clone, Copy)]
enum IncomingEnd {
    Finished,
    Cancelled,
}

struct OutgoingActive {
    record: TransferSnapshot,
    progress: HashMap<String, Arc<AtomicU64>>,
    cancel: CancellationToken,
    by_peer: Arc<AtomicBool>,
    host: String,
    remote_session_id: Option<String>,
    task: tokio::task::JoinHandle<()>,
}

struct Controller {
    identity: Arc<Identity>,
    server: Arc<ServerHandle>,
    discovery: Arc<DiscoveryHandle>,
    socket: PathBuf,
    destination: PathBuf,
    started_at: String,
    status: String,
    error: Option<ControllerError>,
    pending: Option<PendingIncoming>,
    incoming: Option<IncomingActive>,
    outgoing: Option<OutgoingActive>,
    selecting_outgoing: bool,
    recent: VecDeque<TransferSnapshot>,
    refresh_id: Option<String>,
    early_remote_cancel: Option<(String, String)>,
    internal_tx: mpsc::Sender<InternalEvent>,
}

pub async fn run(config: DaemonConfig) -> Result<()> {
    anyhow::ensure!(config.port != 0, "Port 0 is not supported");
    anyhow::ensure!(config.socket.is_absolute(), "Socket path must be absolute");
    anyhow::ensure!(
        config.destination.is_absolute(),
        "Destination path must be absolute"
    );

    // Keep the socket private from the instant it is created; chmod below
    // narrows it further to exactly 0600.
    unsafe {
        libc::umask(0o077);
    }

    let (listener, socket_guard) = bind_management_socket(&config.socket)?;
    let destination = prepare_destination(&config.destination)?;
    anyhow::ensure!(
        destination.to_str().is_some(),
        "Destination path must be valid UTF-8"
    );
    let identity = Arc::new(Identity::load_or_generate(
        &config.state_directory,
        config.alias,
        config.port,
    )?);

    let (server_tx, mut server_rx) = mpsc::channel::<ServerEventV2>(64);
    let (server_stop_tx, server_stop_rx) = oneshot::channel();
    let server = Arc::new(
        start_with_port(
            config.port,
            Some(identity.tls_config()),
            identity.client_info(),
            None,
            Some(ServerConfigV2 {
                pin: None,
                verify_checksums: true,
                event_tx: server_tx,
            }),
            None,
            server_stop_rx,
        )
        .await
        .context("Could not start the LocalSend receiver")?,
    );

    let (discovery_tx, mut discovery_rx) = mpsc::channel::<DiscoveryEvent>(64);
    let (discovery_stop_tx, discovery_stop_rx) = oneshot::channel();
    let discovery = Arc::new(
        localsend::discovery::start(
            DiscoveryConfig {
                group: DEFAULT_MULTICAST_GROUP,
                group_v6: Some(DEFAULT_MULTICAST_GROUP_V6),
                port: config.port,
                interface_filter: InterfaceFilter::default(),
                device: identity.multicast_device(),
                identity: DeviceIdentity {
                    cert_pem: identity.cert_pem.clone(),
                    private_key_pem: identity.key_pem.clone(),
                },
                timeout: DEFAULT_DISCOVERY_TIMEOUT,
                event_tx: Some(discovery_tx),
            },
            discovery_stop_rx,
        )
        .await,
    );

    let (rpc_tx, mut rpc_rx) = mpsc::channel::<RpcCall>(64);
    let socket_cancel = CancellationToken::new();
    let socket_task = tokio::spawn(serve_management(listener, rpc_tx, socket_cancel.clone()));
    let (internal_tx, mut internal_rx) = mpsc::channel::<InternalEvent>(64);

    let mut controller = Controller {
        identity,
        server,
        discovery,
        socket: config.socket,
        destination,
        started_at: now(),
        status: "running".to_string(),
        error: None,
        pending: None,
        incoming: None,
        outgoing: None,
        selecting_outgoing: false,
        recent: VecDeque::new(),
        refresh_id: None,
        early_remote_cancel: None,
        internal_tx,
    };
    if let Some(error) = controller.discovery.multicast_error() {
        controller.set_error(
            "multicast_unavailable",
            format!("Discovery is running without multicast: {error}"),
        );
    }
    controller.start_refresh();

    emit_daemon_event(
        "ready",
        json!({
            "socket": controller.socket,
            "port": controller.identity.port,
            "alias": controller.identity.alias,
            "pid": std::process::id()
        }),
    );

    let mut progress_tick = tokio::time::interval(Duration::from_millis(250));
    progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut announce_tick = tokio::time::interval(ANNOUNCE_INTERVAL);
    announce_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // The staged refresh already announces immediately.
    announce_tick.tick().await;
    let shutdown_signal = shutdown_signal();
    tokio::pin!(shutdown_signal);

    let mut stop = false;
    while !stop {
        tokio::select! {
            Some(call) = rpc_rx.recv() => {
                stop = controller.handle_rpc(call).await;
            }
            Some(event) = server_rx.recv() => {
                controller.handle_server_event(event);
                if controller.status == "error" {
                    stop = true;
                }
            }
            Some(event) = discovery_rx.recv() => {
                controller.handle_discovery_event(event);
            }
            Some(event) = internal_rx.recv() => {
                controller.handle_internal_event(event);
            }
            _ = progress_tick.tick() => {
                controller.sync_progress();
            }
            _ = announce_tick.tick() => {
                if controller.refresh_id.is_none() {
                    let discovery = controller.discovery.clone();
                    tokio::spawn(async move { discovery.announce().await; });
                }
            }
            _ = &mut shutdown_signal => {
                stop = true;
            }
        }
    }

    controller.status = "shutting_down".to_string();
    if let Some(pending) = controller.pending.take() {
        let _ = pending.decision_tx.send(PrepareUploadDecisionV2::Decline);
    }
    let outgoing_task = controller.outgoing.take().map(|outgoing| {
        outgoing.cancel.cancel();
        outgoing.task
    });
    if let Some(incoming) = controller.incoming.take() {
        controller
            .server
            .cancel_v2_session(&incoming.record.id)
            .await;
        spawn_cancel_notification(
            controller.identity.clone(),
            incoming.sender,
            incoming.record.id,
        );
    }

    let _ = server_stop_tx.send(());
    let _ = discovery_stop_tx.send(());
    socket_cancel.cancel();

    let _ = tokio::time::timeout(Duration::from_secs(2), controller.server.wait_stopped()).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), controller.discovery.wait_stopped()).await;
    if let Some(task) = outgoing_task {
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), socket_task).await;
    drop(socket_guard);

    emit_daemon_event("stopped", json!({"status": "stopped"}));
    Ok(())
}

impl Controller {
    async fn handle_rpc(&mut self, call: RpcCall) -> bool {
        match call.request {
            RpcRequest::Ping {} => {
                let _ = call.reply.send(RpcResponse::success(json!({
                    "status": self.status,
                    "alias": self.identity.alias,
                    "pid": std::process::id(),
                    "protocolVersion": PROTOCOL_VERSION_V2,
                    "controllerVersion": env!("CARGO_PKG_VERSION")
                })));
            }
            RpcRequest::Snapshot {} => {
                self.sync_progress();
                let snapshot = self.snapshot();
                let _ = call.reply.send(RpcResponse::success(snapshot));
            }
            RpcRequest::Refresh {} => {
                let started = self.refresh_id.is_none();
                let id = self.start_refresh();
                let _ = call.reply.send(RpcResponse::success(json!({
                    "refreshId": id,
                    "started": started
                })));
            }
            RpcRequest::SendFiles { device, paths } => {
                if self.outgoing.is_some() || self.selecting_outgoing {
                    let _ = call.reply.send(RpcResponse::failure(ApiError::new(
                        "outgoing_busy",
                        "An outgoing transfer is already active",
                    )));
                } else {
                    self.selecting_outgoing = true;
                    let internal = self.internal_tx.clone();
                    tokio::spawn(async move {
                        let result =
                            match tokio::task::spawn_blocking(move || collect_selection(&paths))
                                .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(ApiError::new(
                                    "selection_failed",
                                    "The file selection task failed",
                                )),
                            };
                        let _ = internal
                            .send(InternalEvent::SelectionReady {
                                device,
                                result,
                                reply: call.reply,
                            })
                            .await;
                    });
                }
            }
            RpcRequest::SendText { device, text } => {
                let response = if self.outgoing.is_some() || self.selecting_outgoing {
                    RpcResponse::failure(ApiError::new(
                        "outgoing_busy",
                        "An outgoing transfer is already active",
                    ))
                } else {
                    RpcResponse::from_result(
                        crate::files::text_item(text)
                            .and_then(|item| self.start_outgoing(&device, vec![item]))
                            .map(|transfer| json!({"transfer": transfer})),
                    )
                };
                let _ = call.reply.send(response);
            }
            RpcRequest::Accept { request } => {
                let response = RpcResponse::from_result(
                    self.accept_pending(&request)
                        .await
                        .map(|transfer| json!({"transfer": transfer})),
                );
                let _ = call.reply.send(response);
            }
            RpcRequest::Decline { request } => {
                let response = RpcResponse::from_result(
                    self.decline_pending(&request)
                        .map(|transfer| json!({"transfer": transfer})),
                );
                let _ = call.reply.send(response);
            }
            RpcRequest::Cancel { transfer } => {
                let response = RpcResponse::from_result(
                    self.cancel_transfer(&transfer)
                        .await
                        .map(|transfer| json!({"transfer": transfer})),
                );
                let _ = call.reply.send(response);
            }
            RpcRequest::Shutdown {} => {
                let _ = call.reply.send(RpcResponse::success(json!({
                    "status": "shutting_down"
                })));
                return true;
            }
        }
        false
    }

    fn handle_internal_event(&mut self, event: InternalEvent) {
        match event {
            InternalEvent::SelectionReady {
                device,
                result,
                reply,
            } => {
                self.selecting_outgoing = false;
                if reply.is_closed() {
                    return;
                }
                let response = RpcResponse::from_result(
                    result
                        .and_then(|items| self.start_outgoing(&device, items))
                        .map(|transfer| json!({"transfer": transfer})),
                );
                let _ = reply.send(response);
            }
            InternalEvent::RefreshFinished { id, result } => {
                if self.refresh_id.as_deref() == Some(&id) {
                    self.refresh_id = None;
                    match result {
                        Err(message) => self.set_error("refresh_failed", message),
                        Ok(())
                            if self
                                .error
                                .as_ref()
                                .is_some_and(|error| error.code == "refresh_failed") =>
                        {
                            self.error = None;
                        }
                        Ok(()) => {}
                    }
                }
            }
            InternalEvent::OutgoingPrepared {
                transfer_id,
                session_id,
                accepted,
            } => {
                let Some(active) = self
                    .outgoing
                    .as_mut()
                    .filter(|active| active.record.id == transfer_id)
                else {
                    return;
                };
                active.remote_session_id = Some(session_id);
                active.record.state = TransferState::Transferring;
                active.record.started_at = Some(now());
                active.record.updated_at = now();
                for file in &mut active.record.files {
                    if !accepted.contains(&file.id) {
                        file.state = TransferFileState::Declined;
                    }
                }
                if self
                    .early_remote_cancel
                    .take()
                    .is_some_and(|(host, remote_id)| {
                        host == active.host
                            && active.remote_session_id.as_deref() == Some(remote_id.as_str())
                    })
                {
                    active.by_peer.store(true, Ordering::Relaxed);
                    active.cancel.cancel();
                    active.record.state = TransferState::Cancelling;
                }
            }
            InternalEvent::OutgoingFileStarted {
                transfer_id,
                file_id,
            } => {
                if let Some(file) = self.outgoing_file_mut(&transfer_id, &file_id) {
                    file.state = TransferFileState::Transferring;
                    file.error = None;
                }
            }
            InternalEvent::OutgoingFileCompleted {
                transfer_id,
                file_id,
            } => {
                if let Some(file) = self.outgoing_file_mut(&transfer_id, &file_id) {
                    file.state = TransferFileState::Completed;
                    file.completed_bytes = file.size;
                }
            }
            InternalEvent::OutgoingFileFailed {
                transfer_id,
                file_id,
                error,
            } => {
                if let Some(file) = self.outgoing_file_mut(&transfer_id, &file_id) {
                    file.state = TransferFileState::Failed;
                    file.error = Some(error);
                }
            }
            InternalEvent::OutgoingFinished {
                transfer_id,
                state,
                error,
            } => self.finish_outgoing(&transfer_id, state, error),
            InternalEvent::IncomingFileResult {
                transfer_id,
                file_id,
                result,
            } => self.handle_incoming_file_result(&transfer_id, &file_id, result),
        }
    }

    fn outgoing_file_mut(
        &mut self,
        transfer_id: &str,
        file_id: &str,
    ) -> Option<&mut TransferFileSnapshot> {
        let active = self
            .outgoing
            .as_mut()
            .filter(|active| active.record.id == transfer_id)?;
        active.record.updated_at = now();
        active
            .record
            .files
            .iter_mut()
            .find(|file| file.id == file_id)
    }

    fn finish_outgoing(
        &mut self,
        transfer_id: &str,
        state: TransferState,
        error: Option<ApiError>,
    ) {
        let Some(mut active) = self.outgoing.take() else {
            return;
        };
        if active.record.id != transfer_id {
            self.outgoing = Some(active);
            return;
        }
        sync_record_progress(&mut active.record, &active.progress);
        active.record.state = state;
        active.record.error = error;
        if state == TransferState::Completed && active.record.started_at.is_none() {
            active.record.started_at = Some(now());
        }
        active.record.updated_at = now();
        active.record.finished_at = Some(now());
        let transfer_error = active.record.error.clone();
        for file in &mut active.record.files {
            match state {
                TransferState::Completed if file.state != TransferFileState::Declined => {
                    file.state = TransferFileState::Completed;
                    file.completed_bytes = file.size;
                }
                TransferState::Cancelled
                    if matches!(
                        file.state,
                        TransferFileState::Queued | TransferFileState::Transferring
                    ) =>
                {
                    file.state = TransferFileState::Cancelled;
                }
                TransferState::Declined => file.state = TransferFileState::Declined,
                TransferState::Failed
                    if matches!(
                        file.state,
                        TransferFileState::Queued | TransferFileState::Transferring
                    ) =>
                {
                    file.state = TransferFileState::Failed;
                    if file.error.is_none() {
                        file.error = transfer_error.clone();
                    }
                }
                _ => {}
            }
        }
        active.record.completed_bytes = active
            .record
            .files
            .iter()
            .map(|file| file.completed_bytes)
            .sum();
        self.early_remote_cancel = None;
        self.push_recent(active.record);
    }

    fn handle_server_event(&mut self, event: ServerEventV2) {
        match event {
            ServerEventV2::Register { ip, info } => {
                if let Err(error) = validate_sender_info(&info) {
                    self.set_error(error.code, error.message);
                    return;
                }
                let fingerprint = info.fingerprint.clone();
                self.add_confirmed_device(ip.to_string(), info, fingerprint);
            }
            ServerEventV2::PrepareUpload {
                session_id,
                ip,
                info,
                cert_fingerprint,
                files,
                decision_tx,
            } => {
                let Some(fingerprint) = cert_fingerprint else {
                    let _ = decision_tx.send(PrepareUploadDecisionV2::Decline);
                    self.set_error(
                        "unverified_sender",
                        "Rejected an incoming request without a TLS certificate",
                    );
                    return;
                };
                if self.pending.is_some() || self.incoming.is_some() {
                    let _ = decision_tx.send(PrepareUploadDecisionV2::Decline);
                    return;
                }
                if let Err(error) = validate_sender_info(&info) {
                    let _ = decision_tx.send(PrepareUploadDecisionV2::Decline);
                    self.set_error(error.code, error.message);
                    return;
                }
                if let Err(error) = validate_incoming_offer(&files) {
                    let _ = decision_tx.send(PrepareUploadDecisionV2::Decline);
                    self.set_error(error.code, error.message);
                    return;
                }

                let host = ip.to_string();
                self.add_confirmed_device(host.clone(), info.clone(), fingerprint.clone());
                let address = AddressSnapshot {
                    host: host.clone(),
                    port: info.port,
                    protocol: protocol_name(info.protocol).to_string(),
                };
                let sender = PeerSnapshot {
                    fingerprint: fingerprint.clone(),
                    alias: info.alias.clone(),
                    device_type: info.device_type.as_ref().map(device_type_name),
                    model: info.device_model.clone(),
                    address,
                };
                let mut offered: Vec<OfferedFileSnapshot> = files
                    .values()
                    .map(|file| OfferedFileSnapshot {
                        id: file.id.clone(),
                        name: file.file_name.clone(),
                        size: file.size,
                        mime_type: file.file_type.clone(),
                    })
                    .collect();
                offered.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
                let total_bytes = offered.iter().map(|file| file.size).sum();
                let (message_preview, message_preview_truncated) = message_preview(&files);
                self.pending = Some(PendingIncoming {
                    snapshot: IncomingRequestSnapshot {
                        id: session_id,
                        sender,
                        files: offered,
                        total_bytes,
                        message_preview,
                        message_preview_truncated,
                        received_at: now(),
                    },
                    sender: SenderTarget {
                        host,
                        port: info.port,
                        protocol: info.protocol,
                        fingerprint,
                    },
                    files,
                    decision_tx,
                });
            }
            ServerEventV2::FileUpload {
                session_id,
                file_id,
                file: _,
                target_tx,
            } => self.handle_file_upload(session_id, file_id, target_tx),
            ServerEventV2::SessionEnd { session_id, reason } => {
                let Some(active) = self
                    .incoming
                    .as_mut()
                    .filter(|active| active.record.id == session_id)
                else {
                    return;
                };
                active.end = Some(match reason {
                    SessionEndReasonV2::Finished => IncomingEnd::Finished,
                    SessionEndReasonV2::Cancelled => IncomingEnd::Cancelled,
                });
                self.finish_incoming_if_ready();
            }
            ServerEventV2::PrepareUploadAborted { session_id } => {
                if self
                    .pending
                    .as_ref()
                    .map(|pending| pending.snapshot.id.as_str())
                    == Some(session_id.as_str())
                {
                    let pending = self.pending.take().expect("pending was checked");
                    let mut record = transfer_from_pending(&pending, TransferState::Cancelled);
                    let finished = now();
                    record.updated_at = finished.clone();
                    record.finished_at = Some(finished);
                    self.push_recent(record);
                }
            }
            ServerEventV2::CancelReceived { ip, session_id } => {
                let Some(active) = self.outgoing.as_mut() else {
                    return;
                };
                let host = ip.to_string();
                if active.host != host {
                    return;
                }
                if active.remote_session_id.as_deref() == Some(session_id.as_str()) {
                    active.by_peer.store(true, Ordering::Relaxed);
                    active.cancel.cancel();
                    active.record.state = TransferState::Cancelling;
                    active.record.updated_at = now();
                } else if active.remote_session_id.is_none() {
                    self.early_remote_cancel = Some((host, session_id));
                }
            }
            ServerEventV2::ListenerFailed { error } => {
                self.discovery.set_answer_announcements(false);
                self.status = "error".to_string();
                self.set_error("receiver_stopped", error);
            }
        }
    }

    fn handle_file_upload(
        &mut self,
        session_id: String,
        file_id: String,
        target_tx: oneshot::Sender<FileUploadTarget>,
    ) {
        let Some(active) = self
            .incoming
            .as_mut()
            .filter(|active| active.record.id == session_id)
        else {
            return;
        };
        if active.in_flight.contains(&file_id) {
            return;
        }
        let Some(file) = active
            .record
            .files
            .iter_mut()
            .find(|file| file.id == file_id)
        else {
            return;
        };
        let Some(path) = file.destination_path.as_ref().map(PathBuf::from) else {
            return;
        };

        let progress = active
            .progress
            .entry(file_id.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        progress.store(0, Ordering::Relaxed);
        file.state = TransferFileState::Transferring;
        file.completed_bytes = 0;
        file.error = None;
        active.in_flight.insert(file_id.clone());
        active.record.updated_at = now();

        let (progress_tx, mut progress_rx) = mpsc::channel::<u64>(16);
        tokio::spawn(async move {
            while let Some(written) = progress_rx.recv().await {
                progress.store(written, Ordering::Relaxed);
            }
        });

        let (result_tx, result_rx) = oneshot::channel::<Result<(), String>>();
        let internal = self.internal_tx.clone();
        tokio::spawn(async move {
            let result = match result_rx.await {
                Ok(result) => result,
                Err(_) => Err("Upload aborted".to_string()),
            };
            let _ = internal
                .send(InternalEvent::IncomingFileResult {
                    transfer_id: session_id,
                    file_id,
                    result,
                })
                .await;
        });

        let _ = target_tx.send(FileUploadTarget::Path {
            path,
            result_tx,
            progress_tx: Some(progress_tx),
        });
    }

    fn handle_incoming_file_result(
        &mut self,
        transfer_id: &str,
        file_id: &str,
        result: Result<(), String>,
    ) {
        let Some(active) = self
            .incoming
            .as_mut()
            .filter(|active| active.record.id == transfer_id)
        else {
            return;
        };
        active.in_flight.remove(file_id);
        let Some(file) = active
            .record
            .files
            .iter_mut()
            .find(|file| file.id == file_id)
        else {
            return;
        };
        match result {
            Ok(()) => {
                file.state = TransferFileState::Completed;
                file.completed_bytes = file.size;
                file.error = None;
            }
            Err(_) => {
                if let Some(path) = file.destination_path.as_deref() {
                    let _ = fs::remove_file(path);
                }
                file.state = TransferFileState::Failed;
                file.error = Some(ApiError::new(
                    "receive_file_failed",
                    format!("Could not receive {}", file.name),
                ));
            }
        }
        active.record.updated_at = now();
        self.finish_incoming_if_ready();
    }

    fn finish_incoming_if_ready(&mut self) {
        let ready = self
            .incoming
            .as_ref()
            .is_some_and(|active| active.end.is_some() && active.in_flight.is_empty());
        if !ready {
            return;
        }
        let mut active = self.incoming.take().expect("incoming was checked");
        sync_record_progress(&mut active.record, &active.progress);
        match active.end.expect("end was checked") {
            IncomingEnd::Cancelled => {
                active.record.state = TransferState::Cancelled;
                for file in &mut active.record.files {
                    if matches!(
                        file.state,
                        TransferFileState::Queued | TransferFileState::Transferring
                    ) {
                        if let Some(path) = file.destination_path.as_deref() {
                            let _ = fs::remove_file(path);
                        }
                        file.state = TransferFileState::Cancelled;
                    }
                }
            }
            IncomingEnd::Finished => {
                if active
                    .record
                    .files
                    .iter()
                    .any(|file| file.state == TransferFileState::Failed)
                {
                    active.record.state = TransferState::Failed;
                    active.record.error = Some(ApiError::new(
                        "receive_failed",
                        "One or more files could not be received",
                    ));
                } else {
                    active.record.state = TransferState::Completed;
                }
            }
        }
        active.record.completed_bytes = active
            .record
            .files
            .iter()
            .map(|file| file.completed_bytes)
            .sum();
        active.record.updated_at = now();
        active.record.finished_at = Some(now());
        self.push_recent(active.record);
    }

    fn handle_discovery_event(&mut self, event: DiscoveryEvent) {
        if matches!(event, DiscoveryEvent::MulticastFailed) {
            self.set_error(
                "multicast_stopped",
                "Multicast discovery stopped; manual HTTP refresh remains available",
            );
        }
    }

    fn add_confirmed_device(&self, host: String, mut info: RegisterDtoV2, fingerprint: String) {
        if fingerprint == self.identity.fingerprint {
            return;
        }
        info.fingerprint = fingerprint.clone();
        let device = DiscoveredDevice {
            alias: info.alias,
            version: info.version,
            device_model: info.device_model,
            device_type: info.device_type,
            fingerprint,
            channel: DeviceChannel::Http(HttpChannel {
                host,
                port: info.port,
                protocol: info.protocol,
            }),
            download: info.download,
        };
        let discovery = self.discovery.clone();
        tokio::spawn(async move {
            discovery.add_device(device).await;
        });
    }

    fn start_refresh(&mut self) -> String {
        if let Some(id) = &self.refresh_id {
            return id.clone();
        }
        let id = Uuid::new_v4().to_string();
        self.refresh_id = Some(id.clone());
        let discovery = self.discovery.clone();
        let internal = self.internal_tx.clone();
        let port = self.identity.port;
        let mut known = HashSet::new();
        for device in discovery.devices() {
            for channel in device.channels.keys() {
                if let Some(http) = channel.http() {
                    known.insert(http.clone());
                }
            }
        }
        let interfaces = local_interface_addresses(&InterfaceFilter::default()).unwrap_or_default();
        let event_id = id.clone();
        tokio::spawn(async move {
            let result = discovery
                .discover_staged(
                    known.into_iter().collect(),
                    interfaces,
                    port,
                    ProtocolType::Https,
                    Duration::from_secs(1),
                )
                .await
                .map_err(|_| "Staged discovery did not complete".to_string());
            let _ = internal
                .send(InternalEvent::RefreshFinished {
                    id: event_id,
                    result,
                })
                .await;
        });
        id
    }

    fn start_outgoing(
        &mut self,
        fingerprint: &str,
        items: Vec<OutgoingItem>,
    ) -> ApiResult<TransferSnapshot> {
        if self.outgoing.is_some() {
            return Err(ApiError::new(
                "outgoing_busy",
                "An outgoing transfer is already active",
            ));
        }
        validate_fingerprint(fingerprint)?;
        let device = self
            .discovery
            .devices()
            .into_iter()
            .find(|device| device.device.fingerprint.eq_ignore_ascii_case(fingerprint))
            .ok_or_else(|| ApiError::new("device_not_found", "Device is not in discovery state"))?;
        if !device_online(&device) {
            return Err(ApiError::new(
                "device_offline",
                "Device has not been confirmed recently; refresh discovery first",
            ));
        }
        let http = device
            .get_best_channel()
            .and_then(DeviceChannel::http)
            .cloned()
            .ok_or_else(|| ApiError::new("device_unreachable", "Device has no HTTP address"))?;

        let total_bytes = items.iter().try_fold(0u64, |total, item| {
            total
                .checked_add(item.file.size)
                .ok_or_else(|| ApiError::new("selection_too_large", "Total byte count overflowed"))
        })?;
        let transfer_id = Uuid::new_v4().to_string();
        let peer = peer_from_device(&device, &http);
        let created = now();
        let mut files: Vec<TransferFileSnapshot> = items
            .iter()
            .map(|item| TransferFileSnapshot {
                id: item.file.id.clone(),
                name: item.file.file_name.clone(),
                size: item.file.size,
                mime_type: item.file.file_type.clone(),
                state: TransferFileState::Queued,
                completed_bytes: 0,
                source_path: item.source_path.clone(),
                destination_path: None,
                error: None,
            })
            .collect();
        files.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        let record = TransferSnapshot {
            id: transfer_id.clone(),
            direction: TransferDirection::Outgoing,
            state: TransferState::Preparing,
            peer,
            file_count: files.len(),
            files,
            total_bytes,
            completed_bytes: 0,
            created_at: created.clone(),
            started_at: None,
            updated_at: created,
            finished_at: None,
            error: None,
        };
        let progress: HashMap<String, Arc<AtomicU64>> = items
            .iter()
            .map(|item| (item.file.id.clone(), Arc::new(AtomicU64::new(0))))
            .collect();
        let cancel = CancellationToken::new();
        let by_peer = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(run_outgoing(
            transfer_id,
            self.identity.clone(),
            device,
            http.clone(),
            items,
            progress.clone(),
            cancel.clone(),
            by_peer.clone(),
            self.internal_tx.clone(),
        ));
        self.outgoing = Some(OutgoingActive {
            record: record.clone(),
            progress,
            cancel,
            by_peer,
            host: http.host,
            remote_session_id: None,
            task,
        });
        Ok(record)
    }

    async fn accept_pending(&mut self, request_id: &str) -> ApiResult<TransferSnapshot> {
        validate_current_request(
            self.pending
                .as_ref()
                .map(|pending| pending.snapshot.id.as_str()),
            request_id,
        )?;
        if self.incoming.is_some() {
            return Err(ApiError::new(
                "incoming_busy",
                "An incoming transfer is already active",
            ));
        }
        let embedded_text = self
            .pending
            .as_ref()
            .and_then(|pending| embedded_message(&pending.files))
            .map(str::to_string);
        if let Some(text) = embedded_text {
            copy_to_clipboard(&text).await?;
            let pending = self.pending.take().expect("pending was validated");
            let mut record = transfer_from_pending(&pending, TransferState::Completed);
            pending
                .decision_tx
                .send(PrepareUploadDecisionV2::Accept(HashSet::new()))
                .map_err(|_| {
                    ApiError::new("stale_request", "Incoming request is no longer pending")
                })?;
            record.completed_bytes = record.total_bytes;
            for file in &mut record.files {
                file.state = TransferFileState::Completed;
                file.completed_bytes = file.size;
            }
            record.started_at = Some(now());
            record.updated_at = now();
            record.finished_at = Some(now());
            self.push_recent(record.clone());
            return Ok(record);
        }

        let mut offered: Vec<(String, String)> = self
            .pending
            .as_ref()
            .expect("pending was validated")
            .files
            .iter()
            .map(|(id, file)| (id.clone(), file.file_name.clone()))
            .collect();
        offered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let mut paths = HashMap::new();
        for (id, name) in offered {
            match reserve_unique_destination(&self.destination, &name) {
                Ok(path) => {
                    paths.insert(id, path);
                }
                Err(error) => {
                    remove_reservations(paths.into_values());
                    return Err(error);
                }
            }
        }

        let pending = self.pending.take().expect("pending was validated");
        let accepted: HashSet<String> = pending.files.keys().cloned().collect();
        let mut record = transfer_from_pending(&pending, TransferState::Transferring);
        if pending
            .decision_tx
            .send(PrepareUploadDecisionV2::Accept(accepted))
            .is_err()
        {
            remove_reservations(paths.into_values());
            return Err(ApiError::new(
                "stale_request",
                "Incoming request is no longer pending",
            ));
        }

        record.started_at = Some(now());
        record.updated_at = now();
        for file in &mut record.files {
            file.destination_path = paths
                .get(&file.id)
                .and_then(|path| path.to_str())
                .map(str::to_string);
        }
        self.incoming = Some(IncomingActive {
            record: record.clone(),
            sender: pending.sender,
            progress: HashMap::new(),
            in_flight: HashSet::new(),
            end: None,
        });
        Ok(record)
    }

    fn decline_pending(&mut self, request_id: &str) -> ApiResult<TransferSnapshot> {
        validate_current_request(
            self.pending
                .as_ref()
                .map(|pending| pending.snapshot.id.as_str()),
            request_id,
        )?;
        let pending = self.pending.take().expect("pending was validated");
        let mut record = transfer_from_pending(&pending, TransferState::Declined);
        pending
            .decision_tx
            .send(PrepareUploadDecisionV2::Decline)
            .map_err(|_| ApiError::new("stale_request", "Incoming request is no longer pending"))?;
        let finished = now();
        record.updated_at = finished.clone();
        record.finished_at = Some(finished);
        self.push_recent(record.clone());
        Ok(record)
    }

    async fn cancel_transfer(&mut self, transfer_id: &str) -> ApiResult<TransferSnapshot> {
        validate_uuid(transfer_id, "transfer")?;
        if let Some(active) = self
            .outgoing
            .as_mut()
            .filter(|active| active.record.id == transfer_id)
        {
            active.cancel.cancel();
            active.record.state = TransferState::Cancelling;
            active.record.updated_at = now();
            return Ok(active.record.clone());
        }
        if self
            .incoming
            .as_ref()
            .is_some_and(|active| active.record.id == transfer_id)
        {
            self.server.cancel_v2_session(transfer_id).await;
            let active = self.incoming.as_mut().expect("incoming was checked");
            active.end = Some(IncomingEnd::Cancelled);
            active.record.state = TransferState::Cancelling;
            active.record.updated_at = now();
            let response = active.record.clone();
            spawn_cancel_notification(
                self.identity.clone(),
                active.sender.clone(),
                transfer_id.to_string(),
            );
            self.finish_incoming_if_ready();
            return Ok(response);
        }
        Err(ApiError::new("stale_transfer", "Transfer is not active"))
    }

    fn sync_progress(&mut self) {
        if let Some(active) = &mut self.outgoing {
            sync_record_progress(&mut active.record, &active.progress);
        }
        if let Some(active) = &mut self.incoming {
            sync_record_progress(&mut active.record, &active.progress);
        }
    }

    fn snapshot(&self) -> Snapshot {
        let mut devices: Vec<DeviceSnapshot> = self
            .discovery
            .devices()
            .iter()
            .map(device_snapshot)
            .collect();
        devices.sort_by(|a, b| {
            b.online
                .cmp(&a.online)
                .then_with(|| a.alias.to_lowercase().cmp(&b.alias.to_lowercase()))
                .then_with(|| a.fingerprint.cmp(&b.fingerprint))
        });
        devices.truncate(MAX_DEVICES);

        let mut transfers = Vec::new();
        if let Some(incoming) = &self.incoming {
            transfers.push(incoming.record.clone());
        }
        if let Some(outgoing) = &self.outgoing {
            transfers.push(outgoing.record.clone());
        }
        transfers.extend(self.recent.iter().cloned());

        Snapshot {
            daemon: DaemonSnapshot {
                status: self.status.clone(),
                alias: self.identity.alias.clone(),
                fingerprint: self.identity.fingerprint.clone(),
                port: self.identity.port,
                destination: self.destination.to_string_lossy().into_owned(),
                socket: self.socket.to_string_lossy().into_owned(),
                pid: std::process::id(),
                protocol_version: PROTOCOL_VERSION_V2.to_string(),
                controller_version: env!("CARGO_PKG_VERSION").to_string(),
                started_at: self.started_at.clone(),
                refreshing: self.refresh_id.is_some(),
            },
            devices,
            incoming: self
                .pending
                .as_ref()
                .map(|pending| pending.snapshot.clone()),
            transfers,
            error: self.error.clone(),
        }
    }

    fn set_error(&mut self, code: impl Into<String>, message: impl Into<String>) {
        self.error = Some(ControllerError {
            code: code.into(),
            message: message.into(),
            at: now(),
        });
    }

    fn push_recent(&mut self, mut record: TransferSnapshot) {
        debug_assert!(record.state.terminal());
        record.files.truncate(MAX_RECENT_FILES);
        for file in &mut record.files {
            file.source_path = None;
        }
        self.recent.push_front(record);
        self.recent.truncate(MAX_RECENT_TRANSFERS);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_outgoing(
    transfer_id: String,
    identity: Arc<Identity>,
    device: StatefulDevice,
    channel: HttpChannel,
    items: Vec<OutgoingItem>,
    progress: HashMap<String, Arc<AtomicU64>>,
    cancel: CancellationToken,
    by_peer: Arc<AtomicBool>,
    internal: mpsc::Sender<InternalEvent>,
) {
    let expected_fingerprint = match channel.protocol {
        ProtocolType::Https => Some(device.device.fingerprint.clone()),
        ProtocolType::Http => None,
    };
    let client = match LsHttpClientV2::try_new(
        &identity.key_pem,
        &identity.cert_pem,
        expected_fingerprint,
        None,
    ) {
        Ok(client) => client,
        Err(_) => {
            send_outgoing_finished(
                &internal,
                transfer_id,
                TransferState::Failed,
                Some(ApiError::new(
                    "tls_client_error",
                    "Could not initialize the LocalSend TLS client",
                )),
            )
            .await;
            return;
        }
    };

    let files: HashMap<String, FileDto> = items
        .iter()
        .map(|item| (item.file.id.clone(), item.file.clone()))
        .collect();
    let embedded_text = embedded_message(&files).is_some();
    let sources: HashMap<String, OutgoingSource> = items
        .into_iter()
        .map(|item| (item.file.id, item.source))
        .collect();
    let prepared = client
        .prepare_upload(
            channel.protocol,
            &channel.host,
            channel.port,
            None,
            PrepareUploadRequestDtoV2 {
                info: identity.register_dto(),
                files: files.clone(),
            },
            None,
            cancel.clone(),
        )
        .await;

    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(ClientError::Cancelled) => {
            send_outgoing_finished(&internal, transfer_id, TransferState::Cancelled, None).await;
            return;
        }
        Err(ClientError::StatusCode(error)) if error.status == 403 => {
            send_outgoing_finished(&internal, transfer_id, TransferState::Declined, None).await;
            return;
        }
        Err(error) => {
            send_outgoing_finished(
                &internal,
                transfer_id,
                TransferState::Failed,
                Some(prepare_error(&error)),
            )
            .await;
            return;
        }
    };

    let Some(response) = prepared.response else {
        if embedded_text {
            for (id, file) in &files {
                if let Some(progress) = progress.get(id) {
                    progress.store(file.size, Ordering::Relaxed);
                }
            }
            send_outgoing_finished(&internal, transfer_id, TransferState::Completed, None).await;
        } else {
            send_outgoing_finished(&internal, transfer_id, TransferState::Declined, None).await;
        }
        return;
    };

    let accepted: HashSet<String> = response
        .files
        .keys()
        .filter(|id| files.contains_key(*id))
        .cloned()
        .collect();
    if accepted.is_empty() {
        send_outgoing_finished(&internal, transfer_id, TransferState::Declined, None).await;
        return;
    }
    let _ = internal
        .send(InternalEvent::OutgoingPrepared {
            transfer_id: transfer_id.clone(),
            session_id: response.session_id.clone(),
            accepted: accepted.clone(),
        })
        .await;

    let mut ids: Vec<String> = accepted.into_iter().collect();
    ids.sort_by(|a, b| files[a].file_name.cmp(&files[b].file_name));
    for file_id in ids {
        if cancel.is_cancelled() {
            if !by_peer.load(Ordering::Relaxed) {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.cancel(
                        channel.protocol,
                        &channel.host,
                        channel.port,
                        &response.session_id,
                    ),
                )
                .await;
            }
            send_outgoing_finished(&internal, transfer_id, TransferState::Cancelled, None).await;
            return;
        }
        let _ = internal
            .send(InternalEvent::OutgoingFileStarted {
                transfer_id: transfer_id.clone(),
                file_id: file_id.clone(),
            })
            .await;

        let Some(token) = response.files.get(&file_id) else {
            continue;
        };
        let file_progress = progress
            .get(&file_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let body = match upload_body(sources[&file_id].clone(), move |written| {
            file_progress.store(written, Ordering::Relaxed)
        }) {
            Ok(body) => body,
            Err(error) => {
                let _ = internal
                    .send(InternalEvent::OutgoingFileFailed {
                        transfer_id: transfer_id.clone(),
                        file_id,
                        error: error.clone(),
                    })
                    .await;
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.cancel(
                        channel.protocol,
                        &channel.host,
                        channel.port,
                        &response.session_id,
                    ),
                )
                .await;
                send_outgoing_finished(&internal, transfer_id, TransferState::Failed, Some(error))
                    .await;
                return;
            }
        };
        let result = client
            .upload(
                channel.protocol,
                &channel.host,
                channel.port,
                None,
                &response.session_id,
                &file_id,
                token,
                body,
                cancel.clone(),
            )
            .await;
        match result {
            Ok(()) => {
                if let Some(file_progress) = progress.get(&file_id) {
                    file_progress.store(files[&file_id].size, Ordering::Relaxed);
                }
                let _ = internal
                    .send(InternalEvent::OutgoingFileCompleted {
                        transfer_id: transfer_id.clone(),
                        file_id,
                    })
                    .await;
            }
            Err(ClientError::Cancelled) => {
                if !by_peer.load(Ordering::Relaxed) {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        client.cancel(
                            channel.protocol,
                            &channel.host,
                            channel.port,
                            &response.session_id,
                        ),
                    )
                    .await;
                }
                send_outgoing_finished(&internal, transfer_id, TransferState::Cancelled, None)
                    .await;
                return;
            }
            Err(_) => {
                let error = ApiError::new(
                    "upload_failed",
                    format!("Could not upload {}", files[&file_id].file_name),
                );
                let _ = internal
                    .send(InternalEvent::OutgoingFileFailed {
                        transfer_id: transfer_id.clone(),
                        file_id,
                        error: error.clone(),
                    })
                    .await;
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.cancel(
                        channel.protocol,
                        &channel.host,
                        channel.port,
                        &response.session_id,
                    ),
                )
                .await;
                send_outgoing_finished(&internal, transfer_id, TransferState::Failed, Some(error))
                    .await;
                return;
            }
        }
    }

    send_outgoing_finished(&internal, transfer_id, TransferState::Completed, None).await;
}

async fn send_outgoing_finished(
    internal: &mpsc::Sender<InternalEvent>,
    transfer_id: String,
    state: TransferState,
    error: Option<ApiError>,
) {
    let _ = internal
        .send(InternalEvent::OutgoingFinished {
            transfer_id,
            state,
            error,
        })
        .await;
}

fn prepare_error(error: &ClientError) -> ApiError {
    match error {
        ClientError::StatusCode(error) => match error.status {
            401 => ApiError::new("pin_required", "Receiver requires a PIN"),
            409 => ApiError::new("peer_busy", "Receiver is handling another transfer"),
            429 => ApiError::new("rate_limited", "Receiver rejected too many requests"),
            status => ApiError::new(
                "remote_error",
                format!("Receiver returned HTTP status {status}"),
            ),
        },
        ClientError::Json(_) => ApiError::new(
            "protocol_error",
            "Receiver returned an invalid LocalSend response",
        ),
        ClientError::Cancelled => ApiError::new("cancelled", "Transfer was cancelled"),
        _ => ApiError::new("network_error", "Could not contact the receiver"),
    }
}

fn upload_body(
    source: OutgoingSource,
    progress: impl Fn(u64) + Send + 'static,
) -> ApiResult<reqwest::Body> {
    let receiver = match source {
        OutgoingSource::Path {
            path,
            device,
            inode,
            size,
        } => {
            let file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)
                .map_err(|_| {
                    ApiError::new(
                        "source_changed",
                        "Selected source file is no longer readable",
                    )
                })?;
            let metadata = file.metadata().map_err(|_| {
                ApiError::new(
                    "source_changed",
                    "Selected source file could not be verified",
                )
            })?;
            if !metadata.is_file()
                || metadata.dev() != device
                || metadata.ino() != inode
                || metadata.len() != size
            {
                return Err(ApiError::new(
                    "source_changed",
                    "Selected source file changed before it could be sent",
                ));
            }
            let (tx, rx) = mpsc::channel(16);
            tokio::spawn(async move {
                let mut file = tokio::fs::File::from_std(file);
                let mut buffer = vec![0u8; 512 * 1024];
                loop {
                    match file.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(read) => {
                            if tx
                                .send(Bytes::copy_from_slice(&buffer[..read]))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
            rx
        }
        OutgoingSource::Bytes(bytes) => {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.try_send(Bytes::copy_from_slice(bytes.as_slice()));
            rx
        }
    };
    let mut sent = 0u64;
    let stream = ReceiverStream::new(receiver).map(move |chunk| {
        sent += chunk.len() as u64;
        progress(sent);
        Ok::<Bytes, anyhow::Error>(chunk)
    });
    Ok(reqwest::Body::wrap_stream(stream))
}

fn transfer_from_pending(pending: &PendingIncoming, state: TransferState) -> TransferSnapshot {
    let created = pending.snapshot.received_at.clone();
    let file_state = match state {
        TransferState::Declined => TransferFileState::Declined,
        TransferState::Cancelled => TransferFileState::Cancelled,
        TransferState::Completed => TransferFileState::Completed,
        _ => TransferFileState::Queued,
    };
    TransferSnapshot {
        id: pending.snapshot.id.clone(),
        direction: TransferDirection::Incoming,
        state,
        peer: pending.snapshot.sender.clone(),
        file_count: pending.snapshot.files.len(),
        files: pending
            .snapshot
            .files
            .iter()
            .map(|file| TransferFileSnapshot {
                id: file.id.clone(),
                name: file.name.clone(),
                size: file.size,
                mime_type: file.mime_type.clone(),
                state: file_state,
                completed_bytes: if file_state == TransferFileState::Completed {
                    file.size
                } else {
                    0
                },
                source_path: None,
                destination_path: None,
                error: None,
            })
            .collect(),
        total_bytes: pending.snapshot.total_bytes,
        completed_bytes: 0,
        created_at: created.clone(),
        started_at: None,
        updated_at: created,
        finished_at: None,
        error: None,
    }
}

fn validate_incoming_offer(files: &HashMap<String, FileDto>) -> ApiResult<()> {
    if files.is_empty() || files.len() > MAX_INCOMING_FILES {
        return Err(ApiError::new(
            "invalid_offer",
            format!("Incoming request must contain 1 to {MAX_INCOMING_FILES} files"),
        ));
    }
    let mut total = 0u64;
    let mut metadata_bytes = 0usize;
    for (id, file) in files {
        validate_file_id(id)?;
        if file.id != *id {
            return Err(ApiError::new(
                "invalid_offer",
                "Incoming file ID does not match its map key",
            ));
        }
        if file.file_name.is_empty()
            || file.file_name.len() > MAX_INCOMING_NAME_BYTES
            || file.file_name.chars().any(char::is_control)
        {
            return Err(ApiError::new(
                "invalid_offer",
                "Incoming file name is invalid or too long",
            ));
        }
        if file.file_type.len() > MAX_REMOTE_FIELD_BYTES
            || file.file_type.chars().any(char::is_control)
        {
            return Err(ApiError::new(
                "invalid_offer",
                "Incoming file type is invalid or too long",
            ));
        }
        if file
            .preview
            .as_ref()
            .is_some_and(|preview| preview.len() > crate::files::MAX_TEXT_BYTES)
        {
            return Err(ApiError::new(
                "invalid_offer",
                "Incoming text preview is too large",
            ));
        }
        metadata_bytes = metadata_bytes
            .checked_add(id.len() + file.file_name.len() + file.file_type.len())
            .and_then(|value| value.checked_add(file.preview.as_ref().map_or(0, String::len)))
            .ok_or_else(|| ApiError::new("invalid_offer", "Incoming metadata overflowed"))?;
        if metadata_bytes > MAX_INCOMING_METADATA_BYTES {
            return Err(ApiError::new(
                "invalid_offer",
                "Incoming transfer metadata is too large",
            ));
        }
        total = total
            .checked_add(file.size)
            .ok_or_else(|| ApiError::new("invalid_offer", "Incoming byte count overflowed"))?;
    }
    let _ = total;
    Ok(())
}

fn validate_sender_info(info: &RegisterDtoV2) -> ApiResult<()> {
    for (label, value) in [
        ("alias", info.alias.as_str()),
        ("version", info.version.as_str()),
        ("fingerprint", info.fingerprint.as_str()),
    ] {
        if value.is_empty()
            || value.len() > MAX_REMOTE_FIELD_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ApiError::new(
                "invalid_sender",
                format!("Sender {label} is invalid or too long"),
            ));
        }
    }
    if info.device_model.as_ref().is_some_and(|model| {
        model.len() > MAX_REMOTE_FIELD_BYTES || model.chars().any(char::is_control)
    }) {
        return Err(ApiError::new(
            "invalid_sender",
            "Sender device model is invalid or too long",
        ));
    }
    Ok(())
}

fn validate_file_id(value: &str) -> ApiResult<()> {
    if value.is_empty()
        || value.len() > MAX_REMOTE_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ApiError::new(
            "invalid_offer",
            "Incoming file ID is invalid or too long",
        ));
    }
    Ok(())
}

fn validate_current_request(current: Option<&str>, provided: &str) -> ApiResult<()> {
    validate_uuid(provided, "request")?;
    if current != Some(provided) {
        return Err(ApiError::new(
            "stale_request",
            "Incoming request is no longer pending",
        ));
    }
    Ok(())
}

fn validate_uuid(value: &str, kind: &str) -> ApiResult<()> {
    Uuid::parse_str(value)
        .map_err(|_| ApiError::new("invalid_id", format!("{kind} ID must be a valid UUID")))?;
    Ok(())
}

fn validate_fingerprint(value: &str) -> ApiResult<()> {
    if value.is_empty()
        || value.len() > 256
        || value.chars().any(|character| character.is_control())
    {
        return Err(ApiError::new(
            "invalid_fingerprint",
            "Device fingerprint is invalid",
        ));
    }
    Ok(())
}

fn message_preview(files: &HashMap<String, FileDto>) -> (Option<String>, bool) {
    if files.len() != 1 {
        return (None, false);
    }
    let Some(file) = files.values().next() else {
        return (None, false);
    };
    if !file.file_type.starts_with("text/") && file.file_type != "text" {
        return (None, false);
    }
    let Some(preview) = &file.preview else {
        return (None, false);
    };
    let mut characters = preview.chars();
    let shortened: String = characters
        .by_ref()
        .take(MAX_MESSAGE_PREVIEW_CHARS)
        .collect();
    let truncated = characters.next().is_some();
    (Some(shortened), truncated)
}

fn embedded_message(files: &HashMap<String, FileDto>) -> Option<&str> {
    if files.len() != 1 {
        return None;
    }
    let file = files.values().next()?;
    if (!file.file_type.starts_with("text/") && file.file_type != "text") || file.size == 0 {
        return None;
    }
    let preview = file.preview.as_deref()?;
    (preview.len() as u64 == file.size).then_some(preview)
}

async fn copy_to_clipboard(text: &str) -> ApiResult<()> {
    let mut child = tokio::process::Command::new("wl-copy")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| ApiError::new("clipboard_unavailable", "Could not execute wl-copy"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| ApiError::new("clipboard_unavailable", "Could not write the clipboard"))?;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        stdin.write_all(text.as_bytes()).await?;
        stdin.shutdown().await?;
        drop(stdin);
        child.wait().await
    })
    .await;
    let status = match result {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            return Err(ApiError::new(
                "clipboard_unavailable",
                "Could not write the clipboard",
            ));
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(ApiError::new(
                "clipboard_unavailable",
                "Writing the clipboard timed out",
            ));
        }
    };
    if !status.success() {
        return Err(ApiError::new(
            "clipboard_unavailable",
            "wl-copy could not write the clipboard",
        ));
    }
    Ok(())
}

fn sync_record_progress(record: &mut TransferSnapshot, progress: &HashMap<String, Arc<AtomicU64>>) {
    let mut changed = false;
    for file in &mut record.files {
        if let Some(counter) = progress.get(&file.id) {
            let value = counter.load(Ordering::Relaxed).min(file.size);
            if value != file.completed_bytes {
                file.completed_bytes = value;
                changed = true;
            }
        }
    }
    let total = record.files.iter().map(|file| file.completed_bytes).sum();
    if total != record.completed_bytes {
        record.completed_bytes = total;
        changed = true;
    }
    if changed {
        record.updated_at = now();
    }
}

fn device_snapshot(device: &StatefulDevice) -> DeviceSnapshot {
    let address = device
        .get_best_channel()
        .and_then(DeviceChannel::http)
        .map(address_snapshot);
    DeviceSnapshot {
        fingerprint: device.device.fingerprint.clone(),
        alias: device.device.alias.clone(),
        device_type: device.device.device_type.as_ref().map(device_type_name),
        model: device.device.device_model.clone(),
        address,
        online: device_online(device),
        last_seen_at: device
            .logs
            .last()
            .map(|log| format_system_time(log.timestamp)),
    }
}

fn peer_from_device(device: &StatefulDevice, channel: &HttpChannel) -> PeerSnapshot {
    PeerSnapshot {
        fingerprint: device.device.fingerprint.clone(),
        alias: device.device.alias.clone(),
        device_type: device.device.device_type.as_ref().map(device_type_name),
        model: device.device.device_model.clone(),
        address: address_snapshot(channel),
    }
}

fn address_snapshot(channel: &HttpChannel) -> AddressSnapshot {
    AddressSnapshot {
        host: channel.host.clone(),
        port: channel.port,
        protocol: protocol_name(channel.protocol).to_string(),
    }
}

fn device_online(device: &StatefulDevice) -> bool {
    let channel_available = device
        .channels
        .values()
        .any(|status| *status == ChannelStatus::Available);
    let recently_seen = device.logs.last().is_some_and(|log| {
        SystemTime::now()
            .duration_since(log.timestamp)
            .unwrap_or_default()
            <= ONLINE_TTL
    });
    channel_available && recently_seen
}

fn device_type_name(device_type: &DeviceType) -> String {
    match device_type {
        DeviceType::Mobile => "mobile",
        DeviceType::Desktop => "desktop",
        DeviceType::Web => "web",
        DeviceType::Headless => "headless",
        DeviceType::Server => "server",
    }
    .to_string()
}

fn protocol_name(protocol: ProtocolType) -> &'static str {
    match protocol {
        ProtocolType::Http => "http",
        ProtocolType::Https => "https",
    }
}

fn spawn_cancel_notification(identity: Arc<Identity>, sender: SenderTarget, session_id: String) {
    tokio::spawn(async move {
        let expected = match sender.protocol {
            ProtocolType::Https => Some(sender.fingerprint),
            ProtocolType::Http => None,
        };
        let Ok(client) = LsHttpClientV2::try_new(
            &identity.key_pem,
            &identity.cert_pem,
            expected,
            Some(Duration::from_secs(5)),
        ) else {
            return;
        };
        let _ = client
            .cancel(sender.protocol, &sender.host, sender.port, &session_id)
            .await;
    });
}

fn prepare_destination(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("Could not create destination {}", path.display()))?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Could not inspect destination {}", path.display()))?;
    anyhow::ensure!(metadata.is_dir(), "Destination is not a directory");
    fs::canonicalize(path).context("Could not resolve destination")
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn bind_management_socket(path: &Path) -> Result<(UnixListener, SocketGuard)> {
    let parent = path
        .parent()
        .context("Socket path has no parent directory")?;
    let parent_metadata =
        fs::symlink_metadata(parent).context("Could not inspect management socket directory")?;
    anyhow::ensure!(
        parent_metadata.is_dir() && !parent_metadata.file_type().is_symlink(),
        "Socket parent directory is invalid"
    );
    let current_uid = unsafe { libc::geteuid() };
    anyhow::ensure!(
        parent_metadata.uid() == current_uid && parent_metadata.mode() & 0o022 == 0,
        "Socket parent directory must be private to the current user"
    );
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Refusing to replace a non-socket management path"
            );
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => anyhow::bail!("A LocalSend controller daemon is already running"),
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    let current = fs::symlink_metadata(path)?;
                    anyhow::ensure!(
                        current.file_type().is_socket()
                            && current.dev() == metadata.dev()
                            && current.ino() == metadata.ino(),
                        "Management socket changed while checking whether it was stale"
                    );
                    fs::remove_file(path).context("Could not remove stale management socket")?;
                }
                Err(error) => {
                    return Err(error).context("Could not verify existing management socket");
                }
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not inspect management socket"),
    }

    let listener = UnixListener::bind(path).context("Could not bind management socket")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .context("Could not secure management socket")?;
    let metadata = fs::symlink_metadata(path)?;
    Ok((
        listener,
        SocketGuard {
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        },
    ))
}

async fn serve_management(
    listener: UnixListener,
    rpc_tx: mpsc::Sender<RpcCall>,
    cancel: CancellationToken,
) {
    let connections = TaskTracker::new();
    let permits = Arc::new(Semaphore::new(MAX_MANAGEMENT_CONNECTIONS));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        continue;
                    };
                    let rpc_tx = rpc_tx.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        handle_connection(stream, rpc_tx).await;
                    });
                }
                Err(_) => {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    }
                },
            }
        }
    }
    connections.close();
    connections.wait().await;
}

async fn handle_connection(stream: UnixStream, rpc_tx: mpsc::Sender<RpcCall>) {
    let current_uid = unsafe { libc::geteuid() };
    if stream
        .peer_cred()
        .ok()
        .is_none_or(|credentials| credentials.uid() != current_uid)
    {
        return;
    }

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half.take(MAX_RPC_BYTES + 1));
    let mut line = String::new();
    let response =
        match tokio::time::timeout(MANAGEMENT_READ_TIMEOUT, reader.read_line(&mut line)).await {
            Err(_) => RpcResponse::failure(ApiError::new(
                "request_timeout",
                "Management request timed out",
            )),
            Ok(Ok(0)) => RpcResponse::failure(ApiError::new("empty_request", "Request was empty")),
            Ok(Ok(read)) if read as u64 > MAX_RPC_BYTES => RpcResponse::failure(ApiError::new(
                "request_too_large",
                "Request exceeds the management API limit",
            )),
            Ok(Ok(_)) if !line.ends_with('\n') => RpcResponse::failure(ApiError::new(
                "invalid_request",
                "Request must end with a newline",
            )),
            Ok(Ok(_)) => {
                match serde_json::from_str::<RpcRequest>(line.trim_end_matches(['\r', '\n'])) {
                    Ok(request) => {
                        let (reply, response) = oneshot::channel();
                        if rpc_tx.send(RpcCall { request, reply }).await.is_err() {
                            RpcResponse::failure(ApiError::new(
                                "daemon_stopping",
                                "Controller daemon is stopping",
                            ))
                        } else {
                            match tokio::time::timeout(Duration::from_secs(30), response).await {
                                Ok(Ok(response)) => response,
                                Ok(Err(_)) => RpcResponse::failure(ApiError::new(
                                    "daemon_stopping",
                                    "Controller daemon stopped before replying",
                                )),
                                Err(_) => RpcResponse::failure(ApiError::new(
                                    "request_timeout",
                                    "Controller did not reply in time",
                                )),
                            }
                        }
                    }
                    Err(error) => RpcResponse::failure(ApiError::new(
                        "invalid_request",
                        format!("Invalid RPC request: {error}"),
                    )),
                }
            }
            Ok(Err(_)) => RpcResponse::failure(ApiError::new(
                "invalid_request",
                "Request is not valid UTF-8 JSON",
            )),
        };
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        write_response(&mut write_half, &response),
    )
    .await;
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(response).unwrap_or_else(|_| {
        br#"{"ok":false,"error":{"code":"serialization_error","message":"Could not serialize response"}}"#.to_vec()
    });
    if bytes.len() as u64 > MAX_RPC_BYTES {
        bytes = br#"{"ok":false,"error":{"code":"response_too_large","message":"Controller response exceeds the management API limit"}}"#.to_vec();
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.shutdown().await
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler should be available on Linux");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

fn emit_daemon_event(event: &str, result: serde_json::Value) {
    let value = json!({"ok": true, "event": event, "result": result});
    println!("{value}");
}

fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn format_system_time(time: SystemTime) -> String {
    OffsetDateTime::from(time)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_request_ids_are_rejected() {
        let current = "550e8400-e29b-41d4-a716-446655440000";
        let stale = "550e8400-e29b-41d4-a716-446655440001";
        assert!(validate_current_request(Some(current), current).is_ok());
        assert_eq!(
            validate_current_request(Some(current), stale)
                .unwrap_err()
                .code,
            "stale_request"
        );
        assert_eq!(
            validate_current_request(None, current).unwrap_err().code,
            "stale_request"
        );
        assert_eq!(
            validate_current_request(Some(current), "not-a-uuid")
                .unwrap_err()
                .code,
            "invalid_id"
        );
    }

    #[test]
    fn accepts_opaque_file_ids_and_identifies_only_complete_embedded_text() {
        let mut files = HashMap::from([(
            "file-a".to_string(),
            FileDto {
                id: "file-a".to_string(),
                file_name: "message.txt".to_string(),
                size: 5,
                file_type: "text/plain".to_string(),
                sha256: None,
                preview: Some("hello".to_string()),
                metadata: None,
            },
        )]);
        assert!(validate_incoming_offer(&files).is_ok());
        assert_eq!(embedded_message(&files), Some("hello"));

        files.get_mut("file-a").unwrap().size = 6;
        assert_eq!(embedded_message(&files), None);
    }
}
