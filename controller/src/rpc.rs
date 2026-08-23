use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ApiError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "command", rename_all = "kebab-case")]
pub enum RpcRequest {
    Ping {},
    Snapshot {},
    Refresh {},
    SendFiles { device: String, paths: Vec<String> },
    SendText { device: String, text: String },
    Accept { request: String },
    Decline { request: String },
    Cancel { transfer: String },
    Shutdown {},
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RpcResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl RpcResponse {
    pub fn success<T: Serialize>(result: T) -> Self {
        match serde_json::to_value(result) {
            Ok(result) => Self {
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(_) => Self::failure(ApiError::new(
                "serialization_error",
                "Could not serialize the response",
            )),
        }
    }

    pub fn failure(error: ApiError) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(error),
        }
    }

    pub fn from_result<T: Serialize>(result: ApiResult<T>) -> Self {
        match result {
            Ok(result) => Self::success(result),
            Err(error) => Self::failure(error),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub daemon: DaemonSnapshot,
    pub devices: Vec<DeviceSnapshot>,
    pub incoming: Option<IncomingRequestSnapshot>,
    pub transfers: Vec<TransferSnapshot>,
    pub error: Option<ControllerError>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonSnapshot {
    pub status: String,
    pub alias: String,
    pub fingerprint: String,
    pub port: u16,
    pub destination: String,
    pub socket: String,
    pub pid: u32,
    pub protocol_version: String,
    pub controller_version: String,
    pub started_at: String,
    pub refreshing: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerError {
    pub code: String,
    pub message: String,
    pub at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSnapshot {
    pub fingerprint: String,
    pub alias: String,
    #[serde(rename = "type")]
    pub device_type: Option<String>,
    pub model: Option<String>,
    pub address: Option<AddressSnapshot>,
    pub online: bool,
    pub last_seen_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressSnapshot {
    pub host: String,
    pub port: u16,
    pub protocol: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSnapshot {
    pub fingerprint: String,
    pub alias: String,
    #[serde(rename = "type")]
    pub device_type: Option<String>,
    pub model: Option<String>,
    pub address: AddressSnapshot,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingRequestSnapshot {
    pub id: String,
    pub sender: PeerSnapshot,
    pub files: Vec<OfferedFileSnapshot>,
    pub total_bytes: u64,
    pub message_preview: Option<String>,
    pub message_preview_truncated: bool,
    pub received_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OfferedFileSnapshot {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferDirection {
    Incoming,
    Outgoing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferState {
    Preparing,
    Transferring,
    Cancelling,
    Completed,
    Declined,
    Cancelled,
    Failed,
}

impl TransferState {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Declined | Self::Cancelled | Self::Failed
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferFileState {
    Queued,
    Transferring,
    Completed,
    Declined,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferFileSnapshot {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    pub state: TransferFileState,
    pub completed_bytes: u64,
    pub source_path: Option<String>,
    pub destination_path: Option<String>,
    pub error: Option<ApiError>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSnapshot {
    pub id: String,
    pub direction: TransferDirection,
    pub state: TransferState,
    pub peer: PeerSnapshot,
    pub file_count: usize,
    pub files: Vec<TransferFileSnapshot>,
    pub total_bytes: u64,
    pub completed_bytes: u64,
    pub created_at: String,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub error: Option<ApiError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_serializes_rpc_commands() {
        let request: RpcRequest = serde_json::from_str(
            r#"{"command":"send-files","device":"ABC","paths":["/tmp/a","/tmp/b"]}"#,
        )
        .unwrap();
        match request {
            RpcRequest::SendFiles { device, paths } => {
                assert_eq!(device, "ABC");
                assert_eq!(paths, ["/tmp/a", "/tmp/b"]);
            }
            _ => panic!("wrong request variant"),
        }

        let serialized = serde_json::to_value(RpcRequest::Accept {
            request: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        })
        .unwrap();
        assert_eq!(serialized["command"], "accept");
        assert_eq!(
            serialized["request"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn response_envelopes_have_one_payload_branch() {
        let success = serde_json::to_value(RpcResponse::success(serde_json::json!({
            "status": "running"
        })))
        .unwrap();
        assert_eq!(success["ok"], true);
        assert!(success.get("result").is_some());
        assert!(success.get("error").is_none());

        let failure = serde_json::to_value(RpcResponse::failure(ApiError::new(
            "stale_request",
            "Request is no longer pending",
        )))
        .unwrap();
        assert_eq!(failure["ok"], false);
        assert_eq!(failure["error"]["code"], "stale_request");
        assert!(failure.get("result").is_none());
    }

    #[test]
    fn rejects_unknown_rpc_fields() {
        let parsed =
            serde_json::from_str::<RpcRequest>(r#"{"command":"ping","unexpected":"not accepted"}"#);
        assert!(parsed.is_err());
    }
}
