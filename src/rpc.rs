//! Typed RPC system for actor-to-actor communication.
//!
//! This module provides a type-safe request/response pattern for actors,
//! enabling structured communication with correlation IDs, timeouts, and
//! proper error handling.
//!
//! # Overview
//!
//! The RPC system consists of:
//!
//! - **[`RpcMessage`]**: Trait for serializable request/response types
//! - **[`RpcRequest`]/[`RpcResponse`]**: Wrapper types for RPC messages
//! - **[`RpcClient`]**: Client for making RPC calls to actors
//! - **[`RpcHandler`]**: Trait for handling RPC requests
//! - **[`RpcRegistry`]**: Registry for dispatching requests to handlers
//!
//! # Example
//!
//! ```ignore
//! use serde::{Deserialize, Serialize};
//! use actor_kit::rpc::{RpcMessage, RpcClient, RpcHandler, RpcRegistry, RpcRequest, RpcResponse};
//!
//! // Define message types
//! #[derive(Serialize, Deserialize)]
//! struct GetStatusRequest {
//!     component: String,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct GetStatusResponse {
//!     status: String,
//!     uptime_secs: u64,
//! }
//!
//! impl RpcMessage for GetStatusRequest {
//!     const TYPE_NAME: &'static str = "GetStatusRequest";
//! }
//!
//! impl RpcMessage for GetStatusResponse {
//!     const TYPE_NAME: &'static str = "GetStatusResponse";
//! }
//!
//! // Define a handler
//! struct StatusHandler;
//!
//! #[async_trait::async_trait]
//! impl RpcHandler<GetStatusRequest, GetStatusResponse> for StatusHandler {
//!     async fn handle(&self, request: RpcRequest<GetStatusRequest>) -> RpcResponse<GetStatusResponse> {
//!         RpcResponse::ok(request.correlation_id, GetStatusResponse {
//!             status: "running".to_string(),
//!             uptime_secs: 3600,
//!         })
//!     }
//! }
//!
//! // Register handler
//! let mut registry = RpcRegistry::new();
//! registry.register::<GetStatusRequest, GetStatusResponse, _>(StatusHandler);
//!
//! // Make RPC call
//! let response: GetStatusResponse = client.call(target_actor, GetStatusRequest {
//!     component: "database".into(),
//! }).await?;
//! ```

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;
use tracing::debug;
use uuid::Uuid;

use crate::Error;
use crate::{ActorId, ActorScheduler, Message, MessagePayload, Priority};

/// Trait for serializable RPC message types.
///
/// All request and response types must implement this trait to be used
/// with the RPC system.
pub trait RpcMessage: serde::Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// Unique type name for routing and debugging.
    const TYPE_NAME: &'static str;
}

/// A wrapped RPC request with metadata.
#[derive(Debug, Clone)]
pub struct RpcRequest<T: RpcMessage> {
    /// Sender actor ID
    pub from: ActorId,
    /// Target actor ID
    pub to: ActorId,
    /// Request payload
    pub payload: T,
    /// Correlation ID for matching responses
    pub correlation_id: Uuid,
    /// Request timeout
    pub timeout: Duration,
}

impl<T: RpcMessage> RpcRequest<T> {
    /// Create a new RPC request.
    pub fn new(from: ActorId, to: ActorId, payload: T) -> Self {
        Self {
            from,
            to,
            payload,
            correlation_id: Uuid::new_v4(),
            timeout: Duration::from_secs(30),
        }
    }

    /// Set a custom timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Serialize the request to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, RpcError> {
        let raw_request = RawRpcRequest {
            from: self.from,
            to: self.to,
            payload: bincode::serialize(&self.payload)
                .map_err(|e| RpcError::SerializationError(e.to_string()))?,
            correlation_id: self.correlation_id,
            timeout_ms: self.timeout.as_millis() as u64,
            type_name: T::TYPE_NAME.to_string(),
        };
        bincode::serialize(&raw_request).map_err(|e| RpcError::SerializationError(e.to_string()))
    }

    /// Deserialize a request from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RpcError> {
        let raw: RawRpcRequest =
            bincode::deserialize(bytes).map_err(|e| RpcError::SerializationError(e.to_string()))?;
        let payload: T = bincode::deserialize(&raw.payload)
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;
        Ok(Self {
            from: raw.from,
            to: raw.to,
            payload,
            correlation_id: raw.correlation_id,
            timeout: Duration::from_millis(raw.timeout_ms),
        })
    }
}

/// Raw RPC request for wire serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RawRpcRequest {
    from: ActorId,
    to: ActorId,
    payload: Vec<u8>,
    correlation_id: Uuid,
    timeout_ms: u64,
    type_name: String,
}

/// A wrapped RPC response with metadata.
#[derive(Debug, Clone)]
pub struct RpcResponse<T: RpcMessage> {
    /// Correlation ID matching the request
    pub correlation_id: Uuid,
    /// Response result
    pub result: Result<T, RpcError>,
}

impl<T: RpcMessage> RpcResponse<T> {
    /// Create a successful response.
    pub fn ok(correlation_id: Uuid, payload: T) -> Self {
        Self {
            correlation_id,
            result: Ok(payload),
        }
    }

    /// Create an error response.
    pub fn error(correlation_id: Uuid, error: RpcError) -> Self {
        Self {
            correlation_id,
            result: Err(error),
        }
    }

    /// Serialize the response to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, RpcError> {
        let raw_response = RawRpcResponse {
            correlation_id: self.correlation_id,
            result: match &self.result {
                Ok(payload) => {
                    let payload_bytes = bincode::serialize(payload)
                        .map_err(|e| RpcError::SerializationError(e.to_string()))?;
                    Ok(payload_bytes)
                }
                Err(e) => Err(RawRpcError::from(e)),
            },
        };
        bincode::serialize(&raw_response).map_err(|e| RpcError::SerializationError(e.to_string()))
    }

    /// Deserialize a response from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, RpcError> {
        let raw: RawRpcResponse =
            bincode::deserialize(bytes).map_err(|e| RpcError::SerializationError(e.to_string()))?;
        let result = match raw.result {
            Ok(payload_bytes) => {
                let payload: T = bincode::deserialize(&payload_bytes)
                    .map_err(|e| RpcError::SerializationError(e.to_string()))?;
                Ok(payload)
            }
            Err(e) => Err(RpcError::from(e)),
        };
        Ok(Self {
            correlation_id: raw.correlation_id,
            result,
        })
    }
}

/// Raw RPC response for wire serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RawRpcResponse {
    correlation_id: Uuid,
    result: Result<Vec<u8>, RawRpcError>,
}

/// Raw RPC error for wire serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RawRpcError {
    Timeout,
    ActorNotFound(ActorId),
    HandlerPanic(String),
    SerializationError(String),
    InternalError(String),
}

impl From<&RpcError> for RawRpcError {
    fn from(err: &RpcError) -> Self {
        match err {
            RpcError::Timeout => Self::Timeout,
            RpcError::ActorNotFound(id) => Self::ActorNotFound(*id),
            RpcError::HandlerPanic(s) => Self::HandlerPanic(s.clone()),
            RpcError::SerializationError(s) => Self::SerializationError(s.clone()),
            RpcError::InternalError(s) => Self::InternalError(s.clone()),
        }
    }
}

impl From<RawRpcError> for RpcError {
    fn from(err: RawRpcError) -> Self {
        match err {
            RawRpcError::Timeout => Self::Timeout,
            RawRpcError::ActorNotFound(id) => Self::ActorNotFound(id),
            RawRpcError::HandlerPanic(s) => Self::HandlerPanic(s),
            RawRpcError::SerializationError(s) => Self::SerializationError(s),
            RawRpcError::InternalError(s) => Self::InternalError(s),
        }
    }
}

/// Errors that can occur during RPC calls.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RpcError {
    /// Request timed out
    #[error("RPC request timed out")]
    Timeout,
    /// Target actor not found
    #[error("Actor not found: {0:?}")]
    ActorNotFound(ActorId),
    /// Handler panicked
    #[error("Handler panic: {0}")]
    HandlerPanic(String),
    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<RpcError> for Error {
    fn from(err: RpcError) -> Self {
        Error::actor(err.to_string())
    }
}

/// Internal message envelope for RPC communication.
///
/// Public so hosts building custom transports can wrap/unwrap RPC payloads
/// themselves; the runtime also re-exports it at the crate root.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcEnvelope {
    /// Message type name for routing
    type_name: String,
    /// Serialized request payload
    payload: Vec<u8>,
    /// Correlation ID
    correlation_id: Uuid,
}

/// Pending RPC call tracker.
struct PendingCall {
    /// One-shot sender for the response
    tx: oneshot::Sender<Vec<u8>>,
    /// Timeout deadline
    deadline: tokio::time::Instant,
}

/// Tracker for pending RPC calls.
#[derive(Default)]
struct PendingCalls {
    /// Map of correlation ID to pending call
    calls: RwLock<HashMap<Uuid, PendingCall>>,
}

impl PendingCalls {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&self, correlation_id: Uuid, tx: oneshot::Sender<Vec<u8>>, timeout: Duration) {
        let mut calls = self.calls.write();
        calls.insert(
            correlation_id,
            PendingCall {
                tx,
                deadline: tokio::time::Instant::now() + timeout,
            },
        );
    }

    fn remove(&self, correlation_id: &Uuid) -> Option<PendingCall> {
        self.calls.write().remove(correlation_id)
    }

    fn cleanup_expired(&self) {
        let now = tokio::time::Instant::now();
        let mut calls = self.calls.write();
        let expired: Vec<Uuid> = calls
            .iter()
            .filter(|(_, call)| call.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(call) = calls.remove(&id) {
                let error_response = RawRpcResponse {
                    correlation_id: id,
                    result: Err(RawRpcError::Timeout),
                };
                if let Ok(bytes) = bincode::serialize(&error_response) {
                    let _ = call.tx.send(bytes);
                }
            }
        }
    }
}

/// Client for making RPC calls to actors.
pub struct RpcClient {
    /// Reference to the actor scheduler
    scheduler: Arc<ActorScheduler>,
    /// Default timeout for RPC calls
    default_timeout: Duration,
    /// Pending calls tracker
    pending: Arc<PendingCalls>,
}

impl RpcClient {
    /// Create a new RPC client.
    pub fn new(scheduler: Arc<ActorScheduler>) -> Self {
        Self {
            scheduler,
            default_timeout: Duration::from_secs(30),
            pending: Arc::new(PendingCalls::new()),
        }
    }

    /// Set the default timeout for RPC calls.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Make an RPC call with the default timeout.
    pub async fn call<TReq, TResp>(&self, to: ActorId, request: TReq) -> Result<TResp, RpcError>
    where
        TReq: RpcMessage,
        TResp: RpcMessage,
    {
        self.call_with_timeout(to, request, self.default_timeout)
            .await
    }

    /// Make an RPC call with a custom timeout.
    pub async fn call_with_timeout<TReq, TResp>(
        &self,
        to: ActorId,
        request: TReq,
        timeout: Duration,
    ) -> Result<TResp, RpcError>
    where
        TReq: RpcMessage,
        TResp: RpcMessage,
    {
        let from = ActorId::new();
        let correlation_id = Uuid::new_v4();

        let rpc_request = RpcRequest {
            from,
            to,
            payload: request,
            correlation_id,
            timeout,
        };

        let envelope = RpcEnvelope {
            type_name: TReq::TYPE_NAME.to_string(),
            payload: rpc_request.to_bytes()?,
            correlation_id,
        };

        let envelope_bytes = bincode::serialize(&envelope)
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;

        let (tx, rx) = oneshot::channel();
        self.pending.insert(correlation_id, tx, timeout);

        let message = Message {
            sender: Some(from),
            payload: MessagePayload::Custom(envelope_bytes),
            priority: Priority::High,
        };

        self.scheduler
            .send(to, message)
            .await
            .map_err(|e| RpcError::InternalError(e.to_string()))?;

        let response_bytes = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                self.pending.remove(&correlation_id);
                RpcError::Timeout
            })?
            .map_err(|_| RpcError::InternalError("Response channel closed".to_string()))?;

        let response: RpcResponse<TResp> = RpcResponse::from_bytes(&response_bytes)?;

        response.result
    }

    /// Handle an incoming RPC response.
    pub fn handle_response(&self, bytes: &[u8]) -> Result<(), RpcError> {
        let envelope: RpcEnvelope =
            bincode::deserialize(bytes).map_err(|e| RpcError::SerializationError(e.to_string()))?;

        if let Some(call) = self.pending.remove(&envelope.correlation_id) {
            let _ = call.tx.send(envelope.payload);
        } else {
            debug!(
                "Received response for unknown correlation ID: {}",
                envelope.correlation_id
            );
        }

        Ok(())
    }

    /// Start the timeout cleanup task.
    pub fn start_cleanup_task(&self) -> tokio::task::JoinHandle<()> {
        let pending = self.pending.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                pending.cleanup_expired();
            }
        })
    }
}

/// Trait for handling RPC requests.
#[async_trait]
pub trait RpcHandler<TReq, TResp>: Send + Sync
where
    TReq: RpcMessage,
    TResp: RpcMessage,
{
    /// Handle an RPC request and return a response.
    async fn handle(&self, request: RpcRequest<TReq>) -> RpcResponse<TResp>;
}

/// Type-erased handler trait.
trait ErasedHandler: Send + Sync {
    fn handle(
        &self,
        payload: Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, RpcError>> + Send + 'static>,
    >;

    /// Clone the handler into a boxed trait object
    fn clone_boxed(&self) -> Box<dyn ErasedHandler>;
}

/// Wrapper to type-erase handlers.
struct HandlerWrapper<TReq, TResp, H>
where
    TReq: RpcMessage,
    TResp: RpcMessage,
    H: RpcHandler<TReq, TResp>,
{
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<(TReq, TResp)>,
}

impl<TReq, TResp, H> HandlerWrapper<TReq, TResp, H>
where
    TReq: RpcMessage,
    TResp: RpcMessage,
    H: RpcHandler<TReq, TResp>,
{
    fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<TReq, TResp, H> ErasedHandler for HandlerWrapper<TReq, TResp, H>
where
    TReq: RpcMessage,
    TResp: RpcMessage,
    H: RpcHandler<TReq, TResp> + 'static,
{
    fn handle(
        &self,
        payload: Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, RpcError>> + Send + 'static>,
    > {
        let handler = Arc::clone(&self.handler);
        Box::pin(async move {
            let request: RpcRequest<TReq> = RpcRequest::from_bytes(&payload)?;
            let response = handler.handle(request).await;
            response.to_bytes()
        })
    }

    fn clone_boxed(&self) -> Box<dyn ErasedHandler> {
        Box::new(Self {
            handler: Arc::clone(&self.handler),
            _phantom: std::marker::PhantomData,
        })
    }
}

/// Type name to handler mapping.
type HandlerEntry = Box<dyn ErasedHandler>;

/// Registry for RPC handlers.
///
/// Maps message type names to their handlers for dispatching incoming requests.
pub struct RpcRegistry {
    /// Handlers indexed by type name
    handlers: RwLock<HashMap<String, HandlerEntry>>,
    /// Type name to TypeId mapping for type-safe registration
    type_ids: RwLock<HashMap<String, TypeId>>,
}

impl RpcRegistry {
    /// Create a new RPC registry.
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
            type_ids: RwLock::new(HashMap::new()),
        }
    }

    /// Register a handler for a request/response type pair.
    pub fn register<TReq, TResp, H>(&self, handler: H)
    where
        TReq: RpcMessage,
        TResp: RpcMessage,
        H: RpcHandler<TReq, TResp> + 'static,
    {
        let type_name = TReq::TYPE_NAME.to_string();
        let wrapper = HandlerWrapper::<TReq, TResp, H>::new(handler);

        let mut handlers = self.handlers.write();
        let mut type_ids = self.type_ids.write();

        type_ids.insert(type_name.clone(), TypeId::of::<TReq>());
        handlers.insert(type_name, Box::new(wrapper));
    }

    /// Dispatch a request to the appropriate handler.
    pub async fn dispatch(&self, type_name: &str, payload: &[u8]) -> Result<Vec<u8>, RpcError> {
        // Clone the handler to avoid holding the lock across await
        let handler = {
            let handlers = self.handlers.read();
            let handler = handlers.get(type_name).ok_or_else(|| {
                RpcError::InternalError(format!("No handler registered for type: {}", type_name))
            })?;
            // Clone the boxed handler (Arc-like semantics via Box clone)
            handler.clone_boxed()
        };

        handler.handle(payload.to_vec()).await
    }

    /// Check if a handler is registered for a type.
    pub fn has_handler(&self, type_name: &str) -> bool {
        self.handlers.read().contains_key(type_name)
    }

    /// Get the list of registered type names.
    pub fn registered_types(&self) -> Vec<String> {
        self.handlers.read().keys().cloned().collect()
    }

    /// Unregister a handler by type name.
    pub fn unregister(&self, type_name: &str) -> bool {
        let mut handlers = self.handlers.write();
        let mut type_ids = self.type_ids.write();
        type_ids.remove(type_name);
        handlers.remove(type_name).is_some()
    }

    /// Clear all registered handlers.
    pub fn clear(&self) {
        self.handlers.write().clear();
        self.type_ids.write().clear();
    }
}

impl Default for RpcRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Process an incoming RPC message and dispatch to handlers.
pub async fn process_rpc_message(
    registry: &RpcRegistry,
    envelope_bytes: &[u8],
) -> Result<Vec<u8>, RpcError> {
    let envelope: RpcEnvelope = bincode::deserialize(envelope_bytes)
        .map_err(|e| RpcError::SerializationError(e.to_string()))?;

    registry
        .dispatch(&envelope.type_name, &envelope.payload)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestRequest {
        value: u32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestResponse {
        doubled: u64,
    }

    impl RpcMessage for TestRequest {
        const TYPE_NAME: &'static str = "TestRequest";
    }

    impl RpcMessage for TestResponse {
        const TYPE_NAME: &'static str = "TestResponse";
    }

    struct TestHandler;

    #[async_trait]
    impl RpcHandler<TestRequest, TestResponse> for TestHandler {
        async fn handle(&self, request: RpcRequest<TestRequest>) -> RpcResponse<TestResponse> {
            RpcResponse::ok(
                request.correlation_id,
                TestResponse {
                    doubled: request.payload.value as u64 * 2,
                },
            )
        }
    }

    #[test]
    fn test_rpc_request_serialization() {
        let from = ActorId::new();
        let to = ActorId::new();
        let request = RpcRequest::new(from, to, TestRequest { value: 42 });

        let bytes = request.to_bytes().unwrap();
        let decoded = RpcRequest::<TestRequest>::from_bytes(&bytes).unwrap();

        assert_eq!(request.payload, decoded.payload);
        assert_eq!(request.correlation_id, decoded.correlation_id);
    }

    #[test]
    fn test_rpc_response_serialization() {
        let correlation_id = Uuid::new_v4();
        let response = RpcResponse::ok(correlation_id, TestResponse { doubled: 84 });

        let bytes = response.to_bytes().unwrap();
        let decoded = RpcResponse::<TestResponse>::from_bytes(&bytes).unwrap();

        assert_eq!(response.correlation_id, decoded.correlation_id);
        assert_eq!(response.result.as_ref().unwrap().doubled, 84);
    }

    #[test]
    fn test_rpc_response_error() {
        let correlation_id = Uuid::new_v4();
        let response = RpcResponse::<TestResponse>::error(correlation_id, RpcError::Timeout);

        let bytes = response.to_bytes().unwrap();
        let decoded = RpcResponse::<TestResponse>::from_bytes(&bytes).unwrap();

        assert!(decoded.result.is_err());
        assert!(matches!(decoded.result.unwrap_err(), RpcError::Timeout));
    }

    #[test]
    fn test_rpc_registry_register() {
        let registry = RpcRegistry::new();

        assert!(!registry.has_handler("TestRequest"));

        registry.register::<TestRequest, TestResponse, _>(TestHandler);

        assert!(registry.has_handler("TestRequest"));
        assert!(registry
            .registered_types()
            .contains(&"TestRequest".to_string()));
    }

    #[test]
    fn test_rpc_registry_unregister() {
        let registry = RpcRegistry::new();
        registry.register::<TestRequest, TestResponse, _>(TestHandler);

        assert!(registry.unregister("TestRequest"));
        assert!(!registry.has_handler("TestRequest"));
    }

    #[tokio::test]
    async fn test_rpc_registry_dispatch() {
        let registry = RpcRegistry::new();
        registry.register::<TestRequest, TestResponse, _>(TestHandler);

        let from = ActorId::new();
        let to = ActorId::new();
        let request = RpcRequest::new(from, to, TestRequest { value: 21 });

        let response_bytes = registry
            .dispatch(TestRequest::TYPE_NAME, &request.to_bytes().unwrap())
            .await
            .unwrap();

        let response = RpcResponse::<TestResponse>::from_bytes(&response_bytes).unwrap();
        assert_eq!(response.result.unwrap().doubled, 42);
    }

    #[tokio::test]
    async fn test_rpc_registry_dispatch_unknown_type() {
        let registry = RpcRegistry::new();

        let result = registry.dispatch("UnknownType", &[]).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_pending_calls_insert_remove() {
        let pending = PendingCalls::new();
        let correlation_id = Uuid::new_v4();
        let (tx, _rx) = oneshot::channel();

        pending.insert(correlation_id, tx, Duration::from_secs(30));
        let removed = pending.remove(&correlation_id);

        assert!(removed.is_some());
        assert!(pending.remove(&correlation_id).is_none());
    }

    #[test]
    fn test_rpc_request_with_timeout() {
        let from = ActorId::new();
        let to = ActorId::new();
        let request = RpcRequest::new(from, to, TestRequest { value: 1 })
            .with_timeout(Duration::from_secs(60));

        assert_eq!(request.timeout, Duration::from_secs(60));
    }

    struct FailingHandler;

    #[async_trait]
    impl RpcHandler<TestRequest, TestResponse> for FailingHandler {
        async fn handle(&self, request: RpcRequest<TestRequest>) -> RpcResponse<TestResponse> {
            RpcResponse::error(
                request.correlation_id,
                RpcError::HandlerPanic("Test failure".to_string()),
            )
        }
    }

    #[tokio::test]
    async fn test_rpc_handler_error() {
        let registry = RpcRegistry::new();
        registry.register::<TestRequest, TestResponse, _>(FailingHandler);

        let from = ActorId::new();
        let to = ActorId::new();
        let request = RpcRequest::new(from, to, TestRequest { value: 1 });

        let response_bytes = registry
            .dispatch(TestRequest::TYPE_NAME, &request.to_bytes().unwrap())
            .await
            .unwrap();

        let response = RpcResponse::<TestResponse>::from_bytes(&response_bytes).unwrap();
        assert!(response.result.is_err());
        assert!(matches!(
            response.result.unwrap_err(),
            RpcError::HandlerPanic(_)
        ));
    }

    #[test]
    fn test_rpc_error_conversions() {
        let error = RpcError::Timeout;
        let kit_error: Error = error.into();
        assert!(kit_error.to_string().contains("timed out"));
    }

    #[test]
    fn test_rpc_registry_clear() {
        let registry = RpcRegistry::new();
        registry.register::<TestRequest, TestResponse, _>(TestHandler);
        assert!(!registry.registered_types().is_empty());

        registry.clear();
        assert!(registry.registered_types().is_empty());
    }
}
