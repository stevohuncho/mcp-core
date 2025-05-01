use crate::protocol::{Protocol, RequestOptions};
use crate::transport::{
    JsonRpcError, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, Message, RequestId,
    Transport,
};
use crate::types::ErrorCode;
use anyhow::Result;
use async_trait::async_trait;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use tokio::time::timeout;
use tracing::debug;
use tokio::io::{AsyncBufReadExt, BufReader};

/// Server transport that communicates with MCP clients over standard I/O.
///
/// The `ServerStdioTransport` uses standard input and output streams (stdin/stdout)
/// to send and receive MCP messages. This transport is ideal for command-line
/// applications, where the server needs to communicate with a client that launched
/// it as a child process.
///
/// Use cases include:
/// - CLI tools that implement MCP
/// - Embedding MCP in existing command-line applications
/// - Testing and development scenarios
///
/// # Example
///
/// ```
/// use mcp_core::{protocol::Protocol, transport::{ServerStdioTransport, Transport}};
///
/// async fn example() {
///     let protocol = Protocol::builder().build();
///     let transport = ServerStdioTransport::new(protocol);
///     // Start handling messages
///     transport.open().await.expect("Failed to start stdio server");
/// }
/// ```
#[derive(Clone)]
pub struct ServerStdioTransport {
    protocol: Protocol,
}

impl ServerStdioTransport {
    /// Creates a new `ServerStdioTransport` instance.
    ///
    /// # Arguments
    ///
    /// * `protocol` - The MCP protocol instance to use for handling messages
    ///
    /// # Returns
    ///
    /// A new `ServerStdioTransport` instance
    pub fn new(protocol: Protocol) -> Self {
        Self { protocol }
    }
}

#[async_trait()]
impl Transport for ServerStdioTransport {
    /// Opens the transport and starts processing messages.
    ///
    /// This method enters a loop that:
    /// 1. Polls for incoming messages from stdin
    /// 2. Processes each message according to its type (request, notification, response)
    /// 3. Sends responses as needed
    /// 4. Continues until EOF is received on stdin
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn open(&self) -> Result<()> {
        loop {
            match self.poll_message().await {
                Ok(Some(message)) => match message {
                    Message::Request(request) => {
                        let response = self.protocol.handle_request(request).await;
                        self.send_response(response.id, response.result, response.error)
                            .await?;
                    }
                    Message::Notification(notification) => {
                        self.protocol.handle_notification(notification).await;
                    }
                    Message::Response(response) => {
                        self.protocol.handle_response(response).await;
                    }
                },
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    tracing::error!("Error receiving message: {:?}", e);
                }
            }
        }
        Ok(())
    }

    /// Closes the transport.
    ///
    /// This is a no-op for the stdio transport as standard I/O streams are managed by the OS.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success
    async fn close(&self) -> Result<()> {
        Ok(())
    }

    /// Polls for incoming messages from stdin.
    ///
    /// This method reads a line from stdin and parses it as a JSON-RPC message.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<Message>`. `None` indicates EOF.
    async fn poll_message(&self) -> Result<Option<Message>> {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line.is_empty() {
            return Ok(None);
        }

        debug!("Received: {line}");
        let message: Message = serde_json::from_str(&line)?;
        Ok(Some(message))
    }

    /// Sends a request to the client and waits for a response.
    ///
    /// This method:
    /// 1. Creates a new request ID
    /// 2. Constructs a JSON-RPC request
    /// 3. Sends it to stdout
    /// 4. Waits for a response with the same ID, with a timeout
    ///
    /// # Arguments
    ///
    /// * `method` - The method name for the request
    /// * `params` - Optional parameters for the request
    /// * `options` - Request options (like timeout)
    ///
    /// # Returns
    ///
    /// A `Future` that resolves to a `Result` containing the response
    fn request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
        options: RequestOptions,
    ) -> Pin<Box<dyn Future<Output = Result<JsonRpcResponse>> + Send + Sync>> {
        let protocol = self.protocol.clone();
        let method = method.to_owned();
        Box::pin(async move {
            let (id, rx) = protocol.create_request().await;
            let request = JsonRpcRequest {
                id,
                method,
                jsonrpc: Default::default(),
                params,
            };
            let serialized = serde_json::to_string(&request).unwrap_or_default();
            debug!("Sending: {serialized}");

            // Use Tokio's async stdout to perform thread-safe, nonblocking writes.
            let mut stdout = io::stdout();
            stdout.write_all(serialized.as_bytes())?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;

            let result = timeout(options.timeout, rx).await;
            match result {
                // The request future completed before the timeout.
                Ok(inner_result) => match inner_result {
                    Ok(response) => Ok(response),
                    Err(_) => {
                        protocol.cancel_response(id).await;
                        Ok(JsonRpcResponse {
                            id,
                            result: None,
                            error: Some(JsonRpcError {
                                code: ErrorCode::RequestTimeout as i32,
                                message: "Request cancelled".to_string(),
                                data: None,
                            }),
                            ..Default::default()
                        })
                    }
                },
                // The timeout expired.
                Err(_) => {
                    protocol.cancel_response(id).await;
                    Ok(JsonRpcResponse {
                        id,
                        result: None,
                        error: Some(JsonRpcError {
                            code: ErrorCode::RequestTimeout as i32,
                            message: "Request cancelled".to_string(),
                            data: None,
                        }),
                        ..Default::default()
                    })
                }
            }
        })
    }

    /// Sends a notification to the client.
    ///
    /// This method constructs a JSON-RPC notification and writes it to stdout.
    /// Unlike requests, notifications do not expect a response.
    ///
    /// # Arguments
    ///
    /// * `method` - The method name for the notification
    /// * `params` - Optional parameters for the notification
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn send_notification(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: Default::default(),
            method: method.to_owned(),
            params,
        };
        let serialized = serde_json::to_string(&notification).unwrap_or_default();
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        debug!("Sending: {serialized}");
        writer.write_all(serialized.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    /// Sends a response to the client.
    ///
    /// This method constructs a JSON-RPC response and writes it to stdout.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the request being responded to
    /// * `result` - Optional successful result
    /// * `error` - Optional error information
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn send_response(
        &self,
        id: RequestId,
        result: Option<serde_json::Value>,
        error: Option<JsonRpcError>,
    ) -> Result<()> {
        let response = JsonRpcResponse {
            id,
            result,
            error,
            jsonrpc: Default::default(),
        };
        let serialized = serde_json::to_string(&response).unwrap_or_default();
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        debug!("Sending: {serialized}");
        writer.write_all(serialized.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }
}
