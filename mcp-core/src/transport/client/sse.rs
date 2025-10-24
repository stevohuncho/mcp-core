use crate::protocol::{Protocol, ProtocolBuilder, RequestOptions};
use crate::transport::{
    JsonRpcError, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, Message, RequestId,
    Transport,
};
use crate::types::ErrorCode;
use anyhow::Result;
use async_trait::async_trait;
use futures::TryStreamExt;
use reqwest_eventsource::{Event, EventSource};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::debug;

/// Client transport that communicates with an MCP server over Server-Sent Events (SSE).
///
/// The `ClientSseTransport` establishes a connection to an MCP server using Server-Sent
/// Events (SSE) for receiving messages from the server, and HTTP for sending messages
/// to the server. This transport is suitable for web-based applications and environments
/// where network communication is required.
///
/// Features:
/// - Uses SSE for efficient one-way server-to-client communication
/// - Uses HTTP for client-to-server communication
/// - Supports authentication with bearer tokens
/// - Allows custom HTTP headers
/// - Automatically manages session state
///
/// # Example
///
/// ```
/// use mcp_core::transport::ClientSseTransport;
///
/// async fn example() {
///     let transport = ClientSseTransport::builder("https://example.com/sse".to_string())
///         .with_bearer_token("my-token".to_string())
///         .with_header("User-Agent", "My MCP Client")
///         .build();
///     
///     transport.open().await.expect("Failed to open SSE connection");
///     // Use transport...
///     transport.close().await.expect("Failed to close SSE connection");
/// }
/// ```
#[derive(Clone)]
pub struct ClientSseTransport {
    protocol: Protocol,
    server_url: String,
    client: reqwest::Client,
    bearer_token: Option<String>,
    session_endpoint: Arc<Mutex<Option<String>>>,
    headers: HashMap<String, String>,
    event_source: Arc<Mutex<Option<EventSource>>>,
}

/// Builder for configuring and creating `ClientSseTransport` instances.
///
/// This builder allows customizing the SSE transport with options like:
/// - Server URL
/// - Authentication tokens
/// - Custom HTTP headers
///
/// Use this builder to create a new `ClientSseTransport` with the desired configuration.
pub struct ClientSseTransportBuilder {
    server_url: String,
    bearer_token: Option<String>,
    headers: HashMap<String, String>,
    protocol_builder: ProtocolBuilder,
}

impl ClientSseTransportBuilder {
    /// Creates a new builder with the specified server URL.
    ///
    /// # Arguments
    ///
    /// * `server_url` - The URL of the SSE endpoint on the MCP server
    ///
    /// # Returns
    ///
    /// A new `ClientSseTransportBuilder` instance
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
            bearer_token: None,
            headers: HashMap::new(),
            protocol_builder: ProtocolBuilder::new(),
        }
    }

    /// Adds a bearer token for authentication.
    ///
    /// This token will be included in the `Authorization` header as `Bearer {token}`.
    ///
    /// # Arguments
    ///
    /// * `token` - The bearer token to use for authentication
    ///
    /// # Returns
    ///
    /// The modified builder instance
    pub fn with_bearer_token(mut self, token: String) -> Self {
        self.bearer_token = Some(token);
        self
    }

    /// Adds a custom HTTP header to the SSE request.
    ///
    /// # Arguments
    ///
    /// * `key` - The header name
    /// * `value` - The header value
    ///
    /// # Returns
    ///
    /// The modified builder instance
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Builds the `ClientSseTransport` with the configured options.
    ///
    /// # Returns
    ///
    /// A new `ClientSseTransport` instance
    pub fn build(self) -> ClientSseTransport {
        ClientSseTransport {
            protocol: self.protocol_builder.build(),
            server_url: self.server_url,
            client: reqwest::Client::new(),
            bearer_token: self.bearer_token,
            session_endpoint: Arc::new(Mutex::new(None)),
            headers: self.headers,
            event_source: Arc::new(Mutex::new(None)),
        }
    }
}

impl ClientSseTransport {
    /// Creates a new builder for configuring the transport.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the SSE endpoint on the MCP server
    ///
    /// # Returns
    ///
    /// A new `ClientSseTransportBuilder` instance
    pub fn builder(url: String) -> ClientSseTransportBuilder {
        ClientSseTransportBuilder::new(url)
    }
}

#[async_trait()]
impl Transport for ClientSseTransport {
    /// Opens the transport by establishing an SSE connection to the server.
    ///
    /// This method:
    /// 1. Creates an SSE connection to the server URL
    /// 2. Adds configured headers and authentication
    /// 3. Starts a background task for handling incoming messages
    /// 4. Waits for the session endpoint to be received
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn open(&self) -> Result<()> {
        debug!("ClientSseTransport: Opening transport");

        let mut request = self.client.get(self.server_url.clone());

        // Add custom headers
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        // Add auth header if configured
        if let Some(bearer_token) = &self.bearer_token {
            request = request.header("Authorization", format!("Bearer {}", bearer_token));
        }

        let event_source = EventSource::new(request)?;

        {
            let mut es_lock = self.event_source.lock().await;
            *es_lock = Some(event_source);
        }

        // Spawn a background task to continuously poll messages
        let transport_clone = self.clone();
        tokio::task::spawn(async move {
            loop {
                match transport_clone.poll_message().await {
                    Ok(Some(message)) => match message {
                        Message::Request(request) => {
                            let response = transport_clone.protocol.handle_request(request).await;
                            let _ = transport_clone
                                .send_response(response.id, response.result, response.error)
                                .await;
                        }
                        Message::Notification(notification) => {
                            let _ = transport_clone
                                .protocol
                                .handle_notification(notification)
                                .await;
                        }
                        Message::Response(response) => {
                            transport_clone.protocol.handle_response(response).await;
                        }
                    },
                    Ok(None) => continue, // No message or control message, continue polling
                    Err(e) => {
                        debug!("ClientSseTransport: Error polling message: {:?}", e);
                        // Maybe add some backoff or retry logic here
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // Wait for the session URL to be set
        let mut attempts = 0;
        while attempts < 100 {
            if self.session_endpoint.lock().await.is_some() {
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            attempts += 1;
        }

        Err(anyhow::anyhow!("Timeout waiting for initial SSE message"))
    }

    /// Closes the transport by terminating the SSE connection.
    ///
    /// This method:
    /// 1. Closes the EventSource connection
    /// 2. Clears the session endpoint
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    async fn close(&self) -> Result<()> {
        debug!("ClientSseTransport: Closing transport");
        // Close the event source
        *self.event_source.lock().await = None;

        // Clear the session URL
        *self.session_endpoint.lock().await = None;

        Ok(())
    }

    /// Polls for incoming messages from the SSE connection.
    ///
    /// This method processes SSE events and:
    /// - Handles control messages (like endpoint information)
    /// - Parses JSON-RPC messages
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<Message>` if a message is available
    async fn poll_message(&self) -> Result<Option<Message>> {
        let mut event_source_guard = self.event_source.lock().await;
        let event_source = event_source_guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Transport not opened"))?;

        match event_source.try_next().await {
            Ok(Some(event)) => match event {
                Event::Message(m) => {
                    if &m.event[..] == "endpoint" {
                        let endpoint = m
                            .data
                            .trim_start_matches("http://")
                            .trim_start_matches("https://")
                            .split_once('/')
                            .map(|(_, path)| format!("/{}", path))
                            .unwrap_or(m.data);
                        debug!("Received session endpoint: {}", endpoint);
                        *self.session_endpoint.lock().await = Some(endpoint);
                        return Ok(None); // This is a control message, not a JSON-RPC message
                    } else {
                        debug!("Received SSE message: {}", m.data);
                        let message: Message = serde_json::from_str(&m.data)?;
                        return Ok(Some(message));
                    }
                }
                _ => return Ok(None),
            },
            Ok(None) => return Ok(None), // Stream ended
            Err(e) => {
                debug!("Error receiving SSE message: {:?}", e);
                return Err(anyhow::anyhow!("Failed to parse SSE message: {:?}", e));
            }
        }
    }

    /// Sends a request to the server via HTTP and waits for a response.
    ///
    /// This method:
    /// 1. Creates a JSON-RPC request
    /// 2. Sends it to the session endpoint via HTTP POST
    /// 3. Waits for a response with the same ID through the SSE connection
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
        let client = self.client.clone();
        let server_url = self.server_url.clone();
        let session_endpoint = self.session_endpoint.clone();
        let bearer_token = self.bearer_token.clone();
        let method = method.to_owned();
        let headers = self.headers.clone();

        Box::pin(async move {
            let (id, rx) = protocol.create_request().await;
            let request = JsonRpcRequest {
                id,
                method,
                jsonrpc: Default::default(),
                params,
            };

            // Get the session URL
            let session_url = {
                let url = session_endpoint.lock().await;
                url.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("No session URL available"))?
                    .clone()
            };

            let base_url = if let Some(idx) = server_url.find("://") {
                let domain_start = idx + 3;
                let domain_end = server_url[domain_start..]
                    .find('/')
                    .map(|i| domain_start + i)
                    .unwrap_or(server_url.len());
                &server_url[..domain_end]
            } else {
                let domain_end = server_url.find('/').unwrap_or(server_url.len());
                &server_url[..domain_end]
            }
            .to_string();

            debug!("ClientSseTransport: Base URL: {}", base_url);

            let full_url = format!("{}{}", base_url, session_url);
            debug!(
                "ClientSseTransport: Sending request to {}: {:?}",
                full_url, request
            );

            let mut req_builder = client.post(&full_url).json(&request);

            for (key, value) in headers {
                req_builder = req_builder.header(key, value);
            }

            if let Some(token) = bearer_token {
                req_builder = req_builder.header("Authorization", format!("Bearer {}", token));
            }

            let response = req_builder.send().await?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await?;
                return Err(anyhow::anyhow!(
                    "Failed to send request, status: {status}, body: {text}"
                ));
            }

            debug!("ClientSseTransport: Request sent successfully");

            // Wait for the response with a timeout
            let result = timeout(options.timeout, rx).await;
            match result {
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
                Err(_) => {
                    protocol.cancel_response(id).await;
                    Ok(JsonRpcResponse {
                        id,
                        result: None,
                        error: Some(JsonRpcError {
                            code: ErrorCode::RequestTimeout as i32,
                            message: "Request timed out".to_string(),
                            data: None,
                        }),
                        ..Default::default()
                    })
                }
            }
        })
    }

    /// Sends a response to a request previously received from the server.
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

        // Get the session URL
        let session_url = {
            let url = self.session_endpoint.lock().await;
            url.as_ref()
                .ok_or_else(|| anyhow::anyhow!("No session URL available"))?
                .clone()
        };

        let server_url = self.server_url.clone();
        let base_url = if let Some(idx) = server_url.find("://") {
            let domain_start = idx + 3;
            let domain_end = server_url[domain_start..]
                .find('/')
                .map(|i| domain_start + i)
                .unwrap_or(server_url.len());
            &server_url[..domain_end]
        } else {
            let domain_end = server_url.find('/').unwrap_or(server_url.len());
            &server_url[..domain_end]
        }
        .to_string();

        debug!("ClientSseTransport: Base URL: {}", base_url);

        let full_url = format!("{}{}", base_url, session_url);
        debug!(
            "ClientSseTransport: Sending response to {}: {:?}",
            full_url, response
        );

        let mut req_builder = self.client.post(&full_url).json(&response);

        for (key, value) in &self.headers {
            req_builder = req_builder.header(key, value);
        }

        if let Some(token) = &self.bearer_token {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", token));
        }

        let response = req_builder.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(anyhow::anyhow!(
                "Failed to send response, status: {status}, body: {text}"
            ));
        }

        Ok(())
    }

    /// Sends a notification to the server via HTTP.
    ///
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

        // Get the session URL
        let session_url = {
            let url = self.session_endpoint.lock().await;
            url.as_ref()
                .ok_or_else(|| anyhow::anyhow!("No session URL available"))?
                .clone()
        };

        let server_url = self.server_url.clone();
        let base_url = if let Some(idx) = server_url.find("://") {
            let domain_start = idx + 3;
            let domain_end = server_url[domain_start..]
                .find('/')
                .map(|i| domain_start + i)
                .unwrap_or(server_url.len());
            &server_url[..domain_end]
        } else {
            let domain_end = server_url.find('/').unwrap_or(server_url.len());
            &server_url[..domain_end]
        }
        .to_string();

        debug!("ClientSseTransport: Base URL: {}", base_url);

        let full_url = format!("{}{}", base_url, session_url);
        debug!(
            "ClientSseTransport: Sending notification to {}: {:?}",
            full_url, notification
        );

        let mut req_builder = self.client.post(&full_url).json(&notification);

        for (key, value) in &self.headers {
            req_builder = req_builder.header(key, value);
        }

        if let Some(token) = &self.bearer_token {
            req_builder = req_builder.header("Authorization", format!("Bearer {}", token));
        }

        let response = req_builder.send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await?;
            return Err(anyhow::anyhow!(
                "Failed to send notification, status: {status}, body: {text}"
            ));
        }

        Ok(())
    }
}
