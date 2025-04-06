use crate::types::{CallToolRequest, CallToolResponse, Tool};
use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

pub struct Tools {
    tool_handlers: HashMap<String, ToolHandler>,
}

impl Tools {
    pub(crate) fn new(map: HashMap<String, ToolHandler>) -> Self {
        Self { tool_handlers: map }
    }

    pub fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_handlers
            .get(name)
            .map(|tool_handler| tool_handler.tool.clone())
    }

    pub async fn call_tool(&self, req: CallToolRequest) -> Result<CallToolResponse> {
        let handler = self
            .tool_handlers
            .get(&req.name)
            .ok_or_else(|| anyhow::anyhow!("Tool not found: {}", req.name))?;

        Ok((handler.f)(req).await)
    }

    pub fn list_tools(&self) -> Vec<Tool> {
        self.tool_handlers
            .values()
            .map(|tool_handler| tool_handler.tool.clone())
            .collect()
    }
}

pub type ToolHandlerFn = Box<
    dyn Fn(CallToolRequest) -> Pin<Box<dyn Future<Output = CallToolResponse> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

pub(crate) struct ToolHandler {
    pub tool: Tool,
    pub f: Box<ToolHandlerFn>,
}
