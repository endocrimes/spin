use anyhow::{ensure, Result};
use spin_sdk::{
    http::{Request, Response},
    http_component,
    key_value::{Error, Store},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Default)]
pub struct Data {
    view_count: u64
}

#[http_component]
fn handle_request(_req: Request) -> Result<Response> {
    let store = Store::open("")?;

    let raw = store.get("/app-data").unwrap_or_default();
    let mut data: Data = serde_json::from_slice(&raw[..]).unwrap_or_default();
    data.view_count += 1;
    let new = serde_json::to_vec(&data).unwrap_or_default();
    store.set("/app-data", new)?;
    let response = serde_json::to_string(&data).unwrap_or_default();
    Ok(http::Response::builder().status(200).body(Some(response.into()))?)
}
