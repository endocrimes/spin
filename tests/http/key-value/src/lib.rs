use anyhow::{ensure, Result};
use spin_sdk::{
    http::{Request, Response},
    http_component,
    key_value::{Error, Store},
};

#[http_component]
fn handle_request(_req: Request) -> Result<Response> {
    let store = Store::open("")?;

    store.delete("bar")?;

    ensure!(!store.exists("bar")?);

    ensure!(matches!(store.get("bar"), Err(Error::NoSuchKey)));

    store.set("bar", b"baz")?;

    ensure!(store.exists("bar")?);

    ensure!(b"baz" as &[_] == &store.get("bar")?);

    store.set("bar", b"wow")?;

    ensure!(b"wow" as &[_] == &store.get("bar")?);

    store.delete("bar")?;

    ensure!(!store.exists("bar")?);

    ensure!(matches!(store.get("bar"), Err(Error::NoSuchKey)));

    Ok(http::Response::builder().status(200).body(None)?)
}
