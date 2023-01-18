use anyhow::{ensure, Result};
use spin_sdk::{
    http::{Request, Response},
    http_component,
    key_value::{Error, Namespace},
};

#[http_component]
fn handle_request(req: Request) -> Result<Response> {
    let namespace = Namespace::open("foo")?;

    ensure!(!namespace.exists("bar")?);

    ensure!(matches!(namespace.get("bar"), Err(Error::NoSuchKey)));

    namespace.set("bar", b"baz")?;

    ensure!(namespace.exists("bar")?);

    ensure!(b"baz" as &[_] == &namespace.get("bar")?);

    namespace.delete("bar")?;

    ensure!(!namespace.exists("bar")?);

    ensure!(matches!(namespace.get("bar"), Err(Error::NoSuchKey)));

    Ok(http::Response::builder().status(200).body(None)?)
}
