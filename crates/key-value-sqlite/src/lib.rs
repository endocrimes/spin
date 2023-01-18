mod host_component;

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;
use wit_bindgen_wasmtime::async_trait;

pub use host_component::KeyValueSqliteComponent;

wit_bindgen_wasmtime::export!({paths: ["../../wit/ephemeral/key-value.wit"], async: *});

use key_value::{Error, KeyValue, Namespace};

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

#[derive(Default)]
pub struct KeyValueSqlite {
    connection: Option<Connection>,
    next_namespace: Namespace,
    namespaces: HashMap<Namespace, String>,
}

impl KeyValueSqlite {
    fn connect(&mut self) -> Result<(), Error> {
        if self.connection.is_none() {
            // TODO: Check spin.toml, a runtime config file, or CLI option for database path.
            let connection = Connection::open("spin_key_value.db").map_err(log_error)?;

            connection
                .execute(
                    "CREATE TABLE IF NOT EXISTS spin_key_value (
                       namespace TEXT NOT NULL,
                       key       TEXT NOT NULL,
                       value     BLOB NOT NULL,

                       PRIMARY KEY (namespace, key)
                    )",
                    (),
                )
                .map_err(log_error)?;

            // Note: We could consider preparing all statements here and storing them for later use.  That
            // would require `KeyValue` to be self-referential, in which case we'd need something like
            // https://crates.io/crates/ouroboros.  That will only be worth pursuing if we find that statement
            // preparation is a significant performance bottleneck.

            self.connection = Some(connection)
        }

        Ok(())
    }
}

#[async_trait]
impl KeyValue for KeyValueSqlite {
    async fn open(&mut self, name: &str) -> Result<Namespace, Error> {
        if self.namespaces.len() == u32::MAX as usize {
            Err(Error::NamespaceTableFull)
        } else {
            loop {
                let key = self.next_namespace;
                self.next_namespace = self.next_namespace.wrapping_add(1);
                if self.namespaces.contains_key(&key) {
                    continue;
                }
                self.namespaces.insert(key, name.to_owned());
                return Ok(key);
            }
        }
    }

    async fn get(&mut self, namespace: Namespace, key: &str) -> Result<Vec<u8>, Error> {
        self.connect()?;

        let name = self
            .namespaces
            .get(&namespace)
            .ok_or(Error::InvalidNamespace)?;

        let mut stmt = self
            .connection
            .as_ref()
            .unwrap()
            .prepare("SELECT value FROM spin_key_value WHERE namespace=$1 AND key=$2")
            .map_err(log_error)?;

        let value = stmt
            .query_map([name, key], |row| row.get(0))
            .map_err(log_error)?
            .collect::<Result<Vec<Vec<u8>>, _>>()
            .map_err(log_error)?
            .into_iter()
            .next()
            .ok_or(Error::NoSuchKey)?;

        Ok(value)
    }

    async fn set(&mut self, namespace: Namespace, key: &str, value: &[u8]) -> Result<(), Error> {
        self.connect()?;

        let name = self
            .namespaces
            .get(&namespace)
            .ok_or(Error::InvalidNamespace)?;

        let mut stmt = self
            .connection
            .as_ref()
            .unwrap()
            .prepare(
                "INSERT INTO spin_key_value (namespace, key, value) VALUES ($1, $2, $3)
                   ON CONFLICT(namespace, key) DO UPDATE SET value=$3",
            )
            .map_err(log_error)?;

        stmt.execute(rusqlite::params![name, key, value])
            .map_err(log_error)
            .map(drop)
    }

    async fn delete(&mut self, namespace: Namespace, key: &str) -> Result<(), Error> {
        self.connect()?;

        let name = self
            .namespaces
            .get(&namespace)
            .ok_or(Error::InvalidNamespace)?;

        let mut stmt = self
            .connection
            .as_ref()
            .unwrap()
            .prepare("DELETE FROM spin_key_value WHERE namespace=$1 AND key=$2")
            .map_err(log_error)?;

        stmt.execute([name, key]).map_err(log_error).map(drop)
    }

    async fn exists(&mut self, namespace: Namespace, key: &str) -> Result<bool, Error> {
        match self.get(namespace, key).await {
            Ok(_) => Ok(true),
            Err(Error::NoSuchKey) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn close(&mut self, namespace: Namespace) {
        self.namespaces.remove(&namespace);
    }
}

fn log_error(err: impl std::fmt::Debug) -> Error {
    tracing::warn!("SQLite error: {err:?}");
    Error::Store(format!("{err:?}"))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn all() -> Result<()> {
        let mut kv = KeyValueSqlite::default();

        assert!(matches!(
            kv.exists(42, "bar").await,
            Err(Error::InvalidNamespace)
        ));

        let namespace = kv.open("foo").await?;

        assert!(!kv.exists(namespace, "bar").await?);

        assert!(matches!(
            kv.get(namespace, "bar").await,
            Err(Error::NoSuchKey)
        ));

        kv.set(namespace, "bar", b"baz").await?;

        assert!(kv.exists(namespace, "bar").await?);

        assert_eq!(b"baz" as &[_], &kv.get(namespace, "bar").await?);

        kv.set(namespace, "bar", b"wow").await?;

        assert_eq!(b"wow" as &[_], &kv.get(namespace, "bar").await?);

        kv.delete(namespace, "bar").await?;

        assert!(!kv.exists(namespace, "bar").await?);

        assert!(matches!(
            kv.get(namespace, "bar").await,
            Err(Error::NoSuchKey)
        ));

        kv.close(namespace).await;

        assert!(matches!(
            kv.exists(namespace, "bar").await,
            Err(Error::InvalidNamespace)
        ));

        Ok(())
    }
}
