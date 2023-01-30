use anyhow::Result;
use std::collections::HashMap;
use table::Table;
use wit_bindgen_wasmtime::async_trait;

mod host_component;
pub mod sqlite;
mod table;

pub use host_component::KeyValueComponent;

wit_bindgen_wasmtime::export!({paths: ["../../wit/ephemeral/key-value.wit"], async: *});

use key_value::{Error, KeyValue, Store};

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

#[derive(Clone)]
pub enum ImplConfig {
    Sqlite(sqlite::DatabaseLocation),
}

#[derive(Clone)]
pub struct Config {
    pub configs: HashMap<String, ImplConfig>,
}

#[async_trait]
trait Impl: Sync + Send {
    async fn open(&mut self, name: &str) -> Result<Box<dyn ImplStore>, Error>;
}

#[async_trait]
trait ImplStore: Sync + Send {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Error>;

    async fn delete(&self, key: &str) -> Result<(), Error>;

    async fn exists(&self, key: &str) -> Result<bool, Error>;
}

pub struct KeyValueDispatch {
    impls: HashMap<String, Box<dyn Impl>>,
    stores: Table<Box<dyn ImplStore>>,
}

impl KeyValueDispatch {
    fn new(config: Config) -> Self {
        Self {
            impls: config
                .configs
                .into_iter()
                .map(|(name, config)| {
                    (
                        name,
                        match config {
                            ImplConfig::Sqlite(location) => {
                                Box::new(sqlite::KeyValueSqlite::new(location)) as Box<dyn Impl>
                            }
                        },
                    )
                })
                .collect(),
            stores: Table::new(),
        }
    }
}

#[async_trait]
impl KeyValue for KeyValueDispatch {
    async fn open(&mut self, name: &str) -> Result<Store, Error> {
        self.stores
            .push(
                self.impls
                    .get_mut(name)
                    .ok_or(Error::NoSuchStore)?
                    .open(name)
                    .await?,
            )
            .map_err(|()| Error::StoreTableFull)
    }

    async fn get(&mut self, store: Store, key: &str) -> Result<Vec<u8>, Error> {
        self.stores
            .get(store)
            .ok_or(Error::InvalidStore)?
            .get(key)
            .await
    }

    async fn set(&mut self, store: Store, key: &str, value: &[u8]) -> Result<(), Error> {
        self.stores
            .get(store)
            .ok_or(Error::InvalidStore)?
            .set(key, value)
            .await
    }

    async fn delete(&mut self, store: Store, key: &str) -> Result<(), Error> {
        self.stores
            .get(store)
            .ok_or(Error::InvalidStore)?
            .delete(key)
            .await
    }

    async fn exists(&mut self, store: Store, key: &str) -> Result<bool, Error> {
        self.stores
            .get(store)
            .ok_or(Error::InvalidStore)?
            .exists(key)
            .await
    }

    async fn close(&mut self, store: Store) {
        self.stores.remove(store);
    }
}

fn log_error(err: impl std::fmt::Debug) -> Error {
    tracing::warn!("SQLite error: {err:?}");
    Error::Runtime(format!("{err:?}"))
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn all() -> Result<()> {
        let mut kv = KeyValueDispatch::new(Config {
            configs: [(
                "".to_owned(),
                ImplConfig::Sqlite(sqlite::DatabaseLocation::InMemory),
            )]
            .into_iter()
            .collect(),
        });

        assert!(matches!(
            kv.exists(42, "bar").await,
            Err(Error::InvalidStore)
        ));

        assert!(matches!(kv.open("foo").await, Err(Error::NoSuchStore)));

        let store = kv.open("").await?;

        assert!(!kv.exists(store, "bar").await?);

        assert!(matches!(kv.get(store, "bar").await, Err(Error::NoSuchKey)));

        kv.set(store, "bar", b"baz").await?;

        assert!(kv.exists(store, "bar").await?);

        assert_eq!(b"baz" as &[_], &kv.get(store, "bar").await?);

        kv.set(store, "bar", b"wow").await?;

        assert_eq!(b"wow" as &[_], &kv.get(store, "bar").await?);

        kv.delete(store, "bar").await?;

        assert!(!kv.exists(store, "bar").await?);

        assert!(matches!(kv.get(store, "bar").await, Err(Error::NoSuchKey)));

        kv.close(store).await;

        assert!(matches!(
            kv.exists(store, "bar").await,
            Err(Error::InvalidStore)
        ));

        Ok(())
    }
}
