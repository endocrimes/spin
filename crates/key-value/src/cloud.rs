use crate::{key_value::Error, Impl, ImplStore};
use cloud_kv::KVClient;
use bytes::Bytes;
use async_std::{
    sync::{Arc, Mutex},
};
use anyhow::Result;
use tokio::task;
use wit_bindgen_wasmtime::async_trait;

pub struct KeyValueCloud {
    client: Arc<Mutex<KVClient>>,
}

impl KeyValueCloud {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(KVClient::connect_lazy("http://3.76.121.86:8000").expect("uri already validated"))),
        }
    }
}

#[async_trait]
impl Impl for KeyValueCloud {
    async fn open(&mut self, name: &str) -> Result<Box<dyn ImplStore>, Error> {
        Ok(Box::new(CloudStore::new("default".to_owned(), self.client.clone())))
    }
}

struct CloudStore {
    namespace: String,
    client: Arc<Mutex<KVClient>>,
}

impl CloudStore {
    fn new(namespace: String, client: Arc<Mutex<KVClient>>) -> Self {
        Self {
            namespace,
            client
        }
    }
}

#[async_trait]
impl ImplStore for CloudStore {
    async fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.client
            .lock_arc()
            .await
            .get(self.namespace.clone(), key.to_owned())
            .await
            .map_err(|e| Error::InvalidStore )
            .map(|b| b.to_vec())
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.client
            .lock_arc()
            .await
            .set(self.namespace.clone(), key.to_owned(), Some(Bytes::from(value.to_owned())))
            .await
            .map_err(|e| Error::StoreTableFull )
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        self.client
            .lock_arc()
            .await
            .set(self.namespace.clone(), key.to_owned(), None)
            .await
            .map_err(|e| Error::InvalidStore )
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        match self.get(key).await {
            Ok(val) => {
                if val.len() > 0 {
                    Ok(true)
                } else {
                    Err(Error::NoSuchKey)
                }
            },
            Err(e) => Err(e),
        }
    }

    async fn get_keys(&self) -> Result<Vec<String>, Error> {
        self.client
            .lock_arc()
            .await
            .list_keys(self.namespace.clone())
            .await
            .map_err(|e| Error::InvalidStore )
    }
}
