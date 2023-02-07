use kv::{
    kv::v1::{kv_service_client::KvServiceClient, GetRequest, SetRequest, ListKeysRequest},
};
use tonic::{transport::{Channel, Endpoint}};
use bytes::Bytes;

pub struct KVClient {
    client: KvServiceClient<Channel>,
}

impl KVClient {
    pub fn new(channel: Channel) -> Self {
        let client = KvServiceClient::new(channel);
        Self { client }
    }

    pub fn connect_lazy<E>(endpoint: impl TryInto<Endpoint, Error = E>) -> std::result::Result<Self, E> {
        let endpoint = endpoint.try_into()?;
        Ok(Self::new(endpoint.connect_lazy()))
    }

    pub async fn get(&mut self, namespace: String, key: String) -> std::result::Result<Bytes, tonic::Status> {
        let msg = GetRequest{
            namespace,
            key,
        };

        self.client
            .get(msg)
            .await
            .map(|r| r.into_inner().value)
    }

    pub async fn set(&mut self, namespace: String, key: String, value: Option<Bytes>) -> std::result::Result<(), tonic::Status> {
        let msg = SetRequest{
            namespace,
            key,
            value: value.unwrap_or_default(),
        };

        self.client
            .set(msg)
            .await
            .map(|_| ())
    }

    pub async fn list_keys(&mut self, namespace: String) -> std::result::Result<Vec<String>, tonic::Status> {
        let msg = ListKeysRequest{
            namespace,
        };

        self.client
            .list_keys(msg)
            .await
            .map(|r| r.into_inner().keys.iter_mut().map(|k| k.key.clone()).collect())
    }
}
