/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use reqwest::Client;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::time;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use vector_store_text::IndexId;
use vector_store_text::Key;
use vector_store_text::Limit;
use vector_store_text::httproutes::PostIndexAddRequest;
use vector_store_text::httproutes::PostIndexSearchRequest;

fn enable_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_new("info").unwrap())
        .with(fmt::layer().with_target(false))
        .init();
}

pub(crate) struct HttpClient {
    client: Client,
    url_api: String,
}

impl HttpClient {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self {
            url_api: format!("http://{addr}/api/v1/text-search"),
            client: Client::new(),
        }
    }

    pub(crate) async fn indexes(&self) -> Vec<IndexId> {
        self.client
            .get(format!("{}", self.url_api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub(crate) async fn create(&self, id: &IndexId) {
        _ = self
            .client
            .put(format!("{}/{}", self.url_api, id))
            .send()
            .await
            .unwrap();
    }

    pub(crate) async fn add(&self, id: &IndexId, key: Key, text: String) {
        _ = self
            .client
            .post(format!("{}/{}/add", self.url_api, id))
            .json(&PostIndexAddRequest { id: key, text })
            .send()
            .await
            .unwrap()
    }

    pub(crate) async fn search(&self, id: &IndexId, text: String, limit: Limit) -> Vec<Key> {
        self.client
            .post(format!("{}/{}/search", self.url_api, id))
            .json(&PostIndexSearchRequest { text, limit })
            .send()
            .await
            .unwrap()
            .json::<Vec<Key>>()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn simple_create_search_delete_opensearch() {
    crate::enable_tracing();

    let index_factory = {
        let addr = dotenvy::var("OPENSEARCH_ADDRESS").unwrap_or("http://localhost".to_string());
        let port = dotenvy::var("OPENSEARCH_PORT").unwrap_or("9200".to_string());
        let addr = format!("{addr}:{port}");
        vector_store_text::new_index_factory(addr).unwrap()
    };

    let (_server_actor, addr) =
        vector_store_text::run(SocketAddr::from(([127, 0, 0, 1], 0)).into(), index_factory)
            .await
            .unwrap();
    let client = HttpClient::new(addr);

    assert!(client.indexes().await.is_empty());

    let id = "tstidx".to_string().into();

    client.create(&id).await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes.get(0).unwrap(), &id);

    client
        .add(&id, "key0".to_string().into(), " dead this ".to_string())
        .await;
    client
        .add(&id, "key1".to_string().into(), " beef that ".to_string())
        .await;

    time::timeout(Duration::from_secs(10), async {
        loop {
            let found = client
                .search(
                    &id,
                    "that".to_string(),
                    NonZeroUsize::new(1).unwrap().into(),
                )
                .await;
            if !found.is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();

    let found = client
        .search(
            &id,
            "that".to_string(),
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;
    assert_eq!(found.len(), 1);
    assert_eq!(found.get(0).unwrap().as_ref(), "key1");
}
