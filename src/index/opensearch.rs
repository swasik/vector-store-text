/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// TODO: Please remove if necessary implementation is provided.
#![allow(dead_code)]

use crate::IndexFactory;
use crate::IndexId;
use crate::Key;
use crate::index::actor::Index;
use opensearch::IndexParts;
use opensearch::OpenSearch;
use opensearch::http::Url;
use opensearch::http::transport::SingleNodeConnectionPool;
use opensearch::http::transport::TransportBuilder;
use opensearch::indices::IndicesCreateParts;
use opensearch::indices::IndicesDeleteParts;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::warn;

pub struct OpenSearchIndexFactory {
    client: Arc<OpenSearch>,
}

impl OpenSearchIndexFactory {
    fn create_opensearch_client(addr: &str) -> anyhow::Result<OpenSearch> {
        let address = Url::parse(addr)?;
        let conn_pool = SingleNodeConnectionPool::new(address);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
        let client = OpenSearch::new(transport);
        Ok(client)
    }
}

impl IndexFactory for OpenSearchIndexFactory {
    fn create_index(&self, id: IndexId) -> anyhow::Result<mpsc::Sender<Index>> {
        new(id, self.client.clone())
    }
}

pub fn new_opensearch(addr: &str) -> Result<OpenSearchIndexFactory, anyhow::Error> {
    Ok(OpenSearchIndexFactory {
        client: Arc::new(OpenSearchIndexFactory::create_opensearch_client(addr)?),
    })
}

async fn create_index(id: &IndexId, client: Arc<OpenSearch>) -> anyhow::Result<()> {
    _ = client
        .indices()
        .create(opensearch::indices::IndicesCreateParts::Index(&id.0))
        .body(json!({
            "mappings": {
                "properties": {
                    "article_id": { "type" : "text" },
                    "article_content": { "type" : "text" }
                }
            }
        }))
        .send()
        .await?;
    Ok(())
}

async fn delete_index(id: &IndexId, client: Arc<OpenSearch>) -> anyhow::Result<()> {
    _ = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[&id.0]))
        .send()
        .await?;
    Ok(())
}

pub fn new(id: IndexId, client: Arc<OpenSearch>) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with id: {id}");
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn({
        let cloned_id = id.clone();
        async move {
            if let Err(err) = delete_index(&id, client.clone()).await {
                warn!("engine::new: unable to delete index with id {id}: {err}");
            }
            if let Err(err) = create_index(&id, client.clone()).await {
                error!("engine::new: unable to create index with id {id}: {err}");
                return;
            }

            debug!("starting");

            // This semaphore decides how many tasks are queued for an opensearch process.
            // We are currently using SingleNodeConnectionPool, so we can only have one
            // connection to the server. This means that we can only have one task at a time,
            // so we set the semaphore to 2, so we always have something in queue.
            let semaphore = Arc::new(Semaphore::new(2));

            let id = Arc::new(id);

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let id = Arc::clone(&id);
                    let client = Arc::clone(&client);
                    async move {
                        process(msg, id, client).await;
                        drop(permit);
                    }
                });
            }

            debug!("finished");
        }
        .instrument(debug_span!("opensearch", "{cloned_id}"))
    });

    Ok(tx)
}

async fn process(msg: Index, id: Arc<IndexId>, client: Arc<OpenSearch>) {
    // TODO: Implement the logic for processing the messages
    match msg {
        Index::Add {
            article_id,
            article_content,
        } => add(id, article_id, article_content, client).await,
        Index::Remove { article_id } => {
            let _ = article_id;
        }
        Index::Search { text, limit, tx } => {
            let _ = text;
            let _ = limit;
            _ = tx.send(Ok(vec![]));
        }
    }
}

async fn add(id: Arc<IndexId>, article_id: Key, article_content: String, client: Arc<OpenSearch>) {
    _ = client
        .index(IndexParts::IndexId(&id.0, &article_id.0.to_string()))
        .body(json!({
            "article_id": article_id,
            "article_content": article_content,
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("add: unable to add text for a key {article_id}: {err}");
        });
}
