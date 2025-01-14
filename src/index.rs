/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        Dimensions, Distance, Embeddings, Key, Limit,
    },
    anyhow::anyhow,
    tokio::sync::{mpsc, oneshot},
    usearch::IndexOptions,
};

pub(crate) enum Index {
    Ann {
        embeddings: Embeddings,
        limit: Limit,
        tx: oneshot::Sender<anyhow::Result<Vec<(Key, Distance)>>>,
    },
    Stop,
}

impl MessageStop for Index {
    fn message_stop() -> Self {
        Index::Stop
    }
}

pub(crate) trait IndexExt {
    async fn ann(
        &self,
        embeddings: Embeddings,
        limit: Limit,
    ) -> anyhow::Result<Vec<(Key, Distance)>>;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn ann(
        &self,
        embeddings: Embeddings,
        limit: Limit,
    ) -> anyhow::Result<Vec<(Key, Distance)>> {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embeddings,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }
}

pub(crate) fn new(dimensions: Dimensions) -> anyhow::Result<(mpsc::Sender<Index>, ActorHandle)> {
    let options = IndexOptions {
        dimensions: dimensions.0,
        ..Default::default()
    };
    let idx = usearch::Index::new(&options)?;
    idx.reserve(1000)?;
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Index::Ann {
                    embeddings,
                    limit,
                    tx,
                } => {
                    _ = tx.send(if embeddings.0.len() != dimensions.0 {
                        Err(anyhow!(
                            "index ann query: wrong embeddings dimensions: {} != {dimensions}",
                            embeddings.0.len()
                        ))
                    } else if limit.0 < 1 {
                        Err(anyhow!("index ann query: wrong limit value {limit}"))
                    } else if let Ok(results) = idx.search(&embeddings.0, limit.0) {
                        Ok(results
                            .keys
                            .into_iter()
                            .map(|key| key.into())
                            .zip(results.distances.into_iter().map(|value| value.into()))
                            .collect())
                    } else {
                        Err(anyhow!("index ann query: search failed"))
                    });
                }
                Index::Stop => {
                    rx.close();
                }
            }
        }
    });
    Ok((tx, task))
}
