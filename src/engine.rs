/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, ActorStop, MessageStop},
        index::{self, Index},
        monitor,
        supervisor::{Supervisor, SupervisorExt},
        ColumnName, Dimensions, IndexName, ScyllaDbUri, TableName,
    },
    std::{collections::HashMap, future::Future},
    tokio::sync::{mpsc, oneshot},
    tracing::{error, warn},
};

pub(crate) enum Engine {
    GetIndexes {
        tx: oneshot::Sender<Vec<IndexName>>,
    },
    AddIndex {
        index: IndexName,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
    },
    DelIndex {
        index: IndexName,
    },
    GetIndex {
        index: IndexName,
        tx: oneshot::Sender<Option<mpsc::Sender<Index>>>,
    },
    Stop,
}

impl MessageStop for Engine {
    fn message_stop() -> Self {
        Engine::Stop
    }
}

pub(crate) trait EngineExt {
    async fn get_indexes(&self) -> Vec<IndexName>;
    async fn add_index(
        &self,
        index: IndexName,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
    );
    async fn del_index(&self, index: IndexName);
    fn get_index(
        &self,
        index: IndexName,
    ) -> impl Future<Output = Option<mpsc::Sender<Index>>> + Send;
}

impl EngineExt for mpsc::Sender<Engine> {
    async fn get_indexes(&self) -> Vec<IndexName> {
        let (tx, rx) = oneshot::channel();
        if self.send(Engine::GetIndexes { tx }).await.is_ok() {
            rx.await.unwrap_or(Vec::new())
        } else {
            Vec::new()
        }
    }

    async fn add_index(
        &self,
        index: IndexName,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
        dimensions: Dimensions,
    ) {
        self.send(Engine::AddIndex {
            index,
            table,
            col_id,
            col_emb,
            dimensions,
        })
        .await
        .unwrap_or_else(|err| warn!("EngineExt::add_index: unable to send request: {err}"));
    }

    async fn del_index(&self, index: IndexName) {
        self.send(Engine::DelIndex { index })
            .await
            .unwrap_or_else(|err| warn!("EngineExt::del_index: unable to send request: {err}"));
    }

    async fn get_index(&self, index: IndexName) -> Option<mpsc::Sender<Index>> {
        let (tx, rx) = oneshot::channel();
        if self.send(Engine::GetIndex { index, tx }).await.is_ok() {
            rx.await.ok().flatten()
        } else {
            None
        }
    }
}

pub(crate) fn new(
    uri: ScyllaDbUri,
    supervisor_actor: mpsc::Sender<Supervisor>,
) -> (mpsc::Sender<Engine>, ActorHandle) {
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        let mut indexes = HashMap::new();
        let mut monitors = HashMap::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                Engine::GetIndexes { tx } => {
                    tx.send(indexes.keys().cloned().collect())
                        .unwrap_or_else(|_| {
                            warn!("engine::new: Engine::GetIndexes: unable to send response")
                        });
                }
                Engine::AddIndex {
                    index: index_name,
                    table,
                    col_id,
                    col_emb,
                    dimensions,
                } => {
                    if indexes.contains_key(&index_name) {
                        continue;
                    }
                    if let Ok((index_actor, index_task)) = index::new(dimensions) {
                        if let Ok((monitor_actor, monitor_task)) = monitor::new(
                            uri.clone(),
                            table.clone(),
                            col_id.clone(),
                            col_emb.clone(),
                            index_actor.clone(),
                        )
                        .await
                        {
                            supervisor_actor
                                .attach(index_actor.clone(), index_task)
                                .await;
                            supervisor_actor
                                .attach(monitor_actor.clone(), monitor_task)
                                .await;
                            indexes.insert(index_name.clone(), index_actor);
                            monitors.insert(index_name, monitor_actor);
                        } else {
                            error!("unable to create monitor with uri {uri}, table {table}, col_id {col_id}, col_emb {col_emb}");
                            index_actor.actor_stop().await;
                            index_task.await.unwrap_or_else(|err| warn!("engine::new: Engine::AddIndex: issue while stopping index actor: {err}"));
                        }
                    } else {
                        error!("unable to create index with dimensions {dimensions}");
                    }
                }
                Engine::DelIndex { index: index_name } => {
                    if let Some(index) = indexes.remove(&index_name) {
                        index.actor_stop().await;
                    }
                    if let Some(monitor) = monitors.remove(&index_name) {
                        monitor.actor_stop().await;
                    }
                }
                Engine::GetIndex { index, tx } => {
                    tx.send(indexes.get(&index).cloned()).unwrap_or_else(|_| {
                        warn!("engine::new: Engine::GetIndex: unable to send response")
                    });
                }
                Engine::Stop => {
                    rx.close();
                }
            }
        }
    });
    (tx, task)
}
