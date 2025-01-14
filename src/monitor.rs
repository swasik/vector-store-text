/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        index::Index,
        ColumnName, ScyllaDbUri, TableName,
    },
    scylla::{Session, SessionBuilder},
    tokio::sync::mpsc::{self, Sender},
};

pub(crate) enum Monitor {
    Stop,
}

impl MessageStop for Monitor {
    fn message_stop() -> Self {
        Monitor::Stop
    }
}

async fn new_session(uri: &ScyllaDbUri) -> anyhow::Result<Session> {
    Ok(SessionBuilder::new()
        .known_node(uri.0.as_str())
        .build()
        .await?)
}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    _table: TableName,
    _col_id: ColumnName,
    _col_emb: ColumnName,
    _index: Sender<Index>,
) -> (Sender<Monitor>, ActorHandle) {
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        _ = new_session(&uri).await;
        while let Some(msg) = rx.recv().await {
            match msg {
                Monitor::Stop => {
                    rx.close();
                }
            }
        }
    });
    (tx, task)
}
