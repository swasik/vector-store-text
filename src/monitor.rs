/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        index::{Index, IndexExt},
        ColumnName, ScyllaDbUri, TableName,
    },
    futures::TryStreamExt,
    scylla::{Session, SessionBuilder},
    tokio::sync::mpsc::{self, Sender},
    tracing::warn,
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
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    index: Sender<Index>,
) -> anyhow::Result<(Sender<Monitor>, ActorHandle)> {
    let session = new_session(&uri).await?;
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        table_to_index(&session, &table, &col_id, &col_emb, &index)
            .await
            .unwrap_or_else(|err| {
                warn!("monitor::new: unable to copy data from table to index: {err}")
            });
        while let Some(msg) = rx.recv().await {
            match msg {
                Monitor::Stop => {
                    rx.close();
                }
            }
        }
    });
    Ok((tx, task))
}

async fn table_to_index(
    session: &Session,
    table: &TableName,
    col_id: &ColumnName,
    col_emb: &ColumnName,
    index: &Sender<Index>,
) -> anyhow::Result<()> {
    let mut rows = session
        .query_iter(format!("SELECT {col_id}, {col_emb} FROM {table}"), &[])
        .await?
        .rows_stream::<(i64, Vec<f32>)>()?;
    while let Some((key, embeddings)) = rows.try_next().await? {
        if key < 0 {
            continue;
        }
        index.add((key as u64).into(), embeddings.into()).await;
    }
    Ok(())
}
