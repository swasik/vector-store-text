/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        index::{Index, IndexExt},
        ColumnName, Embeddings, Key, ScyllaDbUri, TableName,
    },
    anyhow::Context,
    futures::{Stream, TryStreamExt},
    scylla::{
        prepared_statement::PreparedStatement, transport::errors::QueryError, Session,
        SessionBuilder,
    },
    std::sync::Arc,
    tokio::{
        sync::mpsc::{self, Sender},
        time,
    },
    tracing::{debug, info, info_span, warn, Instrument},
};

pub(crate) enum MonitorItems {
    Stop,
}

impl MessageStop for MonitorItems {
    fn message_stop() -> Self {
        MonitorItems::Stop
    }
}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    index: Sender<Index>,
) -> anyhow::Result<(Sender<MonitorItems>, ActorHandle)> {
    let db = Arc::new(Db::new(uri, table, col_id, col_emb).await?);
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        info!("resetting items");
        let mut state = State::Reset;
        let mut interval = time::interval(time::Duration::from_nanos(1));
        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    match state {
                        State::Reset => if !reset_items(&db)
                            .await
                            .unwrap_or_else(|err| {
                                warn!("monitor_items: unable to reset items in table: {err}");
                                false
                            }) {
                                info!("copying items");
                                state = State::Copy;
                            },
                        State::Copy => table_to_index(&db, &index).await.unwrap_or_else(|err| {
                            warn!("monitor_items: unable to copy data from table to index: {err}")
                        }),
                    }
                }
                Some(msg) = rx.recv() => {
                    match msg {
                        MonitorItems::Stop => rx.close(),
                    }
                }
            }
        }
    }.instrument(info_span!("monitor items")));
    Ok((tx, task))
}

enum State {
    Reset,
    Copy,
}

struct Db {
    session: Session,
    st_get_processed_ids: PreparedStatement,
    st_get_items: PreparedStatement,
    st_reset_items: PreparedStatement,
    st_update_item: PreparedStatement,
}

impl Db {
    async fn new(
        uri: ScyllaDbUri,
        table: TableName,
        col_id: ColumnName,
        col_emb: ColumnName,
    ) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
        Ok(Self {
            st_get_processed_ids: session
                .prepare(Self::get_processed_ids_query(&table, &col_id))
                .await
                .context("get_processed_ids_query")?,
            st_get_items: session
                .prepare(Self::get_items_query(&table, &col_id, &col_emb))
                .await
                .context("get_items_query")?,
            st_reset_items: session
                .prepare(Self::reset_items_query(&table))
                .await
                .context("reset_items_query")?,
            st_update_item: session
                .prepare(Self::update_item_query(&table))
                .await
                .context("update_item_query")?,
            session,
        })
    }

    fn get_processed_ids_query(table: &TableName, col_id: &ColumnName) -> String {
        format!(
            "
            SELECT {col_id}
            FROM {table}
            WHERE processed = TRUE
            LIMIT 1000
            "
        )
    }
    async fn get_processed_ids(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<Key, QueryError>>> {
        Ok(self
            .session
            .execute_iter(self.st_get_processed_ids.clone(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map_ok(|(key,)| (key as u64).into()))
    }

    fn get_items_query(table: &TableName, col_id: &ColumnName, col_emb: &ColumnName) -> String {
        format!(
            "
            SELECT {col_id}, {col_emb}
            FROM {table}
            WHERE processed = FALSE
            LIMIT 1000
            "
        )
    }
    async fn get_items(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<(Key, Embeddings), QueryError>>> {
        Ok(self
            .session
            .execute_iter(self.st_get_items.clone(), ())
            .await?
            .rows_stream::<(i64, Vec<f32>)>()?
            .map_ok(|(key, embeddings)| ((key as u64).into(), embeddings.into())))
    }

    fn reset_items_query(table: &TableName) -> String {
        format!(
            "
            UPDATE {table}
                SET processed = False
                WHERE id IN ?
            "
        )
    }
    async fn reset_items(&self, keys: &[Key]) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_reset_items, (keys,))
            .await?;
        Ok(())
    }

    fn update_item_query(table: &TableName) -> String {
        format!(
            "
            UPDATE {table}
                SET processed = True
                WHERE id = ?
            "
        )
    }
    async fn update_item(&self, key: Key) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_update_item, (key,))
            .await?;
        Ok(())
    }
}

async fn reset_items(db: &Arc<Db>) -> anyhow::Result<bool> {
    let mut keys_chunks = db.get_processed_ids().await?.try_chunks(100);
    let mut resetting = Vec::new();
    while let Some(keys) = keys_chunks.try_next().await? {
        let db = Arc::clone(db);
        resetting.push(tokio::spawn(async move {
            db.reset_items(&keys).await.map(|_| keys.len())
        }));
    }
    let mut count = 0;
    for processed in resetting.into_iter() {
        count += processed.await??;
    }
    debug!("processed new items: {count}");
    Ok(count > 0)
}

async fn table_to_index(db: &Db, index: &Sender<Index>) -> anyhow::Result<()> {
    let mut rows = db.get_items().await?;
    while let Some((key, embeddings)) = rows.try_next().await? {
        db.update_item(key).await?;
        tokio::spawn({
            let index = index.clone();
            async move {
                index.add(key, embeddings).await;
            }
        });
    }
    Ok(())
}
