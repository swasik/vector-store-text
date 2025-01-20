/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        engine::{Engine, EngineExt},
        Connectivity, Dimensions, ExpansionAdd, IndexId, ScyllaDbUri,
    },
    anyhow::Context,
    itertools::Itertools,
    scylla::{prepared_statement::PreparedStatement, Session, SessionBuilder},
    std::collections::HashSet,
    tokio::sync::mpsc::{self, Sender},
    tracing::warn,
};

pub(crate) enum MonitorIndexes {
    Stop,
}

impl MessageStop for MonitorIndexes {
    fn message_stop() -> Self {
        MonitorIndexes::Stop
    }
}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    engine: Sender<Engine>,
) -> anyhow::Result<(Sender<MonitorIndexes>, ActorHandle)> {
    let db = Db::new(uri).await?;
    let mut known = HashSet::new();
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        while !rx.is_closed() {
            let indexes = db.not_cancelled_indexes().await.unwrap_or_else(|err| {
                warn!("monitor_indexes: unable to get not cancelled indexes in db: {err}");
                HashSet::new()
            });
            let cancelled = db.cancelled_indexes().await.unwrap_or_else(|err| {
                warn!("monitor_indexes: unable to get cancelled indexes in db: {err}");
                HashSet::new()
            });
            del_indexes(&engine, known.difference(&indexes).chain(cancelled.iter())).await;
            add_indexes(
                &db,
                &engine,
                indexes
                    .difference(&known)
                    .filter(|id| !cancelled.contains(id)),
            )
            .await;
            known = indexes
                .into_iter()
                .filter(|id| !cancelled.contains(id))
                .collect();
            if !rx.is_empty() {
                match rx.recv().await {
                    Some(MonitorIndexes::Stop) => rx.close(),
                    _ => {}
                }
            }
        }
    });
    Ok((tx, task))
}

struct Db {
    session: Session,
    st_not_cancelled_indexes: PreparedStatement,
    st_cancelled_indexes: PreparedStatement,
    st_get_index_params: PreparedStatement,
}

impl Db {
    async fn new(uri: ScyllaDbUri) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
        Ok(Self {
            st_not_cancelled_indexes: session
                .prepare(Self::NOT_CANCELLED_INDEXES)
                .await
                .context("NOT_CANCELLED_INDEXES")?,
            st_cancelled_indexes: session
                .prepare(Self::CANCELLED_INDEXES)
                .await
                .context("CANCELLED_INDEXES")?,
            st_get_index_params: session
                .prepare(Self::GET_INDEX_PARAMS)
                .await
                .context("GET_INDEX_PARAMS")?,
            session,
        })
    }

    const NOT_CANCELLED_INDEXES: &str = "
        SELECT id
        FROM vector_benchmark.vector_indexes
        WHERE canceled = FALSE
        ALLOW FILTERING
        ";
    async fn not_cancelled_indexes(&self) -> anyhow::Result<HashSet<IndexId>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_not_cancelled_indexes, &[])
            .await?
            .into_rows_result()?
            .rows::<(i32,)>()?
            .map_ok(|(id,)| id.into())
            .try_collect()?)
    }

    const CANCELLED_INDEXES: &str = "
        SELECT id
        FROM vector_benchmark.vector_indexes
        WHERE canceled = TRUE
        ALLOW FILTERING
        ";
    async fn cancelled_indexes(&self) -> anyhow::Result<HashSet<IndexId>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_cancelled_indexes, &[])
            .await?
            .into_rows_result()?
            .rows::<(i32,)>()?
            .map_ok(|(id,)| id.into())
            .try_collect()?)
    }

    const GET_INDEX_PARAMS: &str =
        "SELECT dimension, param_m, param_ef_construct FROM vector_benchmark.vector_indexes WHERE id = ?";
    async fn get_index_params(
        &self,
        id: IndexId,
    ) -> anyhow::Result<Option<(Dimensions, Connectivity, ExpansionAdd)>> {
        Ok(self
            .session
            .execute_unpaged(&self.st_get_index_params, (id,))
            .await?
            .into_rows_result()?
            .rows::<(i32, i32, i32)>()?
            .filter_ok(|(dimensions, connectivity, expansion_add)| {
                *dimensions > 0 && *connectivity >= 0 && *expansion_add >= 0
            })
            .map_ok(|(dimensions, connectivity, expansion_add)| {
                (
                    (dimensions as usize).into(),
                    (connectivity as usize).into(),
                    (expansion_add as usize).into(),
                )
            })
            .next()
            .transpose()?)
    }
}

async fn add_indexes(db: &Db, engine: &Sender<Engine>, ids: impl Iterator<Item = &IndexId>) {
    for id in ids {
        let Ok(Some((dimensions, connectivity, expansion_add))) =
            db.get_index_params(*id).await.inspect_err(|err| {
                warn!("monitor_indexes::add_indexes: unable to get index params: {err}")
            })
        else {
            continue;
        };
        engine
            .add_index(
                *id,
                "vector_benchmark.vector_items".to_string().into(),
                "id".to_string().into(),
                "embedding".to_string().into(),
                dimensions,
                connectivity,
                expansion_add,
            )
            .await;
    }
}
async fn del_indexes(engine: &Sender<Engine>, ids: impl Iterator<Item = &IndexId>) {
    for id in ids {
        engine.del_index(*id).await;
    }
}
