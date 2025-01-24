/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        engine::{Engine, EngineExt},
        index::IndexExt,
        Distance, Embeddings, IndexId, Key, Limit, QueryId, ScyllaDbUri,
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
    tracing::warn,
};

pub(crate) enum MonitorQueries {
    Stop,
}

impl MessageStop for MonitorQueries {
    fn message_stop() -> Self {
        MonitorQueries::Stop
    }
}

pub(crate) async fn new(
    uri: ScyllaDbUri,
    engine: Sender<Engine>,
) -> anyhow::Result<(Sender<MonitorQueries>, ActorHandle)> {
    let db = Arc::new(Db::new(uri).await?);
    let (tx, mut rx) = mpsc::channel(10);
    let task = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_nanos(1));
        while !rx.is_closed() {
            tokio::select! {
                _ = interval.tick() => {
                    process_queries(Arc::clone(&db), &engine).await.unwrap_or_else(|err| {
                        warn!("monitor_queries::new: unable to process_queries: {err}")
                    });
                }
                Some(msg) = rx.recv() => {
                    match msg {
                        MonitorQueries::Stop => rx.close(),
                    }
                }
            }
        }
    });
    Ok((tx, task))
}

struct Db {
    session: Session,
    st_get_queries: PreparedStatement,
    st_answer_query: PreparedStatement,
    st_cancel_query: PreparedStatement,
}

impl Db {
    async fn new(uri: ScyllaDbUri) -> anyhow::Result<Self> {
        let session = SessionBuilder::new()
            .known_node(uri.0.as_str())
            .build()
            .await?;
        Ok(Self {
            st_get_queries: session
                .prepare(Self::GET_QUERIES)
                .await
                .context("GET_QUERIES")?,
            st_answer_query: session
                .prepare(Self::ANSWER_QUERY)
                .await
                .context("ANSWER_QUERY")?,
            st_cancel_query: session
                .prepare(Self::CANCEL_QUERY)
                .await
                .context("CANCEL_QUERY")?,
            session,
        })
    }

    const GET_QUERIES: &str = "
        SELECT id, vector_index_id, top_results_limit, embedding
            FROM vector_benchmark.vector_queries
            WHERE result_computed = FALSE
            ALLOW FILTERING
        ";
    async fn get_queries(
        &self,
    ) -> anyhow::Result<impl Stream<Item = Result<(QueryId, IndexId, Limit, Embeddings), QueryError>>>
    {
        Ok(self
            .session
            .execute_iter(self.st_get_queries.clone(), ())
            .await?
            .rows_stream::<(i32, i32, i32, Vec<f32>)>()?
            .map_ok(|(id, index_id, limit, embeddings)| {
                (
                    id.into(),
                    index_id.into(),
                    (limit as usize).into(),
                    embeddings.into(),
                )
            }))
    }

    const ANSWER_QUERY: &str = "
        UPDATE vector_benchmark.vector_queries
            SET result_computed = True,
                result_keys = ?,
                result_scores = ?
            WHERE id = ?
        ";
    async fn answer_query(
        &self,
        id: QueryId,
        keys: &[Key],
        distances: &[Distance],
    ) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_answer_query, (keys, distances, id))
            .await?;
        Ok(())
    }

    const CANCEL_QUERY: &str = "
        UPDATE vector_benchmark.vector_queries
            SET result_computed = True,
                result_keys = Null,
                result_scores = Null
            WHERE id = ?
        ";
    async fn cancel_query(&self, id: QueryId) -> anyhow::Result<()> {
        self.session
            .execute_unpaged(&self.st_cancel_query, (id,))
            .await?;
        Ok(())
    }
}

async fn process_queries(db: Arc<Db>, engine: &Sender<Engine>) -> anyhow::Result<()> {
    let mut rows = db.get_queries().await?;
    let mut tasks = Vec::new();
    while let Some((id, index_id, limit, embeddings)) = rows.try_next().await? {
        tasks.push(tokio::spawn({
            let db = Arc::clone(&db);
            let engine = engine.clone();
            async move {
            if let Some(idx) = engine.get_index(index_id).await {
                match idx.ann(embeddings, limit).await {
                    Err(err) => {
                        warn!("monitor_queries::process_queries: unable to search ann: {err}");
                        db.cancel_query(id).await.unwrap_or_else(|err| warn!("monitor_queries::process_queries: unable to cancel query because of failed ann: {err}"));
                    }
                    Ok((keys, distances)) => db.answer_query(id, &keys, &distances).await.unwrap_or_else(|err| warn!("monitor_queries::process_queries: unable to answer query: {err}")),
                }
            } else {
                warn!("monitor_queries::process_queries: query {id} with unknown index {index_id}");
                db.cancel_query(id).await.unwrap_or_else(|err| warn!("monitor_queries::process_queries: unable to cancel query because of wrong index: {err}"));
            }
        }}));
    }
    Ok(())
}
