/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        engine::{Engine, EngineExt},
        index::IndexExt,
        ColumnName, Connectivity, Dimensions, Distance, Embeddings, ExpansionAdd, IndexId, Key,
        Limit, TableName,
    },
    axum::{
        extract::{self, Path, State},
        http::StatusCode,
        response::{self, IntoResponse, Response},
        routing::{get, post, put},
        Router,
    },
    tokio::sync::mpsc::Sender,
    tower_http::trace::TraceLayer,
};

pub(crate) fn new(engine: Sender<Engine>) -> Router {
    Router::new()
        .route("/", get("OK"))
        .route("/indexes", get(get_indexes))
        .route("/indexes/{id}", put(put_index).delete(del_index))
        .route("/indexes/{id}/ann", post(post_index_ann))
        .route("/indexes/{id}/status", get(get_index_status))
        .layer(TraceLayer::new_for_http())
        .with_state(engine)
}

async fn get_indexes(State(engine): State<Sender<Engine>>) -> response::Json<Vec<IndexId>> {
    response::Json(engine.get_indexes().await)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PutIndexPayload {
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
}

async fn put_index(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
    extract::Json(payload): extract::Json<PutIndexPayload>,
) {
    engine
        .add_index(
            id,
            payload.table,
            payload.col_id,
            payload.col_emb,
            payload.dimensions,
            payload.connectivity,
            payload.expansion_add,
        )
        .await;
}

async fn del_index(State(engine): State<Sender<Engine>>, Path(id): Path<IndexId>) {
    engine.del_index(id).await;
}

#[derive(serde::Deserialize)]
struct PostIndexAnnRequest {
    embeddings: Embeddings,
    #[serde(default = "default_limit")]
    limit: Limit,
}

fn default_limit() -> Limit {
    Limit(1)
}

#[derive(serde::Serialize)]
struct PostIndexAnnResponse {
    key: Key,
    distance: Distance,
}

#[axum::debug_handler]
async fn post_index_ann(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
    extract::Json(request): extract::Json<PostIndexAnnRequest>,
) -> Response {
    let Some(index) = engine.get_index(id).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    match index.ann(request.embeddings, request.limit).await {
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        Ok((keys, distances)) => (
            StatusCode::OK,
            response::Json(
                keys.into_iter()
                    .zip(distances.into_iter())
                    .map(|(key, distance)| PostIndexAnnResponse { key, distance })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
    }
}

async fn get_index_status(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
) -> Response {
    let Some(_index) = engine.get_index(id).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    (StatusCode::OK, "Not implemented index status").into_response()
}
