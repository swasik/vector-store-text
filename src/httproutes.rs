/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    crate::{
        engine::{Engine, EngineExt},
        index::IndexExt,
        ColumnName, Dimensions, Distance, Embeddings, IndexName, Key, Limit, TableName,
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
        .route("/indexes/{index}", put(put_index).delete(del_index))
        .route("/indexes/{index}/ann", post(post_index_ann))
        .route("/indexes/{index}/status", get(get_index_status))
        .layer(TraceLayer::new_for_http())
        .with_state(engine)
}

async fn get_indexes(State(engine): State<Sender<Engine>>) -> response::Json<Vec<IndexName>> {
    response::Json(engine.get_indexes().await)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PutIndexPayload {
    table: TableName,
    col_id: ColumnName,
    col_emb: ColumnName,
    dimensions: Dimensions,
}

async fn put_index(
    State(engine): State<Sender<Engine>>,
    Path(index): Path<IndexName>,
    extract::Json(payload): extract::Json<PutIndexPayload>,
) {
    engine
        .add_index(
            index,
            payload.table,
            payload.col_id,
            payload.col_emb,
            payload.dimensions,
        )
        .await;
}

async fn del_index(State(engine): State<Sender<Engine>>, Path(index): Path<IndexName>) {
    engine.del_index(index).await;
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
    Path(index_name): Path<IndexName>,
    extract::Json(request): extract::Json<PostIndexAnnRequest>,
) -> Response {
    let Some(index) = engine.get_index(index_name).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    match index.ann(request.embeddings, request.limit).await {
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        Ok(result) => (
            StatusCode::OK,
            response::Json(
                result
                    .into_iter()
                    .map(|(key, distance)| PostIndexAnnResponse { key, distance })
                    .collect::<Vec<_>>(),
            ),
        )
            .into_response(),
    }
}

async fn get_index_status(
    State(engine): State<Sender<Engine>>,
    Path(index_name): Path<IndexName>,
) -> Response {
    let Some(_index) = engine.get_index(index_name).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    (StatusCode::OK, "Not implemented index status").into_response()
}
