/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::Key;
use crate::Limit;
use crate::engine::Engine;
use crate::engine::EngineExt;
use crate::index::IndexExt;
use axum::Router;
use axum::extract;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response;
use axum::response::IntoResponse;
use axum::response::Response;
use tokio::sync::mpsc::Sender;
use tower_http::trace::TraceLayer;
use tracing::debug;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_swagger_ui::SwaggerUi;

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "scylla-vector-store", description = "Scylla Vector Store (API will change after design)")
    )
)]
// TODO: modify HTTP API after design
struct ApiDoc;

pub(crate) fn new(engine: Sender<Engine>) -> Router {
    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(
            OpenApiRouter::new()
                .routes(routes!(get_indexes))
                .routes(routes!(put_index))
                .routes(routes!(post_index_add))
                .routes(routes!(post_index_search))
                .layer(TraceLayer::new_for_http())
                .with_state(engine),
        )
        .split_for_parts();

    router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api))
}

#[utoipa::path(
    get,
    path = "/api/v1/text-search",
    description = "Get list of current indexes",
    responses(
        (status = 200, description = "List of indexes", body = [IndexId])
    )
)]
async fn get_indexes(State(engine): State<Sender<Engine>>) -> response::Json<Vec<IndexId>> {
    response::Json(engine.get_index_ids().await)
}

#[utoipa::path(
    put,
    path = "/api/v1/text-search/{index}",
    description = "Create an index",
    params(
        ("index" = IndexId, Path, description = "Index to search")
    ),
    responses(
        (status = 200, description = "An Index created"),
    )
)]
async fn put_index(State(engine): State<Sender<Engine>>, Path(id): Path<IndexId>) {
    engine.del_index(id.clone()).await;
    engine.add_index(id).await;
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexAddRequest {
    pub key: Key,
    pub text: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/text-search/{index}/add",
    description = "Add an item to the index",
    params(
        ("index" = IndexId, Path, description = "Index to add")
    ),
    request_body = PostIndexSearchRequest,
    responses(
        (status = 200, description = "Add done"),
    )
)]
async fn post_index_add(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
    extract::Json(request): extract::Json<PostIndexAddRequest>,
) -> Response {
    let Some(index) = engine.get_index(id).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };

    index.add(request.key, request.text).await;
    (StatusCode::OK, "").into_response()
}

#[derive(serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct PostIndexSearchRequest {
    pub text: String,
    #[serde(default)]
    pub limit: Limit,
}

#[utoipa::path(
    post,
    path = "/api/v1/text-search/{index}/search",
    description = "Search in the index",
    params(
        ("index" = IndexId, Path, description = "Index to search")
    ),
    request_body = PostIndexSearchRequest,
    responses(
        (status = 200, description = "Search result", body = Vec<String>),
        (status = 404, description = "Index not found")
    )
)]
async fn post_index_search(
    State(engine): State<Sender<Engine>>,
    Path(id): Path<IndexId>,
    extract::Json(request): extract::Json<PostIndexSearchRequest>,
) -> Response {
    let Some(index) = engine.get_index(id).await else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };

    match index.search(request.text, request.limit).await {
        Err(err) => {
            let msg = format!("index.search request error: {err}");
            debug!("post_index_ann: {msg}");
            (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
        }

        Ok(keys) => (StatusCode::OK, serde_json::to_string(&keys).unwrap()).into_response(),
    }
}
