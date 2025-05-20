/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

mod engine;
pub mod httproutes;
mod httpserver;
mod index;

use index::factory;
use index::factory::IndexFactory;
use std::borrow::Cow;
use std::hash::Hash;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use time::OffsetDateTime;
use tokio::signal;
use utoipa::PartialSchema;
use utoipa::ToSchema;
use utoipa::openapi::KnownFormat;
use utoipa::openapi::ObjectBuilder;
use utoipa::openapi::RefOr;
use utoipa::openapi::Schema;
use utoipa::openapi::SchemaFormat;
use utoipa::openapi::schema::Type;
use uuid::Uuid;

#[derive(Clone, derive_more::From, derive_more::Display)]
pub struct ScyllaDbUri(String);

#[derive(
    Clone,
    Hash,
    Eq,
    PartialEq,
    Debug,
    serde::Deserialize,
    serde::Serialize,
    derive_more::From,
    derive_more::Display,
    derive_more::AsRef,
    utoipa::ToSchema,
)]
/// DB's absolute index/table name (with keyspace) for which index should be build
#[schema(example = "vector_benchmark.vector_items")]
pub struct IndexId(String);

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// A table name of the table with vectors in a db
pub struct Key(String);

#[derive(
    Clone,
    Debug,
    Eq,
    Hash,
    PartialEq,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
    serde::Deserialize,
    utoipa::ToSchema,
)]
pub struct KeyspaceName(String);

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// A table name of the table with vectors in a db
pub struct IndexName(String);

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// A table name of the table with vectors in a db
pub struct TableName(String);

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Name of the column in a db table
pub struct ColumnName(String);

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, derive_more::From, utoipa::ToSchema,
)]
/// Distance beetwen embeddings
pub struct Distance(f32);

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
/// Dimensions of embeddings
pub struct Dimensions(NonZeroUsize);

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
/// Limit number of neighbors per graph node
pub struct Connectivity(usize);

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the recall of indexing
pub struct ExpansionAdd(usize);

#[derive(
    Copy,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
    utoipa::ToSchema,
)]
/// Control the quality of the search
pub struct ExpansionSearch(usize);

#[derive(
    Copy,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Display,
)]
struct ParamM(usize);

#[derive(
    Clone,
    Debug,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::From,
    utoipa::ToSchema,
)]
/// Embedding vector
pub struct Embedding(Vec<f32>);

#[derive(
    Clone,
    serde::Serialize,
    serde::Deserialize,
    derive_more::AsRef,
    derive_more::Display,
    derive_more::From,
)]
/// Limit the number of search result
pub struct Limit(NonZeroUsize);

impl ToSchema for Limit {
    fn name() -> Cow<'static, str> {
        Cow::Borrowed("Limit")
    }
}

impl PartialSchema for Limit {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
            .into()
    }
}

impl Default for Limit {
    fn default() -> Self {
        Self(NonZeroUsize::new(1).unwrap())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, derive_more::From)]
pub struct IndexVersion(Uuid);

#[derive(Clone, Copy, Debug, derive_more::From, derive_more::AsRef)]
pub struct Timestamp(OffsetDateTime);

#[derive(derive_more::From)]
pub struct HttpServerAddr(SocketAddr);

pub async fn run(
    addr: HttpServerAddr,
    index_factory: impl IndexFactory + Send + 'static,
) -> anyhow::Result<(impl Sized, SocketAddr)> {
    let engine_actor = engine::new(index_factory).await?;
    httpserver::new(addr, engine_actor).await
}

pub fn new_index_factory(addr: String) -> anyhow::Result<impl IndexFactory> {
    index::opensearch::new_opensearch(&addr)
}

pub async fn wait_for_shutdown() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
