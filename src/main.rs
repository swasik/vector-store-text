/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

mod actor;
mod engine;
mod httproutes;
mod httpserver;
mod index;
mod monitor;
mod supervisor;

use {
    crate::{actor::ActorStop, supervisor::SupervisorExt},
    anyhow::anyhow,
    std::net::{SocketAddr, ToSocketAddrs},
    tokio::signal,
    tracing_subscriber::{fmt, prelude::*, EnvFilter},
};

#[derive(Clone, derive_more::From)]
pub(crate) struct ScyllaDbUri(String);

#[derive(Clone, derive_more::From, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct IndexName(String);

#[derive(Clone, derive_more::From, serde::Serialize, serde::Deserialize)]
struct TableName(String);

#[derive(Clone, derive_more::From, serde::Serialize, serde::Deserialize)]
struct ColumnName(String);

#[derive(Clone, serde::Serialize, serde::Deserialize, derive_more::From)]
struct Key(u64);

#[derive(Clone, serde::Serialize, serde::Deserialize, derive_more::From)]
struct Distance(f32);

#[derive(Copy, Clone, serde::Serialize, serde::Deserialize, derive_more::Display)]
struct Dimensions(usize);

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Embeddings(Vec<f32>);

#[derive(Clone, serde::Serialize, serde::Deserialize, derive_more::Display)]
struct Limit(usize);

#[derive(derive_more::From)]
struct HttpServerAddr(SocketAddr);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = dotenvy::dotenv();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?)
        .with(fmt::layer().with_target(false))
        .init();
    let scylla_usearch_addr = dotenvy::var("SCYLLA_USEARCH_URI")
        .unwrap_or("127.0.0.1:6080".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(anyhow!(
            "Unable to parse SCYLLA_USEARCH_URI env (host:port)"
        ))?
        .into();
    let scylladb_uri = dotenvy::var("SCYLLADB_URI")
        .unwrap_or("127.0.0.1:9042".to_string())
        .into();
    let (supervisor_actor, supervisor_handle) = supervisor::new();
    let (engine_actor, engine_task) = engine::new(scylladb_uri, supervisor_actor.clone());
    supervisor_actor
        .attach(engine_actor.clone(), engine_task)
        .await;
    let (server_actor, server_task) = httpserver::new(scylla_usearch_addr, engine_actor).await?;
    supervisor_actor.attach(server_actor, server_task).await;
    wait_for_shutdown().await;
    supervisor_actor.actor_stop().await;
    supervisor_handle.await?;
    Ok(())
}

async fn wait_for_shutdown() {
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
