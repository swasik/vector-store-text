/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use anyhow::anyhow;
use std::net::ToSocketAddrs;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

// Index creating/querying is CPU bound task, so that vector-store uses rayon ThreadPool for them.
// From the start there was no need (network traffic seems to be not so high) to support more than
// one thread per network IO bound tasks.
#[tokio::main(flavor = "current_thread")]
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

    let index_factory = {
        let addr = dotenvy::var("OPENSEARCH_ADDRESS").unwrap_or("http://localhost".to_string());
        let port = dotenvy::var("OPENSEARCH_PORT").unwrap_or("9200".to_string());
        let addr = format!("{addr}:{port}");
        vector_store_text::new_index_factory(addr)?
    };

    let (_server_actor, addr) = vector_store_text::run(scylla_usearch_addr, index_factory).await?;
    tracing::info!("listening on {addr}");
    vector_store_text::wait_for_shutdown().await;

    Ok(())
}
