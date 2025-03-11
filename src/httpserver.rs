/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Proprietary
 */

use {
    crate::{
        actor::{ActorHandle, MessageStop},
        engine::Engine,
        httproutes, HttpServerAddr,
    },
    std::sync::Arc,
    tokio::{
        net::TcpListener,
        sync::{
            mpsc::{self, Sender},
            Notify,
        },
    },
};

pub(crate) enum HttpServer {
    Stop,
}

impl MessageStop for HttpServer {
    fn message_stop() -> Self {
        HttpServer::Stop
    }
}

pub(crate) async fn new(
    addr: HttpServerAddr,
    engine: Sender<Engine>,
) -> anyhow::Result<(Sender<HttpServer>, ActorHandle)> {
    let listener = TcpListener::bind(addr.0).await?;
    tracing::info!("listening on {}", listener.local_addr()?);
    let (tx, mut rx) = mpsc::channel(10);
    let notify = Arc::new(Notify::new());
    let task = tokio::spawn({
        let notify = Arc::clone(&notify);
        async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    HttpServer::Stop => {
                        rx.close();
                    }
                }
            }
            notify.notify_one();
        }
    });
    tokio::spawn(async move {
        axum::serve(listener, httproutes::new(engine))
            .with_graceful_shutdown(async move {
                notify.notified().await;
            })
            .await
            .expect("failed to run web server");
    });
    Ok((tx, task))
}
