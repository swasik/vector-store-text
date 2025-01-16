/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: Apache-2.0
 */

use {
    std::future::Future,
    tokio::{sync::mpsc::Sender, task::JoinHandle},
    tracing::warn,
};

pub type ActorHandle = JoinHandle<()>;

pub trait ActorStop {
    fn actor_stop(&self) -> impl Future<Output = ()> + Send;
}

impl<T: MessageStop> ActorStop for Sender<T> {
    async fn actor_stop(&self) {
        self.send(T::message_stop())
            .await
            .unwrap_or_else(|err| warn!("ActorStop: unable to send message: {err}"));
    }
}

pub trait MessageStop: Send {
    fn message_stop() -> Self;
}
