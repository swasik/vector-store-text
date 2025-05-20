/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Key;
use crate::Limit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type SearchR = anyhow::Result<Vec<Key>>;

pub enum Index {
    Add {
        key: Key,
        text: String,
    },
    Remove {
        key: Key,
    },
    Search {
        text: String,
        limit: Limit,
        tx: oneshot::Sender<SearchR>,
    },
}

pub(crate) trait IndexExt {
    async fn add(&self, key: Key, text: String);
    async fn remove(&self, key: Key);
    async fn search(&self, text: String, limit: Limit) -> SearchR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add(&self, key: Key, text: String) {
        self.send(Index::Add { key, text })
            .await
            .expect("internal actor should receive request");
    }

    async fn remove(&self, key: Key) {
        self.send(Index::Remove { key })
            .await
            .expect("internal actor should receive request");
    }

    async fn search(&self, text: String, limit: Limit) -> SearchR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Search { text, limit, tx }).await?;
        rx.await?
    }
}
