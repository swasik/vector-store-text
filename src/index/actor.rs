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
        article_id: Key,
        article_content: String,
        tx: oneshot::Sender<()>,
    },
    Remove {
        article_id: Key,
    },
    Search {
        text: String,
        limit: Limit,
        tx: oneshot::Sender<SearchR>,
    },
}

pub(crate) trait IndexExt {
    async fn add(&self, article_id: Key, article_content: String);
    async fn remove(&self, article_id: Key);
    async fn search(&self, text: String, limit: Limit) -> SearchR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add(&self, article_id: Key, article_content: String) {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Add {
            article_id,
            article_content,
            tx,
        })
        .await
        .expect("internal actor should receive request");
        _ = rx.await;
    }

    async fn remove(&self, article_id: Key) {
        self.send(Index::Remove { article_id })
            .await
            .expect("internal actor should receive request");
    }

    async fn search(&self, text: String, limit: Limit) -> SearchR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Search { text, limit, tx }).await?;
        rx.await?
    }
}
