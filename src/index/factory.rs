/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::IndexId;
use crate::index::actor::Index;
use tokio::sync::mpsc;

pub trait IndexFactory {
    fn create_index(&self, id: IndexId) -> anyhow::Result<mpsc::Sender<Index>>;
}
