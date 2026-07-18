//! Index structures.
//!
//! This module will contain index implementations:
//! - B-tree (primary index structure)

pub mod btree;
pub mod lsm;

use std::sync::Arc;

use idb_core::common::ids::IndexBackend;
use idb_core::storage::engine::IndexEngineOpener;

/// Per-index BPM pool size for file-backed BTree indexes. 128 frames is
/// ~512 KiB per index — enough to keep traversal pages hot without
/// dominating memory in tables with many indexes.
const INDEX_BTREE_POOL_SIZE: usize = 128;

/// The default index-engine factory: BTree on a file-backed BPM, LSM in
/// its own directory, each index under `<parent_dir>/idx_<id>`. Lives
/// with the implementations; the catalog receives it as an injected
/// opener so the schema layer never names concrete engines.
pub fn default_index_opener() -> IndexEngineOpener {
    Arc::new(|backend, id, parent_dir| match backend {
        IndexBackend::BTree => {
            let dir = parent_dir.join(format!("idx_{:08}", id.0));
            std::fs::create_dir_all(&dir)?;
            let dm = crate::storage::FileDiskManager::open_or_create(dir.join("btree.db"))?;
            let bpm = crate::buffer::BufferPoolManager::new(INDEX_BTREE_POOL_SIZE, dm);
            Ok(Arc::new(crate::engines::btree::BTreeEngine::new(bpm)?))
        }
        IndexBackend::Lsm => {
            let dir = parent_dir.join(format!("idx_{:08}", id.0));
            std::fs::create_dir_all(&dir)?;
            Ok(Arc::new(crate::engines::lsm::LsmEngine::new(&dir)?))
        }
    })
}
