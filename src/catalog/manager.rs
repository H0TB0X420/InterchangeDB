//! `Catalog<E>` — public-facing API for table and index metadata.
//!
//! Owns an `Arc<E: StorageEngine>` and persists schemas via the system
//! tables defined in `system_tables`. Provides:
//!
//! - `Catalog::open(engine)` — bootstrap on fresh data dirs, load on existing.
//! - `create_table` / `drop_table` / `get_table` / `list_tables`.
//! - `create_index` (registers metadata only — Phase 12 wires maintenance).
//!
//! # Concurrency
//!
//! All DDL serializes through `ddl_mutex`. Reads (`get_table`, `list_tables`)
//! go through the in-memory `tables` cache via `RwLock` for read concurrency.
//! `next_user_table_id` and `next_index_id` are `AtomicU32` for fast
//! pre-allocation without holding the mutex while encoding system rows.
//!
//! # No transactional DDL in V2
//!
//! `create_table` is committed when the system rows are written. A crash
//! between writing `__sys_tables` and `__sys_columns` would leave a partial
//! table — this is acceptable for V2 because DDL runs outside user
//! transactions and is not expected to interleave with concurrent crash.
//! Phase 11+ may revisit if this becomes a real concern.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::buffer::BufferPoolManager;
use crate::catalog::ids::{FIRST_SYSTEM_TABLE_ID, FIRST_USER_TABLE_ID, SYS_INDEXES_ID};
use crate::catalog::system_tables::{
    initialize_system_tables, is_initialized, load_system_tables, read_all_index_rows,
    sys_indexes_schema,
};
use crate::catalog::{IndexBackend, IndexDef, IndexId, Schema, TableId};
use crate::common::{ConstraintRule, Error, Result};
use crate::engines::btree::BTreeEngine;
use crate::engines::lsm::LsmEngine;
use crate::layout::{DataLayout, LayoutCtx, RowLayout};
use crate::storage::{FileDiskManager, StorageEngine};
use crate::types::Value;

/// Per-index BPM pool size for file-backed BTree indexes. 128 frames is
/// ~512 KiB per index — enough to keep traversal pages hot without
/// dominating memory in tables with many indexes.
const INDEX_BTREE_POOL_SIZE: usize = 128;

/// Public-facing catalog handle.
pub struct Catalog<E: StorageEngine> {
    engine: Arc<E>,
    /// All DDL acquires this first, then performs engine writes and cache
    /// updates. Reads never touch it.
    ddl_mutex: Mutex<()>,
    /// In-memory schema cache. Read concurrency via RwLock; writes only
    /// inside `ddl_mutex`.
    tables: RwLock<HashMap<String, Arc<Schema>>>,
    /// Next user table id. Pre-incremented past existing ids on `open`.
    next_user_table_id: AtomicU32,
    /// Next index id. Pre-incremented past existing ids on `open`.
    next_index_id: AtomicU32,
    /// Per-index storage engine handles, keyed by IndexId. Each index can
    /// use a different backend (BTreeEngine, LsmEngine, future impls)
    /// independently of the table's PK engine.
    ///
    /// `Arc<dyn StorageEngine>`: Arc lets ops share the handle; dyn erases
    /// the backend type so two indexes on one table can be different
    /// engines; `StorageEngine: Send + Sync` already, so the trait object
    /// is too.
    ///
    /// `RwLock` because index creation (DDL) writes; lookups during scans
    /// and inserts read.
    index_engines: RwLock<HashMap<IndexId, Arc<dyn StorageEngine>>>,
    /// Root directory for per-index storage. `Some(dir)` enables
    /// auto-allocation: `create_index` builds the chosen backend under
    /// `<dir>/idx_<id>` and registers it. `None` means tests must
    /// register engines manually via `register_index_engine`.
    index_data_dir: Option<PathBuf>,
}

impl<E: StorageEngine> Catalog<E> {
    /// Open the catalog backed by `engine`. Initializes system tables on a
    /// fresh data dir; loads them otherwise. Performs drift detection.
    ///
    /// Index engines are NOT auto-allocated by this constructor — useful
    /// for tests that want full control via `register_index_engine`. For
    /// production use, `open_persistent` re-instantiates index engines
    /// from `__sys_indexes` on reopen.
    pub fn open(engine: Arc<E>) -> Result<Self> {
        Self::open_inner(engine, None)
    }

    /// Same as `open`, but with auto-allocation of per-index storage
    /// engines rooted at `index_data_dir`. On reopen, every index in
    /// `__sys_indexes` is rebuilt against `<index_data_dir>/idx_<id>`
    /// using its persisted `backend` choice.
    pub fn open_persistent(engine: Arc<E>, index_data_dir: PathBuf) -> Result<Self> {
        Self::open_inner(engine, Some(index_data_dir))
    }

    fn open_inner(engine: Arc<E>, index_data_dir: Option<PathBuf>) -> Result<Self> {
        if !is_initialized(&*engine)? {
            initialize_system_tables(&*engine)?;
        }
        let tables = load_system_tables(&*engine)?;

        let max_user_id = tables
            .values()
            .filter(|s| !s.table_id.is_system())
            .map(|s| s.table_id.0)
            .max()
            .unwrap_or(FIRST_USER_TABLE_ID - 1);

        let max_index_id = read_max_index_id(&*engine)?;

        // P12.3: repopulate per-index engines if we have a data dir.
        // Without a dir, tests will register manually.
        let mut index_engines: HashMap<IndexId, Arc<dyn StorageEngine>> = HashMap::new();
        if let Some(dir) = &index_data_dir {
            std::fs::create_dir_all(dir)?;
            for (id, def) in read_all_index_rows(&*engine)? {
                let idx_engine = build_index_engine(def.backend, id, dir)?;
                index_engines.insert(id, idx_engine);
            }
        }

        Ok(Self {
            engine,
            ddl_mutex: Mutex::new(()),
            tables: RwLock::new(tables),
            next_user_table_id: AtomicU32::new(max_user_id + 1),
            next_index_id: AtomicU32::new(max_index_id + 1),
            index_engines: RwLock::new(index_engines),
            index_data_dir,
        })
    }

    /// Create a new user table. Returns its assigned `TableId`.
    ///
    /// Validates:
    /// - name is not in the reserved `__sys_*` namespace
    /// - name is not already used
    /// - PK is non-empty (Phase 9 requires PK on every table)
    /// - PK columns are non-nullable
    /// - allocated id stays within the user-id range
    pub fn create_table(&self, name: String, mut schema: Schema) -> Result<TableId> {
        if name.starts_with("__sys_") {
            return Err(Error::ConstraintViolation {
                column: format!("<table-name {}>", name),
                rule: ConstraintRule::Arity {
                    // Repurposed for "reserved name" — better than introducing a
                    // dedicated rule for one rare case. The error message is
                    // overridden by the column descriptor.
                    expected: 0,
                    actual: 0,
                },
            });
        }
        if schema.primary_key.is_empty() {
            return Err(Error::ConstraintViolation {
                column: format!("<table {}>", name),
                rule: ConstraintRule::PkNotNull,
            });
        }
        for &pk_idx in &schema.primary_key {
            if schema.columns[pk_idx].nullable {
                return Err(Error::ConstraintViolation {
                    column: schema.columns[pk_idx].name.clone(),
                    rule: ConstraintRule::PkNotNull,
                });
            }
        }

        let _guard = self.ddl_mutex.lock();
        if self.tables.read().contains_key(&name) {
            return Err(Error::TableAlreadyExists { name });
        }

        let id = self.next_user_table_id.fetch_add(1, Ordering::SeqCst);
        if id >= FIRST_SYSTEM_TABLE_ID {
            return Err(Error::StorageCorrupted(format!(
                "user table id space exhausted at {}",
                id
            )));
        }
        let table_id = TableId(id);
        schema.name = name.clone();
        schema.table_id = table_id;

        // Persist into __sys_tables and __sys_columns. (Internal helpers in
        // system_tables that take a Schema and write the corresponding rows.)
        crate::catalog::system_tables::write_user_table_rows(&*self.engine, &schema)?;

        self.tables.write().insert(name, Arc::new(schema));
        Ok(table_id)
    }

    /// Look up a table by name. Errors with `TableNotFound` if absent.
    pub fn get_table(&self, name: &str) -> Result<Arc<Schema>> {
        self.tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound {
                name: name.to_string(),
            })
    }

    /// Drop a table by name. Removes catalog metadata (`__sys_tables` +
    /// `__sys_columns` rows) and the cache entry. Does **not** remove the
    /// table's data rows — that's a sweep we defer (Phase 11+).
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let _guard = self.ddl_mutex.lock();
        let schema = self
            .tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound {
                name: name.to_string(),
            })?;
        if schema.table_id.is_system() {
            return Err(Error::ConstraintViolation {
                column: format!("<table {}>", name),
                rule: ConstraintRule::PkNotNull, // repurposed; system tables are immutable
            });
        }

        crate::catalog::system_tables::delete_user_table_rows(&*self.engine, &schema)?;
        self.tables.write().remove(name);
        Ok(())
    }

    /// Register an index in the catalog. Returns its assigned `IndexId`.
    ///
    /// If the catalog was opened with `open_persistent(dir)`, this also
    /// allocates the per-index storage engine at `<dir>/idx_<id>` using
    /// the backend named in `def.backend`, and registers the resulting
    /// `Arc<dyn StorageEngine>` for future lookups.
    ///
    /// If the catalog was opened with `open()` (no dir), the metadata is
    /// written to `__sys_indexes` but no engine is allocated — tests are
    /// expected to call `register_index_engine` themselves.
    pub fn create_index(&self, def: IndexDef) -> Result<IndexId> {
        let _guard = self.ddl_mutex.lock();

        // Index names are unique across the catalog (matches Postgres).
        let existing = read_index_names(&*self.engine)?;
        if existing.contains(&def.name) {
            return Err(Error::IndexAlreadyExists { name: def.name });
        }

        let id = self.next_index_id.fetch_add(1, Ordering::SeqCst);
        let index_id = IndexId(id);
        crate::catalog::system_tables::write_index_row(&*self.engine, index_id, &def)?;

        if let Some(dir) = &self.index_data_dir {
            let idx_engine = build_index_engine(def.backend, index_id, dir)?;
            self.index_engines.write().insert(index_id, idx_engine);
        }

        Ok(index_id)
    }

    /// All currently-known table names (system + user).
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Engine handle (for `Table` to share when constructed by the catalog).
    pub fn engine(&self) -> &Arc<E> {
        &self.engine
    }

    /// Register a freshly-allocated index's storage engine. P12.3 will call
    /// this from `create_index` after the engine factory builds the chosen
    /// backend.
    pub fn register_index_engine(&self, id: IndexId, engine: Arc<dyn StorageEngine>) {
        self.index_engines.write().insert(id, engine);
    }

    /// Storage engine handle for `id`, or `None` if the index isn't loaded.
    /// Callers use the trait methods (`put`/`get`/`scan`); the concrete
    /// backend is opaque — that's the whole point of per-index choice.
    pub fn index_engine(&self, id: IndexId) -> Option<Arc<dyn StorageEngine>> {
        self.index_engines.read().get(&id).cloned()
    }

    // ---- P14.1: stats access ---------------------------------------------

    /// Persist a table-level statistics row for `table_id`. Called by
    /// `ANALYZE TABLE` (P14.2).
    pub fn put_table_stats(
        &self,
        table_id: TableId,
        stats: &crate::catalog::system_tables::TableStats,
    ) -> Result<()> {
        crate::catalog::system_tables::write_table_stats(&*self.engine, table_id, stats)
    }

    /// Persist a column-level statistics row for `(table_id, column_id)`.
    pub fn put_column_stats(
        &self,
        table_id: TableId,
        column_id: u32,
        stats: &crate::catalog::system_tables::ColumnStats,
    ) -> Result<()> {
        crate::catalog::system_tables::write_column_stats(&*self.engine, table_id, column_id, stats)
    }

    /// Look up the table-level statistics row for `table_id`. `None` if
    /// `ANALYZE` has never run for it. The planner (P14.7) treats
    /// missing stats as "unknown" and falls back to magic-number defaults.
    pub fn get_table_stats(
        &self,
        table_id: TableId,
    ) -> Result<Option<crate::catalog::system_tables::TableStats>> {
        crate::catalog::system_tables::read_table_stats(&*self.engine, table_id)
    }

    /// Look up the column-level statistics row for `(table_id, column_id)`.
    pub fn get_column_stats(
        &self,
        table_id: TableId,
        column_id: u32,
    ) -> Result<Option<crate::catalog::system_tables::ColumnStats>> {
        crate::catalog::system_tables::read_column_stats(&*self.engine, table_id, column_id)
    }

    /// Build the list of `IndexHandle`s for `table_id`, ready to hand to
    /// `Table::with_indexes`. Walks `__sys_indexes`, filters by table,
    /// resolves each index's engine handle, and precomputes the
    /// `(indexed_cols ++ pk_cols)` type list each handle needs for
    /// secondary-key encoding.
    pub fn indexes_for_table(
        &self,
        table_id: TableId,
        table_schema: &Schema,
    ) -> Result<Vec<crate::table::IndexHandle>> {
        let rows = read_all_index_rows(&*self.engine)?;
        let mut out = Vec::new();
        let pk_types: Vec<crate::types::ColumnType> = table_schema
            .primary_key
            .iter()
            .map(|&i| table_schema.columns[i].ty)
            .collect();
        for (id, def) in rows {
            if def.table_id != table_id {
                continue;
            }
            let engine = self.index_engines.read().get(&id).cloned().ok_or_else(|| {
                Error::StorageCorrupted(format!(
                    "index {:?} present in __sys_indexes but no engine registered",
                    id
                ))
            })?;
            let mut key_types: Vec<crate::types::ColumnType> = def
                .columns
                .iter()
                .map(|&i| table_schema.columns[i].ty)
                .collect();
            key_types.extend(pk_types.iter().copied());
            out.push(crate::table::IndexHandle {
                id,
                def,
                engine,
                key_types,
            });
        }
        Ok(out)
    }
}

// ---- internal helpers -----------------------------------------------------

/// Build a fresh `StorageEngine` for an index. The choice of `backend`
/// decides which concrete type. Each index gets its own subdirectory
/// under `parent_dir` keyed by `IndexId` so files don't collide.
fn build_index_engine(
    backend: IndexBackend,
    id: IndexId,
    parent_dir: &Path,
) -> Result<Arc<dyn StorageEngine>> {
    match backend {
        IndexBackend::BTree => {
            // BTree uses a single file backed by a small BPM. Subdir
            // structure: <parent_dir>/idx_<id>/btree.db.
            let dir = parent_dir.join(format!("idx_{:08}", id.0));
            std::fs::create_dir_all(&dir)?;
            let dm = FileDiskManager::open_or_create(dir.join("btree.db"))?;
            let bpm = BufferPoolManager::new(INDEX_BTREE_POOL_SIZE, dm);
            let engine = BTreeEngine::new(bpm)?;
            Ok(Arc::new(engine))
        }
        IndexBackend::Lsm => {
            // LSM uses its own directory (manifest + SSTables live there).
            let dir = parent_dir.join(format!("idx_{:08}", id.0));
            std::fs::create_dir_all(&dir)?;
            let engine = LsmEngine::new(&dir)?;
            Ok(Arc::new(engine))
        }
    }
}

fn read_max_index_id<E: StorageEngine>(engine: &E) -> Result<u32> {
    let sys_indexes = sys_indexes_schema();
    let column_types = sys_indexes.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_INDEXES_ID,
    };
    let mut max_id: u32 = 0;
    for row_result in RowLayout.scan_table(engine, ctx) {
        let (_, values) = row_result?;
        if let Value::Int64(id) = values[0] {
            if id >= 0 && (id as u32) > max_id {
                max_id = id as u32;
            }
        }
    }
    Ok(max_id)
}

fn read_index_names<E: StorageEngine>(engine: &E) -> Result<Vec<String>> {
    let sys_indexes = sys_indexes_schema();
    let column_types = sys_indexes.column_types();
    let ctx = LayoutCtx {
        column_types: &column_types,
        table_id: SYS_INDEXES_ID,
    };
    let mut names = Vec::new();
    for row_result in RowLayout.scan_table(engine, ctx) {
        let (_, values) = row_result?;
        // Row shape: [index_id, table_id, name, columns_blob, unique].
        if let Value::Varchar(n) = &values[2] {
            names.push(n.clone());
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, Schema};
    use crate::engines::btree::BTreeEngine;
    use crate::storage::FileDiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    fn fresh_catalog() -> (Catalog<BTreeEngine>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Catalog::open(engine).unwrap();
        (catalog, dir)
    }

    fn users_schema() -> Schema {
        Schema {
            name: "".into(),      // create_table will set this
            table_id: TableId(0), // create_table will set this
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "name".into(),
                    ty: ColumnType::Varchar(64),
                    nullable: true,
                    default: None,
                },
            ],
            primary_key: vec![0],
        }
    }

    #[test]
    fn open_initializes_fresh_engine() {
        let (catalog, _dir) = fresh_catalog();
        let names = catalog.list_tables();
        // Five system tables after P14.1: tables, columns, indexes,
        // table_stats, column_stats.
        assert_eq!(names.len(), 5);
        assert!(names.contains(&"__sys_tables".to_string()));
        assert!(names.contains(&"__sys_columns".to_string()));
        assert!(names.contains(&"__sys_indexes".to_string()));
        assert!(names.contains(&"__sys_table_stats".to_string()));
        assert!(names.contains(&"__sys_column_stats".to_string()));
    }

    #[test]
    fn create_table_assigns_id_and_caches_schema() {
        let (catalog, _dir) = fresh_catalog();
        let id = catalog
            .create_table("users".into(), users_schema())
            .unwrap();
        assert_eq!(id, TableId(FIRST_USER_TABLE_ID));

        let schema = catalog.get_table("users").unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.table_id, id);
    }

    #[test]
    fn create_table_rejects_reserved_name() {
        let (catalog, _dir) = fresh_catalog();
        let err = catalog
            .create_table("__sys_evil".into(), users_schema())
            .unwrap_err();
        assert!(matches!(err, Error::ConstraintViolation { .. }));
    }

    #[test]
    fn create_table_rejects_empty_pk() {
        let (catalog, _dir) = fresh_catalog();
        let mut s = users_schema();
        s.primary_key.clear();
        let err = catalog.create_table("nopk".into(), s).unwrap_err();
        assert!(matches!(err, Error::ConstraintViolation { .. }));
    }

    #[test]
    fn create_table_rejects_nullable_pk_column() {
        let (catalog, _dir) = fresh_catalog();
        let mut s = users_schema();
        s.columns[0].nullable = true; // PK column made nullable
        let err = catalog.create_table("badpk".into(), s).unwrap_err();
        assert!(matches!(err, Error::ConstraintViolation { .. }));
    }

    #[test]
    fn create_table_duplicate_name_errors() {
        let (catalog, _dir) = fresh_catalog();
        catalog
            .create_table("users".into(), users_schema())
            .unwrap();
        let err = catalog
            .create_table("users".into(), users_schema())
            .unwrap_err();
        assert!(matches!(err, Error::TableAlreadyExists { .. }));
    }

    #[test]
    fn get_table_unknown_errors() {
        let (catalog, _dir) = fresh_catalog();
        let err = catalog.get_table("ghost").unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }));
    }

    #[test]
    fn drop_table_removes_from_cache() {
        let (catalog, _dir) = fresh_catalog();
        catalog
            .create_table("users".into(), users_schema())
            .unwrap();
        catalog.drop_table("users").unwrap();
        let err = catalog.get_table("users").unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }));
    }

    #[test]
    fn drop_table_unknown_errors() {
        let (catalog, _dir) = fresh_catalog();
        let err = catalog.drop_table("ghost").unwrap_err();
        assert!(matches!(err, Error::TableNotFound { .. }));
    }

    #[test]
    fn drop_table_rejects_system_tables() {
        let (catalog, _dir) = fresh_catalog();
        let err = catalog.drop_table("__sys_tables").unwrap_err();
        assert!(matches!(err, Error::ConstraintViolation { .. }));
    }

    #[test]
    fn create_index_assigns_id_and_persists() {
        let (catalog, _dir) = fresh_catalog();
        let table_id = catalog
            .create_table("users".into(), users_schema())
            .unwrap();
        let id = catalog
            .create_index(IndexDef {
                name: "users_by_name".into(),
                table_id,
                columns: vec![1],
                unique: false,
                backend: crate::catalog::IndexBackend::BTree,
            })
            .unwrap();
        assert_eq!(id, IndexId(1));
    }

    #[test]
    fn create_index_duplicate_name_errors() {
        let (catalog, _dir) = fresh_catalog();
        let table_id = catalog
            .create_table("users".into(), users_schema())
            .unwrap();
        catalog
            .create_index(IndexDef {
                name: "ix".into(),
                table_id,
                columns: vec![0],
                unique: false,
                backend: crate::catalog::IndexBackend::BTree,
            })
            .unwrap();
        let err = catalog
            .create_index(IndexDef {
                name: "ix".into(),
                table_id,
                columns: vec![1],
                unique: false,
                backend: crate::catalog::IndexBackend::BTree,
            })
            .unwrap_err();
        assert!(matches!(err, Error::IndexAlreadyExists { .. }));
    }

    /// Reopening the catalog on the same engine recovers all user tables
    /// and the next-id counters advance past existing ids.
    #[test]
    fn reopen_recovers_user_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let id1;
        {
            let dm = FileDiskManager::create(&path).unwrap();
            let bpm = BufferPoolManager::new(256, dm);
            let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
            let catalog = Catalog::open(engine).unwrap();
            id1 = catalog
                .create_table("users".into(), users_schema())
                .unwrap();
            catalog
                .create_table("orders".into(), users_schema())
                .unwrap();
            // BTreeEngine doesn't auto-flush on drop; explicit flush
            // ensures all dirty pages reach disk before reopen.
            catalog.engine().flush().unwrap();
        }
        // Reopen — fresh engine instance, same path.
        let dm = FileDiskManager::open(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Catalog::open(engine).unwrap();

        let users = catalog.get_table("users").unwrap();
        assert_eq!(users.table_id, id1);
        assert_eq!(users.name, "users");
        let orders = catalog.get_table("orders").unwrap();
        assert_eq!(orders.table_id, TableId(id1.0 + 1));

        // Counter must start past existing ids: next create gets id1+2.
        let id3 = catalog
            .create_table("third".into(), users_schema())
            .unwrap();
        assert_eq!(id3, TableId(id1.0 + 2));
    }
}
