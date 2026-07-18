//! The storage *contract* layer. The `StorageEngine` trait family lives
//! in the vocabulary crate so the schema/SQL side can depend on the
//! contract without the implementations; `DiskManager`, `Page`, and the
//! engine impls stay in `idb-storage`.
pub mod engine;
