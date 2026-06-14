//! [`Built<T>`] bundles a constructed subject (a disk manager, an engine, …)
//! with any backing resources it needs to outlive — a `TempDir` for file- or
//! LSM-backed configs. The maker returns it; the test/bench holds it for the
//! duration so the temp directory isn't dropped out from under the subject.

use tempfile::TempDir;

pub struct Built<T> {
    item: T,
    _dir: Option<TempDir>,
}

impl<T> Built<T> {
    /// A subject with no backing directory (pure in-memory).
    pub fn new(item: T) -> Self {
        Self { item, _dir: None }
    }

    /// A subject whose `dir` must stay alive as long as the subject does.
    pub fn with_dir(item: T, dir: TempDir) -> Self {
        Self {
            item,
            _dir: Some(dir),
        }
    }

    pub fn get(&self) -> &T {
        &self.item
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.item
    }
}
