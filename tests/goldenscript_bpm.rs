//! Goldenscript test runner for the buffer pool manager.
//!
//! Exercises the `BufferPoolManager` API via plain text scripts.
//! Each script file in `tests/goldenscripts/buffer_pool/` becomes a separate test.

use std::error::Error;
use std::fmt::Write as _;

use goldenscript::{Command, Runner};
use tempfile::TempDir;
use test_each_file::test_each_path;

use interchangedb::buffer::replacer::{
    ArcReplacer, ClockReplacer, FifoReplacer, LruKReplacer, LruReplacer, TwoQReplacer,
};
use interchangedb::buffer::swap::SwapMode;
use interchangedb::buffer::BufferPoolManager;
use interchangedb::common::PageId;
use interchangedb::storage::DiskManager;

// Auto-discover all scripts in tests/goldenscripts/buffer_pool/.
test_each_path! { in "tests/goldenscripts/buffer_pool" as buffer_pool => test_goldenscript }

fn test_goldenscript(path: &std::path::Path) {
    let mut runner = BpmRunner::new(10);
    goldenscript::run(&mut runner, path).expect("goldenscript failed");
}

/// Runner that owns a `BufferPoolManager` backed by a temp directory.
struct BpmRunner {
    bpm: BufferPoolManager,
    dir: TempDir,
    pool_size: usize,
}

impl BpmRunner {
    fn new(pool_size: usize) -> Self {
        let dir = TempDir::new().expect("failed to create temp dir");
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).expect("failed to create disk manager");
        let bpm = BufferPoolManager::new(pool_size, dm);
        Self { bpm, dir, pool_size }
    }

    /// Rebuild the BPM with a new pool size, reusing the same directory and DB file.
    fn rebuild(&mut self, pool_size: usize) {
        // Drop old BPM (flushes on drop if needed).
        let path = self.dir.path().join("test.db");

        // Open the existing DB file instead of creating a new one.
        let dm = if path.exists() {
            DiskManager::open(&path).expect("failed to open disk manager")
        } else {
            DiskManager::create(&path).expect("failed to create disk manager")
        };

        self.bpm = BufferPoolManager::new(pool_size, dm);
        self.pool_size = pool_size;
    }

    /// Create an eviction policy by name, using pool_size for capacity.
    fn create_policy(
        &self,
        name: &str,
    ) -> Result<Box<dyn interchangedb::buffer::replacer::EvictionPolicy>, Box<dyn Error>> {
        match name {
            "fifo" => Ok(Box::new(FifoReplacer::new())),
            "clock" => Ok(Box::new(ClockReplacer::new())),
            "lru" => Ok(Box::new(LruReplacer::new(self.pool_size))),
            "lru-k" => Ok(Box::new(LruKReplacer::new(2))),
            "2q" => Ok(Box::new(TwoQReplacer::new(self.pool_size))),
            "arc" => Ok(Box::new(ArcReplacer::new(self.pool_size))),
            _ => Err(format!("unknown policy '{name}'").into()),
        }
    }
}

impl Runner for BpmRunner {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            // set pool_size=N — rebuild the BPM with a new pool size.
            "set" => {
                let mut args = command.consume_args();
                let pool_size: usize = args
                    .lookup_parse("pool_size")?
                    .ok_or("set: pool_size not given")?;
                args.reject_rest()?;
                self.rebuild(pool_size);
            }

            // new_page — allocate a new page.
            "new_page" => {
                let args = command.consume_args();
                args.reject_rest()?;
                let guard = self.bpm.new_page()?;
                writeln!(output, "page_id: {}", guard.page_id().0)?;
            }

            // write PAGE_ID DATA — write string data to a page.
            "write" => {
                let mut args = command.consume_args();
                let page_id: u32 = args.next_pos().ok_or("write: page_id not given")?.parse()?;
                let data = &args.next_pos().ok_or("write: data not given")?.value;
                args.reject_rest()?;

                let mut guard = self.bpm.fetch_page_write(PageId::new(page_id))?;
                let bytes = data.as_bytes();
                // Write data + null terminator.
                let len = bytes.len();
                guard.as_mut_slice()[..len].copy_from_slice(bytes);
                guard.as_mut_slice()[len] = 0;
            }

            // read PAGE_ID — read string data from a page (up to null terminator).
            "read" => {
                let mut args = command.consume_args();
                let page_id: u32 = args.next_pos().ok_or("read: page_id not given")?.parse()?;
                args.reject_rest()?;

                let guard = self.bpm.fetch_page_read(PageId::new(page_id))?;
                let slice = guard.as_slice();
                let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
                let data = String::from_utf8_lossy(&slice[..end]);
                writeln!(output, "data: {data}")?;
            }

            // flush [PAGE_ID] — flush a specific page or all pages.
            "flush" => {
                let mut args = command.consume_args();
                let page_id = args.next_pos();
                args.reject_rest()?;

                match page_id {
                    Some(arg) => {
                        let pid: u32 = arg.parse()?;
                        self.bpm.flush_page(PageId::new(pid))?;
                    }
                    None => {
                        self.bpm.flush_all_pages()?;
                    }
                }
            }

            // delete_page PAGE_ID — delete a page from the pool.
            "delete_page" => {
                let mut args = command.consume_args();
                let page_id: u32 = args
                    .next_pos()
                    .ok_or("delete_page: page_id not given")?
                    .parse()?;
                args.reject_rest()?;
                self.bpm.delete_page(PageId::new(page_id))?;
            }

            // pool_info — print pool metadata.
            "pool_info" => {
                let args = command.consume_args();
                args.reject_rest()?;
                writeln!(output, "pool_size: {}", self.bpm.pool_size())?;
                writeln!(output, "page_count: {}", self.bpm.page_count())?;
                writeln!(output, "free_frames: {}", self.bpm.free_frame_count())?;
                writeln!(output, "policy: {}", self.bpm.get_policy_name())?;
            }

            // stats — print performance statistics.
            "stats" => {
                let args = command.consume_args();
                args.reject_rest()?;
                let snap = self.bpm.stats().snapshot();
                writeln!(output, "cache_hits: {}", snap.cache_hits)?;
                writeln!(output, "cache_misses: {}", snap.cache_misses)?;
                writeln!(output, "evictions: {}", snap.evictions)?;
                writeln!(output, "pages_read: {}", snap.pages_read)?;
                writeln!(output, "pages_written: {}", snap.pages_written)?;
            }

            // swap_policy NAME [mode=cold|warm] — swap the eviction policy.
            "swap_policy" => {
                let mut args = command.consume_args();
                let name = &args.next_pos().ok_or("swap_policy: name not given")?.value;
                let mode_str = args
                    .lookup("mode")
                    .map(|a| a.value.as_str().to_owned())
                    .unwrap_or_else(|| "cold".to_owned());
                args.reject_rest()?;

                let mode = match mode_str.as_str() {
                    "cold" => SwapMode::Cold,
                    "warm" => SwapMode::Warm,
                    _ => return Err(format!("unknown swap mode '{mode_str}'").into()),
                };

                let new_policy = self.create_policy(name)?;
                let result = self.bpm.swap_policy(new_policy, mode);

                writeln!(output, "old_policy: {}", result.old_policy)?;
                writeln!(output, "new_policy: {}", result.new_policy)?;
                writeln!(output, "mode: {mode_str}")?;
                writeln!(output, "frames_registered: {}", result.frames_registered)?;
            }

            name => return Err(format!("unknown command '{name}'").into()),
        }
        Ok(output)
    }
}
