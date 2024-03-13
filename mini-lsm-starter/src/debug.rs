use crate::lsm_storage::{LsmStorageInner, MiniLsm};

pub const ENABLE_DEBUG: bool = true;
#[macro_export]
macro_rules! mini_lsm_debug {
    () => {
        if $crate::debug::ENABLE_DEBUG {
            println!()
        }
    };
    ($msg: expr, $($arg:tt)*) => {
        if $crate::debug::ENABLE_DEBUG {
            println!(concat!("\x1B[31m", $msg, "\x1B[0m"), $($arg)*)
        }
    }
}

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
