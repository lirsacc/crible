use std::sync::RwLock;

use crible_lib::index::Index;

use super::Backend;

#[derive(Default, Debug)]
pub struct Memory(RwLock<Index>);

// TODO: Does this even need a copy?

impl Backend for Memory {
    fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report> {
        let mut guard = self.0.write().unwrap();
        *guard = index.clone();
        Ok(())
    }

    fn load(&self) -> Result<Index, eyre::Report> {
        Ok(self.0.read().unwrap().clone())
    }

    fn clear(&self) -> Result<(), eyre::Report> {
        self.0.write().unwrap().clear();
        Ok(())
    }
}
