use std::sync::RwLock;

use async_trait::async_trait;

use crible_lib::index::Index;

use super::Backend;

#[derive(Default, Debug)]
pub struct Memory(RwLock<Index>);

#[async_trait]
impl Backend for Memory {
    async fn dump<'a>(&self, index: &Index) -> Result<(), eyre::Report> {
        let mut guard = self.0.write().unwrap();
        *guard = index.clone();
        Ok(())
    }

    async fn load(&self) -> Result<Index, eyre::Report> {
        Ok(self.0.read().unwrap().clone())
    }

    async fn clear(&self) -> Result<(), eyre::Report> {
        self.0.write().unwrap().clear();
        Ok(())
    }
}
