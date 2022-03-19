use async_trait::async_trait;

use crible_lib::index::Index;

use super::Backend;

#[derive(Default, Debug)]
pub struct Memory(Index);

#[async_trait]
impl Backend for Memory {
    async fn dump<'a>(&mut self, index: &Index) -> Result<(), eyre::Report> {
        self.0 = index.clone();
        Ok(())
    }

    async fn load(&self) -> Result<Index, eyre::Report> {
        Ok(self.0.clone())
    }

    async fn clear(&mut self) -> Result<(), eyre::Report> {
        self.0.clear();
        Ok(())
    }
}
