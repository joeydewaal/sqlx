use crate::{context::Context, SqlFlavour};

impl Context {
    pub fn nullable_for_param(&mut self, param: &str) -> anyhow::Result<Option<bool>> {
        match self.flavour {
            SqlFlavour::Postgres => {
                let index: usize = param[1..].parse()?;
                Ok(self.source.params.get(index - 1).copied())
            }
            SqlFlavour::Sqlite => {
                let nullable = self.source.params.get(self.source.next_param_index);
                self.source.next_param_index += 1;
                Ok(nullable.copied())
            }
        }
    }
}
