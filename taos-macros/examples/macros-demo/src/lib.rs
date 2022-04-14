#[cfg(test)]
mod tests {
    use taos::Taos;
    use taos_macros::test;

    #[test]
    fn sync() {}

    #[test]
    async fn async_unit() {}

    #[test]
    async fn async_with_taos(_taos: &Taos) {}

    #[test]
    async fn async_with_taos_db(_taos: &Taos, _database: &str) {}

    #[test(databases = 10)]
    async fn async_with_taos_multi(taos: &Taos, _databases: &[&str]) -> taos::Result<()> {
        let a = taos.databases().await?;
        assert!(a.len() >= 10);
        Ok(())
    }
}
