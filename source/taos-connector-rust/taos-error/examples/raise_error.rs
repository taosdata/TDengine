use anyhow::Context;
use taos_error::*;

fn main() -> anyhow::Result<()> {
    #[inline(never)]
    fn raise_anyhow_error() -> anyhow::Result<()> {
        Err(anyhow::anyhow!("fake error string"))
    }
    #[inline(never)]
    fn raise_anyhow_error2() -> anyhow::Result<()> {
        raise_anyhow_error().context("Another error")
    }
    #[inline(never)]
    fn raise_anyhow_error3() -> anyhow::Result<()> {
        raise_anyhow_error2().context("Above error")
    }
    #[inline(never)]
    fn raise_error() -> Result<()> {
        Err(Error::new(0x001, "taosc error"))?;
        raise_anyhow_error3()?;
        Ok(())
        // Err(Error::from_anyhow())
    }
    raise_error().map_err(|err| err.context("raise error"))?;
    Ok(())
}
