pub(crate) mod logging;
pub(crate) mod metrics;

pub(crate) fn init_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logging::init_from_env()?;
    metrics::init_from_env()?;
    Ok(())
}
