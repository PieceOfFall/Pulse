pub(crate) mod logging;
pub(crate) mod metrics;

use crate::settings::ObservabilityConfig;

pub(crate) fn init(
    config: &ObservabilityConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logging::init(&config.log)?;
    metrics::init(&config.metrics_bind)?;
    Ok(())
}
