use tracing_subscriber::EnvFilter;

pub(crate) fn init(log: &Option<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = log
        .as_deref()
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::new("mqtt_rs=info,rs_netty=info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .try_init()?;

    Ok(())
}
