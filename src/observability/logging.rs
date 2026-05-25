use tracing_subscriber::EnvFilter;

pub(crate) fn init_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_from_env("MQTT_RS_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("mqtt_rs=info,rs_netty=info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .try_init()?;

    Ok(())
}
