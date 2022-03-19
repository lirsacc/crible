use tracing::Instrument;

use std::time::Duration;

use super::State;

pub async fn do_write(state: &State) -> Result<(), eyre::Report> {
    if state.write_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        return Ok(());
    }

    let mut backend = state.backend.as_ref().write().await;
    let index = state.index.as_ref().read().await;
    backend.dump(&index).instrument(tracing::debug_span!("dump_index")).await?;
    state.write_count.store(0, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

pub async fn handle_write(state: &State) -> Result<(), eyre::Report> {
    state.write_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if state.read_only || !state.flush_on_write {
        return Ok(());
    };

    do_write(state).await
}

pub async fn run_refresh_task(state: State, every: Duration) {
    tracing::info!(
        "Starting refresh task. Will update backend every {:?}.",
        every
    );

    let mut interval = tokio::time::interval(every);

    loop {
        tokio::select! {
            _ = crate::utils::shutdown_signal("Backend task") => {
                break;
            },
            _ = interval.tick() => {
                async {
                    match state.backend
                        .as_ref()
                        .read()
                        .await
                        .load()
                        .instrument(tracing::debug_span!("load_index"))
                        .await
                    {
                        Ok(new_index) => {
                            let mut index = state.index.as_ref().write().await;
                            *index = new_index;
                            tracing::info!("Refreshed index.");
                        }
                        Err(e) => {
                            tracing::error!("Failed to load index data: {}", e);
                        }
                    }
                }
                .instrument(tracing::debug_span!("refresh_index"))
                .await;
            }
        }
    }
}

pub async fn run_flush_task(state: State, every: Duration) {
    tracing::info!("Starting flush task. Will flush data every {:?}.", every);

    let mut interval = tokio::time::interval(every);

    loop {
        tokio::select! {
            _ = crate::utils::shutdown_signal("Backend task") => {
                break;
            },
            _ = interval.tick() => {
                async {
                    match do_write(&state).await
                    {
                        Ok(_) => {
                            tracing::info!("Flushed index.");
                        }
                        Err(e) => {
                            tracing::error!("Failed to load index data: {}", e);
                        }
                    }
                }
                .await;
            }
        }
    }
}
