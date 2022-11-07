use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::HeaderName;
use axum::http::Request;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Router, Server};
use color_eyre::Report;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::request_id::{MakeRequestId, RequestId};
use tower_http::trace::TraceLayer;
use tower_http::ServiceBuilderExt;
use tracing::{Instrument, Span};

use crate::executor::Executor;

mod api;
mod errors;

#[derive(Clone)]
pub struct State(Arc<Executor>);

impl State {
    pub fn new(executor: Executor) -> Self {
        Self(Arc::new(executor))
    }
}

#[inline]
fn x_request_id<T>(request: &Request<T>) -> String {
    request
        .headers()
        .get(HeaderName::from_static("x-request-id"))
        .map_or("".to_owned(), |hv| hv.to_str().unwrap_or("").to_owned())
}

#[inline]
fn format_latency(latency: Duration) -> String {
    format!("{}μs", latency.as_micros())
}

pub async fn run(
    addr: &SocketAddr,
    keep_alive: Option<Duration>,
    state: State,
) -> Result<(), Report> {
    let app = Router::with_state(state)
        .route("/", get(api::handler_home))
        .route("/query", post(api::handler_query))
        .route("/count", post(api::handler_count))
        .route("/stats", post(api::handler_stats))
        .route("/set", post(api::handler_set))
        .route("/set-many", post(api::handler_set_many))
        .route("/unset", post(api::handler_unset))
        .route("/unset-many", post(api::handler_unset_many))
        .route("/get-bit", post(api::handler_get_bit))
        .route("/set-bit", post(api::handler_set_bit))
        .route("/delete-bits", post(api::handler_delete_bits))
        .fallback(api::handler_not_found);

    let svc = ServiceBuilder::new()
        .set_x_request_id(RequestIdBuilder::default())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id = ?x_request_id(request),
                    )
                })
                .on_request(|_: &Request<_>, _: &Span| {
                    tracing::debug!("request received")
                })
                .on_body_chunk(())
                .on_eos(())
                .on_response(
                    |res: &Response<_>, latency: Duration, _: &Span| {
                        tracing::info!(
                            status = &res.status().as_u16(),
                            duration = format_latency(latency).as_str(),
                            "response sent"
                        );
                    },
                )
                .on_failure(
                    |_err: ServerErrorsFailureClass,
                     latency: Duration,
                     _: &Span| {
                        tracing::error!(
                            duration = format_latency(latency).as_str(),
                            "response failed"
                        );
                    },
                ),
        )
        .propagate_x_request_id()
        .layer(CatchPanicLayer::new())
        .service(app);

    Server::bind(addr)
        .tcp_keepalive(keep_alive)
        .serve(Shared::new(svc))
        .with_graceful_shutdown(crate::utils::shutdown_signal("server task"))
        .await
        .unwrap();

    Ok(())
}

#[derive(Clone, Default)]
struct RequestIdBuilder();

impl MakeRequestId for RequestIdBuilder {
    fn make_request_id<B>(&mut self, _: &Request<B>) -> Option<RequestId> {
        Some(RequestId::new(ulid::Ulid::new().to_string().parse().unwrap()))
    }
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
                    match state.0.reload().await
                    {
                        Ok(_) => {
                            tracing::info!("Reloaded index.");
                        }
                        Err(e) => {
                            tracing::error!("Failed to reload index data: {}", e);
                        }
                    }
                }
                .instrument(tracing::info_span!("reload_index"))
                .await;
            }
        }
    }
}
