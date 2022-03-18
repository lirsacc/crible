use axum::{
    extract::Extension,
    handler::Handler,
    http::{header::HeaderName, Request},
    response::Response,
    routing::{delete, get, post},
    Router, Server,
};
use color_eyre::Report;
use tokio::sync::RwLock;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::request_id::{MakeRequestId, RequestId};
use tower_http::{
    catch_panic::CatchPanicLayer, classify::ServerErrorsFailureClass,
    trace::TraceLayer, ServiceBuilderExt,
};
use tracing::Instrument;
use tracing::Span;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::backends::Backend;
use crate::index::Index;

mod api;
mod errors;

#[derive(Clone)]
pub struct State {
    read_only: bool,
    backend: Arc<RwLock<Box<dyn Backend>>>,
    index: Arc<RwLock<Index>>,
}

impl State {
    pub fn new(
        index: Index,
        backend: Box<dyn Backend>,
        read_only: bool,
    ) -> Self {
        State {
            backend: Arc::new(RwLock::new(backend)),
            index: Arc::new(RwLock::new(index)),
            read_only,
        }
    }
}

pub async fn run(addr: &SocketAddr, state: State) -> Result<(), Report> {
    let app = Router::new()
        .route("/", get(api::handler_home))
        .route("/query", post(api::handler_query))
        .route("/count", post(api::handler_count))
        .route("/bitmap", post(api::handler_bitmap))
        .route("/stats", get(api::handler_stats))
        .route("/set", post(api::handler_set))
        .route("/set-many", post(api::handler_set_many))
        .route("/unset", post(api::handler_unset))
        .route("/unset-many", post(api::handler_unset_many))
        .route("/bit/:id", get(api::handler_get_bit))
        .route("/bit/:id", delete(api::handler_delete_bit))
        .route("/bit", delete(api::handler_delete_bits))
        .fallback(api::handler_not_found.into_service());

    let svc = ServiceBuilder::new()
        .set_x_request_id(RequestIdBuilder::default())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id = ?request.headers().get(HeaderName::from_static("x-request-id")).map_or("".to_owned(), |hv| hv.to_str().unwrap_or("").to_owned()),
                    )
                })
                .on_request(|_: &Request<_>, _: &Span| tracing::debug!("request received"))
                .on_body_chunk(())
                .on_eos(())
                .on_response(|res: &Response<_>, latency: Duration, _: &Span| {
                    tracing::info!(
                        status = &res.status().as_u16(),
                        duration = &format!("{}μs", latency.as_micros()).as_str(),
                        "response sent"
                    );
                })
                .on_failure(
                    |_err: ServerErrorsFailureClass, latency: Duration, _: &Span| {
                        tracing::error!(
                            duration = &format!("{}μs", latency.as_micros()).as_str(),
                            "response failed"
                        );
                    },
                ),
        )
        .propagate_x_request_id()
        .layer(Extension(state))
        .layer(CatchPanicLayer::new())
        .service(app);

    Server::bind(addr)
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
                    match state.backend
                        .as_ref()
                        .write()
                        .await
                        .load()
                        .instrument(tracing::info_span!("load_index"))
                        .await
                    {
                        Ok(new_index) => {
                            let mut index = state.index.as_ref().write().await;
                            *index = new_index;
                        }
                        Err(e) => {
                            tracing::error!("Failed to load index data: {}", e);
                        }
                    }
                }
                .instrument(tracing::info_span!("refresh_index"))
                .await;
            }
        }
    }
}
