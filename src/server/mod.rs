use axum::{
    extract::Extension,
    handler::Handler,
    http::{header::HeaderName, Request},
    response::Response,
    routing::{delete, get, post},
    Router, Server,
};
use color_eyre::Report;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::request_id::{MakeRequestId, RequestId};
use tower_http::{
    catch_panic::CatchPanicLayer, classify::ServerErrorsFailureClass,
    trace::TraceLayer, ServiceBuilderExt,
};
use tracing::Span;

use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc, RwLock};
use std::time::Duration;

use crible_lib::index::Index;

use crate::backends::Backend;

mod api;
mod errors;
mod readwrite;

pub use readwrite::{run_flush_task, run_refresh_task};

#[derive(Clone)]
pub struct State {
    read_only: bool,
    flush_on_write: bool,
    backend: Arc<Box<dyn Backend>>,
    index: Arc<RwLock<Index>>,
    write_count: Arc<AtomicU64>,
}

impl State {
    pub fn new(
        index: Index,
        backend: Box<dyn Backend>,
        read_only: bool,
        flush_on_write: bool,
    ) -> Self {
        Self {
            backend: Arc::new(backend),
            index: Arc::new(RwLock::new(index)),
            read_only,
            flush_on_write,
            write_count: Arc::new(AtomicU64::new(0)),
        }
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
        .route("/bit/:id", post(api::handler_set_bit))
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
