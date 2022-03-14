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
use tracing::Span;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::backends::Backend;
use crate::index::Index;

mod api;
mod errors;

pub async fn run_server(
    port: u16,
    index_handle: Arc<RwLock<Index>>,
    backend_handle: Arc<RwLock<Box<dyn Backend + Send + Sync>>>,
    read_only: bool,
) -> Result<(), Report> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    tracing::info!("Starting server at {:?}; read-only = {}", addr, read_only);

    let mut app = Router::new()
        .route("/", get(api::handler_home))
        .route("/query", post(api::handler_query))
        .route("/stats", get(api::handler_stats))
        .route("/item/:id", get(api::handler_item_get));

    if !read_only {
        app = app
            .route("/clear", post(api::handler_clear))
            .route("/set", post(api::handler_set))
            .route("/unset", post(api::handler_unset))
            .route("/set-many", post(api::handler_set_many))
            .route("/item/:id", delete(api::handler_item_delete));
    } else {
        app = app
            .route("/clear", post(api::handler_read_only))
            .route("/set", post(api::handler_read_only))
            .route("/unset", post(api::handler_read_only))
            .route("/set-many", post(api::handler_read_only))
            .route("/item/:id", delete(api::handler_read_only));
    }

    app = app.fallback(api::handler_404.into_service());

    let svc = ServiceBuilder::new()
        .set_x_request_id(RequestIdBuilder::default())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        version = ?request.version(),
                        status = tracing::field::Empty,
                        duration = tracing::field::Empty,
                        request_id = ?request.headers().get(HeaderName::from_static("x-request-id")).map_or("".to_owned(), |hv| hv.to_str().unwrap_or("").to_owned()),
                    )
                })
                .on_request(())
                .on_body_chunk(())
                .on_eos(())
                .on_response(|res: &Response<_>, latency: Duration, span: &Span| {
                    span.record("status", &res.status().as_u16());
                    span.record("duration", &format!("{}μs", latency.as_micros()).as_str());
                    tracing::info!("response sent")
                })
                .on_failure(
                    |_err: ServerErrorsFailureClass, latency: Duration, span: &Span| {
                        span.record("duration", &format!("{}μs", latency.as_micros()).as_str());
                        tracing::debug!("response failed")
                    },
                ),
        )
        .propagate_x_request_id()
        .layer(Extension(index_handle))
        .layer(Extension(backend_handle))
        .layer(CatchPanicLayer::new())
        .service(app);

    Server::bind(&addr)
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
