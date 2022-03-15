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

pub async fn run(
    port: u16,
    index_handle: Arc<RwLock<Index>>,
    backend_handle: Arc<RwLock<Box<dyn Backend>>>,
    read_only: bool,
) -> Result<(), Report> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let mut app = Router::new()
        .route("/", get(api::handler_home))
        .route("/query", post(api::handler_query))
        .route("/count", post(api::handler_count))
        .route("/bitmap", post(api::handler_bitmap))
        .route("/stats", get(api::handler_stats))
        .route("/bit/:id", get(api::handler_get_bit));

    // TODO: Less verbose way to expose this?
    if read_only {
        app = app
            .route("/set", post(api::handler_read_only))
            .route("/set-many", post(api::handler_read_only))
            .route("/unset", post(api::handler_read_only))
            .route("/unset-many", post(api::handler_read_only))
            .route("/bit/:id", delete(api::handler_read_only))
            .route("/bit", delete(api::handler_read_only));
    } else {
        app = app
            .route("/set", post(api::handler_set))
            .route("/set-many", post(api::handler_set_many))
            .route("/unset", post(api::handler_unset))
            .route("/unset-many", post(api::handler_unset_many))
            .route("/bit/:id", delete(api::handler_delete_bit))
            .route("/bit", delete(api::handler_delete_bits));
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
