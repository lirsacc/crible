use std::sync::Arc;

use croaring::Bitmap;
use log::error;
use warp::reply::{Reply, Response};
use warp::Filter;

use crate::error::CribleError;
use crate::expressions::parse_expression;
use crate::index::{Index, Stats};

// #[derive(Serialize, Debug)]
// pub struct FacetStats<'a> {
//     name: &'a str,
//     stats: &'a Stats,
// }

#[derive(Serialize, Debug)]
pub struct StatsResponse<'a> {
    len: usize,
    stats: &'a Stats,
}

// #[derive(Deserialize, Debug)]
// pub struct StatsQuery {
//     full: Option<bool>,
// }

#[derive(Deserialize, Debug)]
pub struct SearchQuery {
    query: String,
}

// TODO: Error handling.
// TODO: Clean up all the `into_response()` uses.

pub async fn facets_handler(
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.facet_ids() {
        Ok(facets) => Ok(warp::reply::json(&facets).into_response()),
        Err(err) => {
            error!("Failed to load facets, error: {:?}", err);
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn facet_stats_handler(
    facet: String,
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.facet_stats(&facet) {
        Ok(stats) => Ok(warp::reply::json(&stats).into_response()),
        Err(CribleError::FacetDoesNotExist(_)) => {
            Ok(warp::reply::with_status(
                format!("Facet {} does not exist", facet),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
        Err(err) => {
            error!(
                "Failed to load facet stats for facet {}, error: {:?}",
                facet, err
            );
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn add_handler(
    facet: String,
    value: u32,
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.add(&facet, value) {
        Ok(_) => Ok(warp::reply::json(&"").into_response()),
        Err(err) => {
            error!(
                "Failed to add value {} to facet {}, error: {:?}",
                value, facet, err
            );
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn remove_handler(
    facet: String,
    value: u32,
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.add(&facet, value) {
        Ok(_) => Ok(warp::reply::json(&"").into_response()),
        Err(CribleError::FacetDoesNotExist(_)) => {
            Ok(warp::reply::with_status(
                format!("Facet {} does not exist", facet),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
        Err(err) => {
            error!(
                "Failed to remove value {} from facet {}, error: {:?}",
                value, facet, err
            );
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn deindex_handler(
    value: u32,
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.deindex(value) {
        Ok(_) => Ok(warp::reply::json(&"").into_response()),
        Err(err) => {
            error!("Failed to deindex value {}, error: {:?}", value, err);
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub async fn drop_facet_handler(
    facet: String,
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    match index.drop_facet(&facet) {
        Ok(_) => Ok(warp::reply::json(&"").into_response()),
        Err(CribleError::FacetDoesNotExist(_)) => {
            Ok(warp::reply::with_status(
                format!("Facet {} does not exist", facet),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
        Err(err) => {
            error!("Failed to drop facet {}, error: {:?}", facet, err);
            Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

pub fn apply_expr(
    index: Arc<Box<dyn Index + Send + Sync>>,
    query: SearchQuery,
) -> Result<Bitmap, Response> {
    match parse_expression(&query.query) {
        Ok(expr) => match index.apply(expr.simplify_via_bdd()) {
            Ok(x) => Ok(x),
            Err(err) => match err {
                CribleError::FacetDoesNotExist(_) => {
                    Err(warp::reply::with_status(
                        format!("{}", err),
                        warp::http::StatusCode::BAD_REQUEST,
                    )
                    .into_response())
                }
                _ => {
                    error!(
                        "Failed to compute query {:?}, error: {:?}",
                        query, err
                    );
                    Err(warp::http::StatusCode::INTERNAL_SERVER_ERROR
                        .into_response())
                }
            },
        },
        Err(err) => Err(warp::reply::with_status(
            format!("{}", err),
            warp::http::StatusCode::BAD_REQUEST,
        )
        .into_response()),
    }
}

pub async fn search_handler(
    index: Arc<Box<dyn Index + Send + Sync>>,
    query: SearchQuery,
) -> Result<impl Reply, std::convert::Infallible> {
    match apply_expr(index, query) {
        Ok(bm) => Ok(warp::reply::json(&bm.to_vec()).into_response()),
        Err(error_response) => Ok(error_response),
    }
}

pub async fn count_handler(
    index: Arc<Box<dyn Index + Send + Sync>>,
    query: SearchQuery,
) -> Result<impl Reply, std::convert::Infallible> {
    match apply_expr(index, query) {
        Ok(bm) => Ok(warp::reply::json(&bm.cardinality()).into_response()),
        Err(error_response) => Ok(error_response),
    }
}

pub async fn stats_handler(
    index: Arc<Box<dyn Index + Send + Sync>>,
) -> Result<impl Reply, std::convert::Infallible> {
    // TODO: Add back facet stats and maybe cache.
    Ok(warp::reply::json(&StatsResponse {
        len: index.len().unwrap(),
        stats: &index.stats().unwrap(),
    })
    .into_response())
}

pub async fn run_server(
    addr: std::net::SocketAddr,
    index: Arc<Box<dyn Index + Send + Sync>>,
) {
    let index_filter = warp::any().map(move || index.clone());

    let are_you_there = warp::path("are_you_there")
        .and(warp::path::end())
        .and(warp::get())
        .map(|| "Yes.");

    let stats = warp::path("stats")
        .and(warp::path::end())
        .and(warp::get())
        .and(index_filter.clone())
        // .and(warp::query::<StatsQuery>())
        .and_then(stats_handler);

    let facet_stats = warp::path!("stats" / String)
        .and(warp::path::end())
        .and(warp::get())
        .and(index_filter.clone())
        .and_then(facet_stats_handler);

    let facets = warp::path("facets")
        .and(warp::path::end())
        .and(warp::get())
        .and(index_filter.clone())
        .and_then(facets_handler);

    let search = warp::path("search")
        .and(warp::path::end())
        .and(warp::get())
        .and(index_filter.clone())
        .and(warp::query::<SearchQuery>())
        .and_then(search_handler);

    let count = warp::path("count")
        .and(warp::path::end())
        .and(warp::get())
        .and(index_filter.clone())
        .and(warp::query::<SearchQuery>())
        .and_then(count_handler);

    let add = warp::path!("add" / String / u32)
        .and(warp::path::end())
        .and(warp::post())
        .and(index_filter.clone())
        .and_then(add_handler);

    let remove = warp::path!("remove" / String / u32)
        .and(warp::path::end())
        .and(warp::post())
        .and(index_filter.clone())
        .and_then(remove_handler);

    let deindex = warp::path!("deindex" / u32)
        .and(warp::path::end())
        .and(warp::post())
        .and(index_filter.clone())
        .and_then(deindex_handler);

    let drop_facet = warp::path!("drop" / String)
        .and(warp::path::end())
        .and(warp::post())
        .and(index_filter.clone())
        .and_then(drop_facet_handler);

    let api = are_you_there
        .or(facet_stats)
        .or(stats)
        .or(facets)
        .or(search)
        .or(count)
        .or(add)
        .or(remove)
        .or(deindex)
        .or(drop_facet);

    warp::serve(api.with(warp::log("crible"))).run(addr).await;
}

// Save the content of the index using the provided backend. THis is meant to be
// used in a background task while the server is running.
//
// TODO: This should be optimised / made safer.
// TODO: Re-add change detection.
//
// Known issues:
//
// - We don't need to save everything all the time, just the facets that have
//    changed.
// - We are collecting all the data, then writing it which will close to double
//   the memory used on disk writes. Writing during the iteration is likely
//   better in that regard.
// - Saving facets could be done in parallel rather than sequentially.
// - Unsure but maybe saving using an async implementation could be better here?
//    We could also not use tokio::main and create the runtime in its own thread
//    and make this a regular background thread (instead of a tokio task)
// - Panicking on retries is easy but not great.
// - The spawned task could get interupted mid-write which could corrupt the
//    backend. Maybe a transactional~ish process would work better? E.g. write
//    updates in a temp directory and move everything at the end.
pub fn run_writer(
    index: Arc<Box<dyn Index + Send + Sync>>,
    every: std::time::Duration,
    max_retries: usize,
) {
    tokio::spawn(async move {
        let mut retries: usize = 0;
        loop {
            tokio::time::delay_for(every).await;
            let index = index.clone();
            match tokio::task::spawn_blocking(move || index.save()).await {
                Ok(_) => {
                    retries = 0;
                }
                Err(err) => {
                    // TODO: Should have a better solution than panicking here.
                    if retries >= max_retries {
                        panic!(format!(
                            "Failed to save data {} times. Original error: {}",
                            retries, err
                        ));
                    } else {
                        error!("Failed to save data (will retry {} times). Original error: {}", max_retries - retries, err);
                        retries += 1;
                    }
                }
            }
        }
    });
}
