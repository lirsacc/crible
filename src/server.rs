use std::sync::Arc;

use log::{error, info};
use tokio::sync::{Mutex, RwLock};
use warp::reply::Reply;
use warp::Filter;

use crate::backend::Backend;
use crate::expressions::parse_expression;
use crate::search_index::{
    FacetStats, GlobalStats, SearchIndex, SearchIndexError,
};

#[derive(Serialize, Debug)]
pub struct StatsResponse<'a> {
    global: &'a GlobalStats,
    facets: Option<&'a Vec<FacetStats>>,
}

#[derive(Deserialize, Debug)]
pub struct StatsQuery {
    full: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct SearchQuery {
    query: String,
}

#[derive(Default)]
pub struct Cache {
    pub stats: Option<(GlobalStats, Vec<FacetStats>)>,
}

pub async fn facets_handler(
    state: Arc<RwLock<SearchIndex>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let index = state.read().await;
    let facets: Vec<&str> =
        index.iter_facets().map(|(k, _)| k.as_ref()).collect();
    Ok(warp::reply::json(&facets))
}

pub async fn facet_stats_handler(
    facet: String,
    state: Arc<RwLock<SearchIndex>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state.read().await.facet_stats(&facet) {
        Ok(stats) => Ok(warp::reply::json(&stats).into_response()),
        Err(SearchIndexError::FacetDoesNotExist(_)) => {
            Ok(warp::http::StatusCode::NOT_FOUND.into_response())
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
    value: u64,
    state: Arc<RwLock<SearchIndex>>,
    cache: Arc<Mutex<Cache>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    state.write().await.add(&facet, value);
    let mut cache_inst = cache.lock().await;
    cache_inst.stats = None;
    Ok("")
}

pub async fn remove_handler(
    facet: String,
    value: u64,
    state: Arc<RwLock<SearchIndex>>,
    cache: Arc<Mutex<Cache>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    match state.write().await.remove(&facet, value) {
        Ok(_) => {
            let mut cache_inst = cache.lock().await;
            cache_inst.stats = None;
            Ok("".into_response())
        }
        Err(SearchIndexError::FacetDoesNotExist(_)) => {
            Ok(warp::http::StatusCode::NOT_FOUND.into_response())
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

pub async fn deindex_handler(
    value: u64,
    state: Arc<RwLock<SearchIndex>>,
    cache: Arc<Mutex<Cache>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    state.write().await.deindex(value);
    let mut cache_inst = cache.lock().await;
    cache_inst.stats = None;
    Ok("")
}

pub async fn drop_facet_handler(
    facet: String,
    state: Arc<RwLock<SearchIndex>>,
    cache: Arc<Mutex<Cache>>,
) -> Result<impl warp::Reply, warp::Rejection> {
    state.write().await.drop_facet(&facet);
    let mut cache_inst = cache.lock().await;
    cache_inst.stats = None;
    Ok("")
}

pub async fn search_handler(
    state: Arc<RwLock<SearchIndex>>,
    query: SearchQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    match parse_expression(&query.query) {
        Ok(expr) => {
            match state.read().await.apply_expression(expr.simplify_via_bdd())
            {
                Ok(tm) => Ok(warp::reply::json(&tm.to_vec()).into_response()),
                Err(err) => Ok(warp::reply::with_status(
                    format!("{}", err),
                    warp::http::StatusCode::BAD_REQUEST,
                )
                .into_response()),
            }
        }
        Err(err) => Ok(warp::reply::with_status(
            format!("{}", err),
            warp::http::StatusCode::BAD_REQUEST,
        )
        .into_response()),
    }
}

pub async fn count_handler(
    state: Arc<RwLock<SearchIndex>>,
    query: SearchQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    match parse_expression(&query.query) {
        Ok(expr) => {
            match state.read().await.apply_expression(expr.simplify_via_bdd())
            {
                Ok(tm) => {
                    Ok(warp::reply::json(&tm.cardinality()).into_response())
                }
                Err(err) => Ok(warp::reply::with_status(
                    format!("{}", err),
                    warp::http::StatusCode::BAD_REQUEST,
                )
                .into_response()),
            }
        }
        Err(err) => Ok(warp::reply::with_status(
            format!("{}", err),
            warp::http::StatusCode::BAD_REQUEST,
        )
        .into_response()),
    }
}

pub async fn stats_handler(
    state: Arc<RwLock<SearchIndex>>,
    cache: Arc<Mutex<Cache>>,
    query: StatsQuery,
) -> Result<impl warp::Reply, warp::Rejection> {
    let index = state.read().await;

    let mut cache_inst = cache.lock().await;

    if cache_inst.stats.is_none() {
        let global = index.stats();
        let facets = index
            .iter_facets()
            .map(|(k, _)| index.facet_stats(k).unwrap())
            .collect();
        cache_inst.stats = Some((global, facets));
    }

    let (global, facets) = cache_inst.stats.as_ref().unwrap();

    Ok(warp::reply::json(&StatsResponse {
        global,
        facets: if query.full.unwrap_or(false) {
            Some(facets)
        } else {
            None
        },
    }))
}

pub async fn run_server(
    addr: std::net::SocketAddr,
    state: Arc<RwLock<SearchIndex>>,
) {
    let cache = Arc::new(Mutex::new(Cache::default()));
    let state_filter = warp::any().map(move || state.clone());
    let cache_filter = warp::any().map(move || cache.clone());

    let are_you_there = warp::get()
        .and(warp::path("are_you_there"))
        .and(warp::path::end())
        .map(|| "Yes.");

    let stats = warp::get()
        .and(warp::path("stats"))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(cache_filter.clone())
        .and(warp::query::<StatsQuery>())
        .and_then(stats_handler);

    let facet_stats = warp::get()
        .and(warp::path!("stats" / String))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and_then(facet_stats_handler);

    let facets = warp::get()
        .and(warp::path("facets"))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and_then(facets_handler);

    let search = warp::get()
        .and(warp::path("search"))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(warp::query::<SearchQuery>())
        .and_then(search_handler);

    let count = warp::get()
        .and(warp::path("count"))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(warp::query::<SearchQuery>())
        .and_then(count_handler);

    let add = warp::post()
        .and(warp::path!("add" / String / u64))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(cache_filter.clone())
        .and_then(add_handler);

    let remove = warp::post()
        .and(warp::path!("remove" / String / u64))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(cache_filter.clone())
        .and_then(remove_handler);

    let deindex = warp::post()
        .and(warp::path!("deindex" / u64))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(cache_filter.clone())
        .and_then(deindex_handler);

    let drop_facet = warp::post()
        .and(warp::path!("drop" / String))
        .and(warp::path::end())
        .and(state_filter.clone())
        .and(cache_filter.clone())
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
pub fn run_writer<B>(index: Arc<RwLock<SearchIndex>>, backend: Arc<B>)
where
    B: Backend + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut last_save = std::time::Instant::now();
        let mut retries: usize = 0;
        let max_retries: usize = 3;
        loop {
            tokio::time::delay_for(std::time::Duration::from_millis(1000))
                .await;
            let (should_save, data) = {
                let index = index.read().await;
                if index.has_changed_since(last_save) {
                    let data: Vec<(String, Vec<u64>)> = index
                        .iter_facets()
                        .map(|(k, f)| (k.clone(), f.to_vec()))
                        .collect();
                    (true, Some(data))
                } else {
                    (false, None)
                }
            };

            if should_save {
                let data = data.unwrap();
                // Why is this clone required here?
                let backend = backend.clone();
                match tokio::task::spawn_blocking(move || {
                    backend.as_ref().save(data, true)
                })
                .await
                {
                    Ok(_) => {
                        last_save = std::time::Instant::now();
                        retries = 0;
                        info!("Saved data");
                    }
                    Err(err) => {
                        // TODO: Should have a better solution than panicking here.
                        if retries >= max_retries {
                            panic!(format!("Failed to save data {} times. Original error: {}", retries, err));
                        } else {
                            error!("Failed to save data (will retry {} times). Original error: {}", max_retries - retries, err);
                            retries += 1;
                        }
                    }
                };
            }
        }
    });
}
