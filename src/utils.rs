use std::time::{Duration, Instant};

pub fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let r = f();
    (r, start.elapsed())
}

pub fn timed_cb<T>(f: impl FnOnce() -> T, log: impl FnOnce(Duration)) -> T {
    let (r, elapsed) = timed(f);
    log(elapsed);
    r
}
