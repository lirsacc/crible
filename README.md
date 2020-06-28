# Crible

:construction: WIP :construction:

This is a prototype for a very simple search index system exposed over HTTP.

- The goal is to support logically combining filters over a large~ish space of possible values where most filter only apply to a small subset of values.
- This is not full text search. It focuses on the faceted part of search where the possible values to search by are known ahead of time. Values are integers (`u64` exactly) and facets are strings.
- The operating model is supposed to be a single instance where the full index can be held into memory, which precludes use cases with larger datasets.
- The core of this solution is to use [Roaring Bitmaps](https://roaringbitmap.org) (through [`croaring-rs`](https://github.com/saulius/croaring-rs)) to efficiently store a bitmap per indexed dimension and combine them.

## Example

After building with cargo, in one shell run:

```
$  ./target/release/crible import-csv --input test-data-sample.csv --output crible-test.db --incremental
$ ./target/release/crible serve --database crible-test.db
...
2020-06-28T19:32:52.056Z INFO  crible          > Starting server on 127.0.0.1:5000
```

and in another shell, using [httpie](https://httpie.org):

```
$ http -b ':5000/stats'
{
    "facets": null,
    "global": {
        "cardinality": 255,
        "length": 205,
        "maximum": 16744882,
        "minimum": 16
    }
}

$ http -b ':5000/facets'
[
    "foo-544",
    "foo-821",
    ...
    "foo-743",
    "bar-709",
    "bar-764",
    "foo-505",
    "bar-720",
    "foo-68",
    "bar-654",
    "foo-2089",
    "bar-665"
]

$ http -b ':5000/search?query=(foo-505 OR bar-665)'
[
    358136,
    3539791,
    13554397
]

# No output but the server log should indicate that changes were saved.
$ http -b POST ':5000/add/bar-665/3539792'

$ http -b ':5000/search?query=(foo-505 OR bar-665)'
[
    358136,
    3539791,
    3539792,
    13554397
]

# We know from constructing test data that the "foo-X" dimensions are exclusive.
$ http -b ':5000/search?query=(foo-505 AND foo-743)'
[]
```

## Use cases

Not many.

This is mostly a quick prototype but the idea is that it should fit in the narrow path where using your regular databases won't cut it but setting up / managing a proper search system is too heavy handed and you don't need full text search.

Coming from a state where you already use a proper database to back your application (e.g. Django), proper indexing across multiple tables or a single search table with carefully tuned indices to match the expected querying pattern is likely to be the best solution for most people at the size of data this is designed to handle.

Alternatively writing a version of this backed by SQLite would also work well for most use cases. It would support much more expressive and powerfule filtering where this tool is by design limited.

## Next steps

In no particular order.

- [ ] Do some correctness checking + write some tests.

- [ ] Do some benchmarks with various source datasets to gauge memory usage and speed of various operation.

- [ ] Write a SQLite backed version of this (import from csv and HTTP API) to benchmark against.

- [ ] Re-add support for negative clauses in the search expressions. The main issue with the initial solution is that it supports a root `NOT` clause which would force inverting from the full set which is not nicely supported by design. I see 2 solutions:

  - We can construct a bitmap from min to max and invert from this (we'd need to cache the min and max over all bitmaps); accepting that this case is likely to be slow.
  - Update the pest grammar to not support negating the root clause.

- [ ] Proper thouhgt given tothe HTTP API structure and design, including evaluating if JSON is a good fit and how facets are represented.

- [ ] General code hygiene, refactoring, etc. from the current _thrown together prototype_ state.

- [ ] Identify area of optimisations.

- [ ] Remote save backends to backup data off-disk for recovery + easy read-only replication.
