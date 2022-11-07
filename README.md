# Crible

:construction: WIP :construction:

This is a prototype for a very simple search index system exposed over HTTP.

- The goal is to support logically combining filters over a large~ish space of possible values where most filter only apply to a small subset of values.
- This is not full text search. It focuses on the faceted/properties part of search where the possible values to search by are known ahead of time. Values are integers and properties are strings.
- The operating model is supposed to be one or more instance where the full index can be held into memory and easily replicated, which precludes use cases with larger datasets.
- The core of this solution is to use [Roaring Bitmaps](https://roaringbitmap.org) (through [`croaring-rs`](https://github.com/saulius/croaring-rs)) to efficiently store a bitmap per indexed property. This should ensure we can manage a pretty large index before the single machine limitation breaks down.
- It pretty similar to something like [Pilosa](https://www.pilosa.com) although targetting a different scale of data.

It's also a practical and fairly small project to dive in Rust for production.

## Use cases

This is mostly a quick prototype but the idea is that it should fit in the narrow path where using your regular databases won't cut it but setting up and managing a full-on search system is too heavy handed operationally + you don't need full text search.

Coming from a state where you already use a database to back your application (e.g. Django + Postgres), proper indexing across multiple tables or a single search table with carefully tuned indices to match the expected querying patterns is likely to carry you very far and be the best solution for most people.

A single index like this becomes useful when:

- the core schema becomes too spread out and the join overhead creeps up
- the indexed space is too large to fit comfortably and consistently in memory
- you need search over data coming from multiple sources

This is not useful when you need:

- Full text search
- Querying against unknown properties (e.g. 'where tag like "postgresql-9*"`)

## TODO / Next steps

- [ ] Benchmarking and test 64 bits support and impact.
- [ ] Tests.
- [ ] Better logging + Proper error handling and reporting.
- [ ] Documentation.
- [ ] Evaluate concurrent processing (e.g. compute cardinalities, multiple queries endpoint, etc.).
- [ ] Support partial load, dump and refresh through backends. Ideally this could make things faster / stall less when only a subset of the index changes on every tick.
- [ ] Postgres backend.
- [ ] Evaluate subscription based backends (vs. current poll approach).
- [ ] Evaluate more schema capabilities / field types. E.g. integer type destructured into individual or range encoded bitmaps.
- [ ] Look at using [roaring-rs](https://github.com/RoaringBitmap/roaring-rs) instead the [croaring wrapper](https://github.com/saulius/croaring-rs).
- [ ] Better performance for `not` operations.
- [ ] Better file sync
