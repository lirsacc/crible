# Crible

:construction: WIP :construction:

This is a prototype for a very simple search index system exposed over HTTP.

- The goal is to support logically combining filters over a large~ish space of possible values where most filter only apply to a small subset of values.
- This is not full text search. It focuses on the faceted/properties part of search where the possible values to search by are known ahead of time. Values are integers and properties are strings.
- The operating model is supposed to be a single instance where the full index can be held into memory, which precludes use cases with larger datasets.
- The core of this solution is to use [Roaring Bitmaps](https://roaringbitmap.org) (through [`croaring-rs`](https://github.com/saulius/croaring-rs)) to efficiently store a bitmap per indexed property. This should ensure we can manage a pretty large index before the single machine limitation breaks down.
- It pretty similar to something like [Pilosa](https://www.pilosa.com) although targetting a different scale of data.

## Use cases

This is mostly a quick prototype but the idea is that it should fit in the narrow path where using your regular databases won't cut it but setting up amd managing a full-on search system is too heavy handed operationally + you don't need full text search.

Coming from a state where you already use a proper database to back your application (e.g. Django), proper indexing across multiple tables or a single search table with carefully tuned indices to match the expected querying patterns is likely to carry you very far and be the best solution for most people.

A single index like this becomes useful when:

- the core schema becomes too spread out and the join overhead creeps up
- the indexed space is too large to fit comfortably and consistently in memory
- you need search over data coming from multiple sources
