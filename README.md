# datalevin-federation

Source adapters and semantic mappings for Datalevin direct analytics.
This project owns every production query-source adapter, including local
relations, PostgreSQL, and optional dataset/Parquet/DataFusion integrations.

- [Companion design and delivery plan](federation.md)
- [Release roadmap and milestone checklist](../datalevin/roadmap.md)
- [Datalevin analytics and virtual relations](../datalevin/analytics.md)
- [Shared schema definitions contract](../datalevin/doc/schema-definitions.md)
- [Public query-source SPI](../datalevin/doc/query-source-spi.md)
- [Optional extensions appendix](../datalevin/doc/query-source-extensions.md)

F0 includes an accepted-metadata [mapping compiler](src/datalevin/federation/mapping.clj),
[canonical fingerprint encoding](src/datalevin/federation/fingerprint.clj), and
[contract fixtures](test/datalevin/federation/contract_test.clj). The initial F1
[local adapter](src/datalevin/federation/local.clj) now runs through embedded
`d/q`, including native joins and multiple fragments. Provider I/O, schema
installation, and an EntityRef runtime codec remain future work.
Build, dependencies, tests, and releases remain separate from Datalevin proper.

With the matching Datalevin checkout at `../datalevin`, run:

```sh
clojure -M:test
```

`local/source` accepts an SPI descriptor and a vector of declared-schema rows.
It copies mutable leaves on construction; `local/replace-rows!` atomically
publishes a new version without changing accepted metadata. Open sessions pin
and charge their complete row version, so replacement cannot change an active
query's view. Passing this caller-owned object grants local read access.
The adapter plans one scan per pattern and leaves predicates to core. It sizes
multi-row batches against the requested row and byte limits, reserves memory
once per batch, then copies owned values from the pinned snapshot. Physical
rows examined include both the sizing and copy passes, plus byte-boundary
lookahead; rows returned are reported separately. Early stop may read ahead
within the current batch. It advertises no optional features and performs no
discovery.

The [execution tests](test/datalevin/federation/local_test.clj) compare eager
results, native joins, aliases under writes, decimal key/predicate behavior,
nominal reference-valued EAV facts, and resource limits. F1 remains in progress;
the core [supported subset and remaining cases](../datalevin/doc/query-source-spi.md#9-source-conformance)
apply to this adapter too.

The [batch tests](test/datalevin/federation/local_batch_test.clj) check exact
row/byte boundaries, mutable ownership after close, empty/missing EAV values,
and reservation failure without consuming rows.

`compile-relation` accepts the main-schema definition map, a relation ID, and a
context containing a compute-context ID and recorded semantic dependencies.
It returns the accepted relation body with digests and its validated SPI source
descriptor. The fixtures include the plan's normalized mapping, composite keys,
and resolved reference targets. Concise mapping normalization and physical-type
conversion remain adapter work; the compiler consumes accepted logical types.

Canonical v1 uses `tag + decimal UTF-8 payload byte length + ':' + payload`.
Tags are `n` nil, `b` boolean (`0`/`1`), `l` long, `i` bigint, `d` decimal
(`scale:unscaled`), `f` double and `r` float (Java hexadecimal string), `t` string,
`k` keyword, `y` symbol, `u` UUID (lowercase text), `z` instant (epoch milliseconds),
`a` bytes, `v` vector, `q` list, `s` set, and `m` map. Keyword/symbol payloads
concatenate encoded namespace (nil when absent) and name. Containers concatenate
encoded children; maps concatenate key/value pairs sorted by unsigned encoded key,
and sets sort unsigned encoded values. Scalar distinctions and sequence order
are retained. The byte/digest fixtures were independently checked with Python's
standard SHA-256 implementation.
