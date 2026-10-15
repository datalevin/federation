# datalevin-federation

Source adapters and semantic mappings for Datalevin direct analytics.
This project owns every production query-source adapter, including local
relations, PostgreSQL, and optional dataset/Parquet/DataFusion integrations.
Each adapter lives in its own subproject and releases its own artifact, following
the `dtlvnative` layout. Shared semantic contracts live in `core`.

- [Companion design and delivery plan](federation.md)
- [Release roadmap and milestone checklist](../datalevin/roadmap.md)
- [Datalevin analytics and virtual relations](../datalevin/analytics.md)
- [Shared schema definitions contract](../datalevin/doc/schema-definitions.md)
- [Public query-source SPI](../datalevin/doc/query-source-spi.md)
- [Optional extensions appendix](../datalevin/doc/query-source-extensions.md)

F0 includes an accepted-metadata [mapping compiler](core/src/datalevin/federation/mapping.clj),
[canonical fingerprint encoding](core/src/datalevin/federation/fingerprint.clj), and
[contract fixtures](core/test/datalevin/federation/contract_test.clj). The initial F1
[local adapter](local/src/datalevin/federation/local.clj) now runs through embedded
`d/q`, including native joins and multiple fragments. Provider I/O, schema
installation, and an EntityRef runtime codec remain future work.
Build, dependencies, tests, and releases remain separate from Datalevin proper.

## Subprojects and releases

| Directory | Artifact | Contents |
| --- | --- | --- |
| [`core`](core/project.clj) | `org.clojars.huahaiy/datalevin-federation-core` | Mapping compiler and canonical fingerprints. |
| [`local`](local/project.clj) | `org.clojars.huahaiy/datalevin-federation-local` | Declared-schema local relation adapter. |

Each subproject owns its version, dependencies, source, tests, and Leiningen
release configuration. The initial development version is `0.1.0-SNAPSHOT`.
From a subproject directory, `lein jar` builds its JAR and POM, `lein install`
installs it locally, and `lein deploy clojars` publishes that artifact using
`CLOJARS_USERNAME` and `CLOJARS_PASSWORD`. Change only that subproject's version
when preparing its release. There is no aggregate release artifact.

Consumers add the artifacts they need. The local adapter accepts SPI descriptors
directly, so it currently uses only Clojure and Datalevin. Applications
using the mapping compiler also add `datalevin-federation-core`. Existing
`datalevin.federation.*` namespaces are unchanged.
Datalevin has Maven `provided` scope: the application explicitly supplies a
compatible Datalevin runtime rather than receiving one transitively from each
adapter.

Future `postgres`, `dataset`, `parquet`, and `datafusion` adapters follow the same
layout, with artifacts named `org.clojars.huahaiy/datalevin-federation-<adapter>`.
These subprojects are planned. Provider drivers and engine dependencies belong
only in the subproject that uses them. Adapters depend on an explicit compatible
version of `core` when they use its mapping APIs; `core` never depends on adapters.

## Development

The root `deps.edn` is a development workspace using the matching Datalevin
checkout at `../datalevin`; release POMs declare Maven dependencies instead.
Run all tests or select one subproject with an isolated source/test classpath:

```sh
clojure -M:test
clojure -M:test:core
clojure -M:test:local
```

Published Datalevin `1.2.5` supplies the current build dependencies but lacks the
query-source SPI. Development currently requires the matching checkout; the SPI
is planned for Datalevin 1.3. Update the provided dependency version to a release
with the SPI before a stable adapter release.

For Leiningen tests, run `lein test` inside either subproject. The standard `dev`
profile adds the sibling checkout to the test classpath; its resources are
excluded from JARs and POM dependencies. `lein with-profile -dev test` checks
against the declared Maven build and requires a version with the SPI.

## Local adapter

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

The [execution tests](local/test/datalevin/federation/local_test.clj) compare eager
results, native joins, aliases under writes, decimal key/predicate behavior,
nominal reference-valued EAV facts, and resource limits. F1 remains in progress;
the core [supported subset and remaining cases](../datalevin/doc/query-source-spi.md#9-source-conformance)
apply to this adapter too.

The [batch tests](local/test/datalevin/federation/local_batch_test.clj) check exact
row/byte boundaries, mutable ownership after close, empty/missing EAV values,
and reservation failure without consuming rows.

## Shared mapping contracts

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
