# datalevin-federation

Source adapters and semantic mappings for Datalevin direct analytics.
The federation interface is mapped entities and attributes queried through
source-qualified EAV patterns. Provider adapters compile accepted mappings to
EAV descriptors. The v2 logical schema exposes accepted attribute types,
cardinality, entity/reference domains, and absence semantics to core; physical
metadata and supporting proofs remain provider-owned. Arbitrary tuple inputs
remain an existing Datalog facility; binding and result tuples are the
execution representation.
The first release joins mapped EAV clauses directly with native facts using
the existing Datalog query/rule engine. Sources contribute access paths, costs,
and capabilities; no parallel federation query engine is planned. Views are
[derived EAV facts](../datalevin/analytics.md#7-views-as-derived-facts), maintained
by extending the same engine with incremental state, and are not required for
federation.

This project owns every production external query-source adapter, including
local relations, APIs, files/exports, PostgreSQL, and optional
dataset/Parquet/DataFusion integrations. The common SPI is being revised around
four source shapes: native Datalevin, SQL, files/exports, and APIs. Native
Datalevin remains built into core with its existing native query path. The
federation planner bridge activates for mixed queries; native-only performance
regressions block integration. API and export workflows lead production adapter
delivery, with PostgreSQL following later.
Delivery proceeds from EAV mappings and runtime identity (F0), to bounded
provider access and native joins (F1–F3), then safe fusion and cost improvements.
Initial planning uses separate entity lookups, attribute enumeration, and
optional value lookups with required bindings, batching limits, and deterministic
structural selection. Advisory estimates may all be unknown. Fusion and cost
ranking have a [separate follow-up gate](../datalevin/roadmap.md#federation-optimization-follow-up).
Repeated estimate rounds and broader search are later improvements to the
existing optimizer. OLTP, OLAP, and hybrid are workload profiles rather
than additional source shapes.
Each adapter lives in its own subproject and releases its own artifact, following
the `dtlvnative` layout. Shared semantic contracts live in `core`.

- [Companion design and delivery plan](federation.md)
- [Release roadmap and milestone checklist](../datalevin/roadmap.md)
- [Datalevin analytics and derived facts](../datalevin/analytics.md)
- [Shared schema definitions contract](../datalevin/doc/schema-definitions.md)
- [Public query-source SPI](../datalevin/doc/query-source-spi.md)
- [Optional extensions appendix](../datalevin/doc/query-source-extensions.md)

F0 includes an accepted-metadata [mapping compiler](core/src/datalevin/federation/mapping.clj),
[canonical fingerprint encoding](core/src/datalevin/federation/fingerprint.clj), and
[contract fixtures](core/test/datalevin/federation/contract_test.clj). The initial F1
[local adapter](local/src/datalevin/federation/local.clj) now runs through embedded
`d/q`, including native joins and multiple fragments. The revised F0 foundation
delivers runtime `EntityRef` identity, its codec, and scalar/reference
cardinality-many mappings, including explicitly snapshot-scoped row keys for
keyless exports; F1 uses them in bounded provider execution and native joins.
Pure preparation for that foundation is implemented in
[`compile-logical-mapping`](core/src/datalevin/federation/mapping.clj) and
[`entity-ref`](core/src/datalevin/federation/entity_ref.clj). The common accepted
representation includes explicit extraction, scalar/reference `:many`, complete
logical schema, combined-mapping guarantees, and scope/completion fingerprints.
`normalize-column-mapping` handles declared columns; other pure provider
normalizers return the same accepted representation. `project-facts` projects
already converted records without provider I/O. The reference codec has bounded
JVM tests, native custom-type storage/reconnect coverage, and independently
verified JVM/Rust [golden fixtures](core/test/fixtures/README.md).
These helpers do not emit SPI descriptors. The active `compile-relation`
compiler still accepts only `:one`, and adapters keep the prototype contract
until the coordinated runtime switch. Provider I/O and schema installation
follow the roadmap's later production-provider gates.
Build, dependencies, tests, and releases remain separate from Datalevin proper.
The [four-shape revision](federation.md#11-common-relation-and-access-contracts)
is planned work: the current executable baseline remains scan-oriented and has
not yet gained parameterized access, per-source consistency requirements, or
scoped one-shot consumption. This SPI is unreleased exploration: migrate core,
adapters, and fixtures together, replacing prototype envelopes without a v1
compatibility path. F1 sign-off requires the revised contract gates.

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

Consumers add the artifacts they need. The earlier v1 local adapter accepts SPI
descriptors directly, so it currently uses only Clojure and Datalevin.
Applications using the mapping compiler also add `datalevin-federation-core`.
The revised local adapter, like every revised federation adapter, will use
accepted EAV mappings and declare a compatible `core` dependency. Existing
`datalevin.federation.*` namespaces are unchanged.
Datalevin has Maven `provided` scope: the application explicitly supplies a
compatible Datalevin runtime rather than receiving one transitively from each
adapter.

Future API, file/export, `postgres`, `dataset`, `parquet`, and `datafusion`
adapters follow the same layout, with artifacts named
`org.clojars.huahaiy/datalevin-federation-<adapter>`.
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

The `:core` suite runs the pure mapping/identity gate without provider execution.
The complete suite also verifies native custom-type storage and reconnects.
To independently verify the experimental identity fixtures with Rust:

```sh
rustc --edition 2021 core/test/fixtures/verify_entity_ref.rs -o /tmp/datalevin-verify-entity-ref
/tmp/datalevin-verify-entity-ref core/test/fixtures/entity-ref-golden.tsv
```

Published Datalevin `1.2.5` supplies the current build dependencies but lacks the
query-source SPI. Development currently requires the matching checkout; the SPI
is planned for Datalevin 1.3. Update the provided dependency version to a release
with the SPI before a stable adapter release.

Each subproject links the sibling checkout as a Leiningen checkout, so `lein
test` and `lein repl` resolve `datalevin` from `../datalevin` ahead of the
declared Maven JAR:

```sh
ln -sfn ../../../datalevin core/checkouts/datalevin
ln -sfn ../../../datalevin local/checkouts/datalevin
```

`checkouts/` is untracked, and the checkout's sources and resources are not
packaged into this project's JAR or POM. Build the checkout once (`lein compile`
or `lein javac` inside `../datalevin`) so its Java classes exist under
`target/classes`; the checkout exposes `src`, `test-src`, resources, and those
compiled classes. Remove the link to fall back to the declared Maven dependency,
which requires a released version with the SPI.

## Local adapter

The current v1 `local/source` accepts an SPI descriptor and a vector of
declared-schema rows, including the earlier tuple-source contract. Revised
federation requires mapped entities and attributes; the local adapter's v2 work
must consume the compiler's EAV descriptor. Applications supplying arbitrary
tuples can use existing Datalog inputs directly.

The current adapter copies mutable leaves on construction; `local/replace-rows!`
atomically publishes a new version without changing accepted metadata. Open sessions pin
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

The [shared conformance tests](local/test/datalevin/federation/local_conformance_test.clj)
run Datalevin's [execution matrix](../datalevin/test-src/datalevin/query/source/conformance.clj)
against the local adapter. They cover seeded eager comparisons, native joins,
input and result key semantics, pinned replacement, public protocol faults,
cancellation, limits, and cleanup. The matching checkout supplies this test-only
harness through the development classpath; it adds no runtime dependency.

The [batch tests](local/test/datalevin/federation/local_batch_test.clj) check exact
row/byte boundaries, mutable ownership after close, empty/missing EAV values,
and reservation failure without consuming rows. Snapshot charges include the
outer row vector's metadata, including empty snapshots. Sizing stops at the
first excess charge or cancellation inside nested values; cancellation also
interrupts owned copying between values and releases any output reservation.
These checks cover acquisition, planning, cursor scratch, and batch handoff.
The adapter still reserves once per output batch.

## Shared mapping contracts

`compile-relation` accepts the main-schema definition map, a relation ID, and a
context containing a compute-context ID and recorded semantic dependencies.
It currently returns the accepted relation body with digests and a validated
v1 SPI source descriptor. Its v2 work must also project the accepted logical
schema and include it in the definition fingerprint, under the
[mapping-to-SPI contract](federation.md#43-compilation-to-the-spi).
The existing fixtures include the plan's normalized mapping, composite keys,
and resolved reference targets; v2 adds guarantee projection and invalidation
checks. The early revision also specifies unordered scalar/reference collections,
their absence and duplicate-element rules, and
[snapshot-scoped export identities](federation.md#snapshot-scoped-row-identity)
that never imply continuity across exports. Ordered occurrences and nested
objects need additional mapping rules. Concise mapping normalization and
physical-type conversion remain adapter work; the compiler consumes accepted
logical types.

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
