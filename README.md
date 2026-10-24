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
dataset/Parquet/DataFusion integrations. The common SPI uses v2 across
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
That mapping/identity foundation is implemented in
[`compile-logical-mapping`](core/src/datalevin/federation/mapping.clj) and
[`entity-ref`](core/src/datalevin/federation/entity_ref.clj). The common accepted
representation includes explicit extraction, scalar/reference `:many`, complete
logical schema, combined-mapping guarantees, and scope/completion fingerprints.
`normalize-column-mapping` handles declared columns; other pure provider
normalizers return the same accepted representation. `project-facts` projects
already converted records without provider I/O. The reference codec has bounded
JVM tests, native custom-type storage/reconnect coverage, and independently
verified JVM/Rust [golden fixtures](core/test/fixtures/README.md).
The active `compile-relation` now uses that normalization boundary and emits
complete v2 EAV descriptors and definition fingerprints. The local adapter and
shared fixtures use the same v2 contract as public Datalevin `q`/`explain`.
Scoped dependent entity/value access, input-free scans, grouped consistency and
independent consumption/replay declarations are covered by shared tests.
The default suite also runs deterministic SQLite, exported-file and paginated
HTTP fixtures through this adapter and the shared engine. They check request and
retry charges, scan/dependent semantics, native joins, actual read guarantees,
cancellation and incomplete acquisition. These bounded session-export fixtures
do not certify production SQL/file/API adapters. Native performance evidence and
the retained-resource audit live in the matching core checkout. No v1
compatibility mode exists.

## Subprojects and releases

| Directory | Artifact | Contents |
| --- | --- | --- |
| [`core`](core/project.clj) | `org.clojars.huahaiy/datalevin-federation-core` | Mapping compiler, canonical fingerprints, and persisted definition installer. |
| [`local`](local/project.clj) | `org.clojars.huahaiy/datalevin-federation-local` | Declared-schema local relation adapter. |
| [`github`](github/project.clj) | `org.clojars.huahaiy/datalevin-federation-github` | Bounded live GitHub work-item key lookups and native triage joins. |
| [`file`](file/project.clj) | `org.clojars.huahaiy/datalevin-federation-file` | Verified, bounded EDN Lines exports with snapshot-scoped identities. |

Each subproject owns its version, dependencies, source, tests, and Leiningen
release configuration. The initial development version is `0.1.0-SNAPSHOT`.
From a subproject directory, `lein jar` builds its JAR and POM, `lein install`
installs it locally, and `lein deploy clojars` publishes that artifact using
`CLOJARS_USERNAME` and `CLOJARS_PASSWORD`. Change only that subproject's version
when preparing its release. There is no aggregate release artifact.

Consumers add the artifacts they need. `local/source` accepts a complete v2
EAV descriptor and already mapped facts, so it uses Clojure and Datalevin.
Applications compiling accepted mappings also add `datalevin-federation-core`.
Existing `datalevin.federation.*` namespaces are unchanged.
Datalevin has Maven `provided` scope: the application explicitly supplies a
compatible Datalevin runtime rather than receiving one transitively from each
adapter.

Future API integrations, `postgres`, `dataset`, `parquet`, and `datafusion`
adapters follow the same layout, with artifacts named
`org.clojars.huahaiy/datalevin-federation-<adapter>`.
These subprojects are planned. Provider drivers and engine dependencies belong
only in the subproject that uses them. Adapters depend on an explicit compatible
version of `core` when they use its mapping APIs; `core` never depends on adapters.


The v2 cleanup baseline passed **71 tests / 22,818 assertions** in this
repository; the local alias passed **41 / 18,231**, including the transport
matrix. Core/sibling regression results and the completed native routing gate
are recorded in the [validation report](../datalevin/doc/measurements/query-source-native-2026-10-19.md).

## Development

The root `deps.edn` is a development workspace using the matching Datalevin
checkout at `../datalevin`; release POMs declare Maven dependencies instead.
Run all tests or select one subproject with an isolated source/test classpath.
The transport matrix requires `sqlite3` on PATH and permission to bind a temporary
loopback HTTP port; it uses no external service or persistent database:

```sh
clojure -M:test
clojure -M:test:core
clojure -M:test:local
clojure -M:test:file
```

The `:core` suite runs the pure mapping/identity gate without provider execution.
The complete suite also verifies native custom-type storage and reconnects.
It includes definition installation, concurrent refresh, dependency invalidation,
native schema edits, remote metadata access, and text/binary dump round trips.
Run the persistence checks alone with:

```sh
clojure -M:test datalevin.federation.install-test datalevin.federation.persistence-integration-test
```
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

## Persisted definitions

F2's [installer](core/src/datalevin/federation/install.clj) uses Datalevin's
schema snapshot and conditional-update APIs. It stores format **1** owner
envelopes containing accepted mapping representation **2**. These are separate
from each definition's increasing `:version` and its semantic fingerprint.
Provider normalizers produce accepted mappings before installation; column
declarations can use `mapping/normalize-column-mapping`.

```clojure
(require '[datalevin.federation.install :as install]
         '[datalevin.federation.mapping :as mapping])

;; conn is a Datalog connection; accepted is provider-normalized representation 2.
(def saved
  (install/install!
    conn
    {:sales
     {:datalevin/definition-kind :source
      :datalevin.federation/definition
      {:format-version 1
       :source-instance-id (:source-instance-id accepted)
       :provider (get-in accepted [:dependencies :provider :name])
       :profile :profiles/sales-readonly}}
     :sales/orders
     {:datalevin/definition-kind :relation
      :datalevin.federation/definition
      {:format-version 1 :source :sales :accepted accepted :depends-on #{}}}}
    {:expected-versions {:sales nil :sales/orders nil}}))

(def loaded (install/load-definitions conn))
(def descriptor
  (mapping/source-info (get-in loaded [:compiled :sales/orders :accepted])
                       :my-app/sales-runtime))

;; Remove both definitions together; native attributes and their data remain.
(install/remove! conn {:sales 1 :sales/orders 1})
```

Every submitted or removed ID needs an expected version: `nil` asserts absence,
and a positive integer asserts the current version. Stale definition versions
reject without overwriting a newer definition. Unrelated database revision
conflicts retry from a fresh snapshot, with bounded attempts and a deadline.
The returned `:schema`, `:revision`, and `:compiled` describe the committed
candidate. Source artifacts hold a fingerprint; relation artifacts also contain
accepted metadata, logical schema, pattern contract, and physical-schema digest.
Adapters supply runtime handles and their actual read capabilities separately.

The installer computes counters, dependency manifests, and fingerprints rather
than trusting submitted values. A replacement advances its definition version;
`:documentation` edits leave its semantic fingerprint unchanged. Semantic
source/relation changes recompile transitive dependents and advance their
versions when their fingerprints change. Relations record dependency IDs and
digests under `:accepted :dependencies :definition-contracts`; `:depends-on`
adds dependencies beyond the required source. Missing dependencies, cycles,
source identity mismatches, and incompatible domain/attribute contracts reject
the whole candidate. Source UUIDs are explicit, and changing identity requires
explicit compatible mapping updates.

Attribute memberships are derived and committed with definitions. New scalar
attributes receive compatible native types/cardinality; nominal/custom values
use opaque native backing, preserving the complete logical type in the accepted
mapping. Existing native types and cardinalities must agree. Include ordinary
property patches in `:schema-update` for a coordinated native schema change;
the installer does not silently overwrite them. Removing the final membership
retains the native attribute and data. Foreign owner metadata is preserved.

For discovery or refresh, optional `:prepare` takes
`{:schema schema :revision revision :definitions submitted}` and
`{:check-active! check-fn :remaining-ms remaining-fn}`, returning replacements
for exactly the submitted IDs. It runs outside the writer and runs again after
a revision conflict; it must tolerate repeated calls and bound its I/O using
the supplied control. Options `:max-attempts` (default 8), `:timeout-ms`
(default 5000, at most one day), and `:cancelled?` bound cooperative preparation
and retries. Remote calls share that deadline with the caller's request budget.
Installation inside an explicit write transaction rejects before preparation.

Persist only portable accepted metadata and qualified keyword profile
references. Source bodies reject inline credentials or runtime handles; profile
resolution and credential storage belong to the application/provider. Provider
normalizers must keep credentials out of accepted mapping metadata as well.
`load-definitions` verifies recorded formats, schemes, digests, dependencies, and
memberships without discovery or new identities. Corrupt or unsupported metadata
requires explicit replacement/revalidation. The integration tests cover reopen,
text/binary dump restore, embedded native joins, and remote metadata refresh.
The [file/export adapter](file/README.md) now reads pinned EDN Lines exports
through these saved mappings. The [GitHub adapter](github/README.md) also reopens
saved API definitions for live work-item lookups.

Validation (2026-10-21): the full companion suite passes **105 tests / 23,120
assertions**, including **17 / 142** persistence checks and **16 / 154** file
adapter checks. The standalone file artifact passes the same tests with
Leiningen and builds its JAR. Companion lint reports zero errors and warnings.

## Local adapter

`local/source` accepts a validated v2 EAV descriptor and a vector of
`[entity attribute value]` facts. Missing entities/values emit no fact.
Applications supplying arbitrary tuples can use ordinary Datalog inputs.

Construction owns mutable leaves and metadata; `local/replace-rows!` atomically
publishes a new fact version without changing its definition. Each session pins
and charges its immutable view and reports a distinct data version. It supports
snapshot reads, including requests for weaker minimum guarantees. Passing the
caller-owned object grants local read access.

The adapter offers single-pattern attribute enumeration and typed entity/value
lookups (`:mode` can restrict the offered paths). Core schedules dependent
bindings and joins every outer witness. Input-free opens omit bindings and IDs;
dependent batches echo the IDs supplied for that open, including empty final
completion. Predicates stay in core. Both paths size one borrowed row at a time,
reserve the output charge, then copy owned batches. Physical read metrics count
the sizing/copy passes and byte-boundary lookahead. Complete access exhaustion
precedes terminal result windows. The adapter performs no discovery or external
backend requests.

The [execution tests](local/test/datalevin/federation/local_test.clj) compare eager
results, native joins, aliases under writes, decimal key/predicate behavior,
nominal reference-valued EAV facts, and resource limits. The core [supported subset and remaining cases](../datalevin/doc/query-source-spi.md#9-source-conformance)
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

The [transport matrix](local/test/datalevin/federation/local_transport_test.clj)
uses the shared test harness with actual SQLite SELECTs, an immutable JSON export
and paginated HTTP responses. Its completed observations are explicitly owned
under the query budget during acquisition, then executed by the local adapter.
Materializing an HTTP observation preserves best-effort consistency and omits a
data version. Page/retry requests consume separate permits; cancellation or a
known page cap returns no partial result. Every live charge is released on exit.

## Shared mapping contracts

`compile-relation` accepts the main-schema definition map, a relation ID, and a
context containing a compute-context ID and recorded semantic dependencies.
It returns the accepted relation body with v2 digests and a validated complete
v2 EAV source descriptor under the
[mapping-to-SPI contract](federation.md#43-compilation-to-the-spi).
Mappings explicitly declare identity scope, extraction/conversion rules, finite
scope and completion. Scalar/reference `:one` and `:many` guarantees and
reference domains participate in the logical schema and definition digest.
Provider normalization consumes captured physical metadata without row I/O;
`project-facts` handles converted values and no-fact absence. Ordered occurrences
and nested-object identity still need additional mapping rules.

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

## GitHub work-item API

The optional [GitHub adapter](github/README.md) implements the first production
API lookup slice (2026-10-22). Explicit repository/deployment identities and
persisted mappings reopen without discovery; repository acquisition verifies the
numeric ID. Entity/key/number accesses use bounded per-key GETs with best-effort
consistency. Scalar/reference many fields join native triage records and pass
shared lookup conformance. HTTP/body timeouts, cancellation, rate/auth errors,
response/parser quotas and cleanup are exercised with real loopback HTTP.
`clojure -M:github-live` checks a native join against public GitHub data; it is
separate from deterministic tests. Scans, pagination and automatic retries are
not advertised. The initial F0–F3 workflow, same-syntax rule parity and native
performance gates are signed off for the advertised lookup/export contracts
(2026-10-24); see the [signoff report](../datalevin/doc/measurements/federation-signoff-2026-10-24.md).

Validation (2026-10-22): the complete companion suite passes **116 tests /
23,271 assertions**, including **11 / 151** GitHub checks. Standalone GitHub
`lein test`, `lein jar`, companion lint, and shared-helper lint pass. The
read-only public GitHub/native check passed separately.

## Integrated release triage (F3)

The [executable workflow](examples/README.md) composes production GitHub/file
adapters, native priorities and ordinary window/witness inputs through `d/q`.
Shared nominal issue/user references, many-valued joins, missing fields, mapped
and native rules, catalog reopen/refresh, grouped consistency, aliases, bounds,
blocked I/O and cleanup have complete eager integration checks. The read-only
public GitHub/export/native workflow passed after persisted reopen.

The [baseline](doc/measurements/release-triage-2026-10-22.md) retains 14 sequential
fresh JVMs, seven scenarios, exact query/input contracts, source snapshots,
complete result digests and actual request/byte/memory observations. It separates
literal/runtime priority thresholds, file scans, eager-local and native controls.
The unqualified large-input quota failure is retained separately from the accepted
explicit-window baseline. The
[follow-up](doc/measurements/release-triage-2026-10-22-followup.md) fixes ordinary
required-head rules and uses captured scalar thresholds before native scans feed
API accesses. Eager and composed queries now use identical rule syntax, comparing
full rows and witness-sensitive counts. The fresh workflow runs retain the same
complete answers with fewer API requests.

Validation (2026-10-22): the full companion suite passes **126 tests / 23,380
assertions**, including **10 / 109** integrated workflow checks. The public
three-source workflow passed previously. The historical 14-JVM baseline and its
378-file snapshot remain unchanged; the follow-up retains fresh source hashes,
request counts, complete result digests and native performance checks.

F0–F3 implementation signoff is complete (2026-10-24) for GitHub keyed lookups,
EDN Lines export scans, persisted mappings and native/ordinary-input joins.
The [final report](../datalevin/doc/measurements/federation-signoff-2026-10-24.md)
reconciles the functional, resource, packaging and native-performance evidence, including the later cache-binding
and rule-allocation fixes. Artifacts remain development snapshots requiring a
matching Datalevin build. Fusion, cost ranking, additional providers and views
retain their separate roadmap gates.
