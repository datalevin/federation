# PostgreSQL relations

`org.clojars.huahaiy/datalevin-federation-postgres` queries PostgreSQL tables and accepted views
through ordinary `d/q` and `d/explain`, alongside native Datalevin data and other
adapters. It supports table scans, entity and value lookups, and same-entity
fusion: several scalar attributes of one entity can be read by one SQL cursor.
Dependent lookups combine up to 256 input keys in a parameterized `VALUES`
join, with results associated with their original binding IDs.
Eligible decimal/time and `LIKE` filters and complete same-snapshot relational queries
can execute in PostgreSQL, including joins between accepted mappings and
supported aggregates.

This development snapshot requires the matching **Datalevin 1.2.10** build
with the optional whole-query, mixed-source fragment, regional-join, and native-text extrema SPI
extensions. The module declares `1.2.10` as a
provided dependency; the development workspace uses the sibling checkout.
SPI revision 2 alone does not establish availability of the new protocols.

## JOB performance milestone

The adapter's first performance goal is to beat native PostgreSQL on at least
one Join Order Benchmark (JOB) query over the full IMDb dataset. Both paths
must use the same PostgreSQL tables and indexes, return the same answer, and
include query planning and execution in the comparison. Loading an additional
Datalevin copy or retaining query answers does not satisfy this milestone.

The development harness supports JOB **6a**, **6b**, and **6d** (five tables),
**17c** and **17d** (seven tables), and **20b** (ten aliases over nine tables).
They cover actor, keyword, company, character, and cast-completeness filters.
It uses the production adapter through `d/q`, with explicitly accepted typed references.
Point it at an already loaded, static JOB database:

```sh
export POSTGRES_JOB_URL='jdbc:postgresql://127.0.0.1:5432/postgres'
export POSTGRES_JOB_USER="$USER"
clojure -M:postgres-job '{:warmups 2 :samples 7}' /tmp/postgres-job-6a.edn
```

The JOB harnesses use an 8 GiB retained-memory allowance, a 256 KiB payload-row
cap, a 16,384-request budget and a five-minute timeout. The JOB alias starts
with a 1 GiB heap and allows it to grow to 16 GiB. The retained allowance is a
conservative accounting cap, not a heap reservation. Binding batches allow
2 MiB and at most 256 keys, including typed-reference ownership charges.

Override individual budgets with `:limits {:retained-bytes ...}` and payloads
with `:max-row-bytes`. Partial limit overrides preserve the other JOB defaults.
Reports include effective query limits, source payload/fetch settings, and the
JVM's maximum heap size. Ordinary federation queries retain the engine's
1 GiB default allowance; these larger defaults apply to the JOB harnesses.

Optional `POSTGRES_JOB_PASSWORD` supplies the password. Credentials stay out of
the report. Set `:query` to select a supported query, for example `:17c` or `:20b`. `:schema`,
`:timeout-ms`, `:max-row-bytes`, `:fusion?`, `:relational?`, `:statistics?`, `:filtered-statistics?`, `:native-extrema?`, and explicit
query-source `:limits` are also configurable. The harness performs reads only;
it does not load data, create indexes, or run `ANALYZE`. Keep the dataset static
throughout a run because the compared invocations acquire separate snapshots.

Before timing, the harness checks every distinct matching output tuple against
SQL and checks the final aggregate answer. It retains every original JOB output
column, including 17c's duplicated minimum, and PostgreSQL's original text collation. A different text minimum
fails the comparison, even when the matched tuples agree. Empty input cannot
qualify as a win.

The harness explicitly requests source-native text extrema so its minima use
the original PostgreSQL column collations. Set `:native-extrema? false` to test
ordinary Datalog extrema and reproduce earlier collation failures. The native
SQL oracle is unchanged; reduction requests count toward adapter time and quotas.

Samples alternate execution order. Both client wall times include a fresh
connection, a read-only snapshot, planning, execution, result decoding, and
cleanup. The adapter additionally pays its normal per-query schema validation
and source planning costs. One-time discovery/acceptance is reported separately.
Preflight checks warm the database; these are warm-cache measurements. Native
`EXPLAIN (ANALYZE, BUFFERS, SETTINGS)` and adapter explanations are separate
diagnostics, outside the timed samples. The report includes raw timings,
median/p95 latency, relation estimates, indexes, effective budgets, and source
fingerprints. Failures record the stage and budget category and retain any
completed evidence.

A run marks `:win? true` only with at least five successful sample pairs,
at least 1.1x median speedup, and the adapter faster in at least 80% of pairs.
Repeat a qualifying run in fresh JVMs before claiming the milestone. The
harness defaults to 6a; this is the first candidate, not a commitment
to that query as the eventual winner.

Run its focused checks with:

```sh
clojure -M:test datalevin.federation.postgres-job-test
POSTGRES_TEST_URL='jdbc:postgresql://127.0.0.1:5432/federation_test' \
  clojure -M:test datalevin.federation.postgres-job-integration-test
```

The integration checks create and remove isolated fixture schemas in the
disposable test database. They cover duplicate joins, absent references, nulls,
filters, empty aggregates, incompatible text minima, and budget failures.

## Full JOB suite timing

The [latest full 113-query pass](../doc/measurements/postgres-job-full113-post-fragments-2026-11-10.md)
completes and matches every original aggregate answer. Adapter client time is
**155.82 s versus native PostgreSQL's 166.38 s**, 6.3% less in this pass. There
are no aggregate execution failures or result mismatches. Complete projected
tuple sets verify for 107 queries; six additional tuple checks hit the unchanged
64 MiB result limit after their original aggregates pass. This is one sample
without warmups or controlled cache state, with native timings varying between
passes, rather than a repeated full-suite win.

On the same 113 aggregates in the
[preceding full pass](../doc/measurements/postgres-job-full113-post-targets-2026-11-10.md),
adapter time rises from 140.28 s to 155.82 s, while native time rises from
157.80 s to 166.38 s. The seven retained dependent plans improve from 11.87 s
to 11.12 s combined. Query 17a is the largest adapter regression, from 7.68 s
to 19.95 s; 17a, 19d, and 26a have the largest gaps against native in this pass.
These separate single passes do not isolate the code change from runtime
variation. The
[31b/7c/17f investigation](../doc/measurements/postgres-job-31b-7c-17f-2026-11-10.md)
explains the repaired memory failures and automatic plan choice. The latest
report retains all per-query regressions and distinguishes aggregate execution
from the separate tuple oracle.

The [single-SQL expansion screen](../doc/measurements/postgres-job-single-sql-2026-11-10.md)
reduces the same 40-query cohort from **34.85 s to 17.85 s**. Native takes
**37.70 s** in the new pass. All 43 executed queries verify their original
aggregates and full projected tuple sets. Its plan-only inventory selected
one SQL plan for 101 of 113 queries, up from 20, with nine dependent plans and
three nested-OR shapes in core before the follow-up above. This is a one-pass cohort comparison;
it does not replace the full 113-query execution baseline below. Separate
five-pair repeats preserve the 6d/17c wins at 339/579 ms against native's
5.07/6.28 s, winning every pair.

The full-suite harness reads all 113 original SQL files from the sibling
`datalevin/benchmarks/JOB-bench/queries` directory. It translates their flat
joins and predicates into accepted PostgreSQL sources and keeps every original
aggregate output and table-existence check. It uses the same 16 GiB JVM and
raised budgets as the single-query harness:

```sh
clojure -M:postgres-job:postgres-job-suite \
  '{:samples 1 :warmups 0}' /tmp/postgres-job-suite.edn
```

`POSTGRES_JOB_URL`, `POSTGRES_JOB_USER` and `POSTGRES_JOB_PASSWORD` have the same
meaning as above. Optional `:corpus` and `:schema` select the existing SQL corpus
and database schema. A `:queries` vector runs an explicitly labeled subset for
debugging. The database must remain static throughout the run.

For the four queries that reached the earlier limits, run:

```sh
clojure -M:postgres-job:postgres-job-suite \
  '{:queries [:7c :20c :29a :29c] :samples 1 :warmups 0}' /tmp/postgres-job-raised-limits.edn
```

Earlier measurements retain their recorded 1 GiB memory, 16 KiB payload,
1,024-request and 8 GiB JVM settings. Use explicit options and JVM flags to
reproduce those settings.

Each query's native and adapter invocations alternate order. Timed calls include
connection, snapshot, schema validation, planning, cardinality estimation,
execution, decoding, and cleanup. JVM startup, initial catalog discovery,
mapping acceptance, warmups, and correctness oracles are separate. Each
successful aggregate comparison is followed by a distinct-output tuple check;
`:oracle-max-rows` bounds that untimed check. Results checkpoint after every
query, and an adapter failure does not prevent timing native PostgreSQL or
continuing to the next query.

The summary reports a native total only when every selected native query
completed. A full-suite adapter total and speedup require all 113 queries to
complete and pass correctness checks. Otherwise, verified subtotals, attempted
time, unsupported translations, resource limits, timeouts, and verification
failures remain separate. External `IS NULL` uses bound attribute absence;
nullable `MIN` outputs use optional projections and independently ignore NULLs.
Unsupported queries never count as zero-time successes.

The default single pass is a baseline screen, with no claim of repeated wins
or controlled cold-cache behavior. Use repeated samples and warmups to assess
stable performance once coverage permits a complete comparison. The generated
Datalog and original SQL, per-query raw times, limits, environment and source
fingerprints are included in the artifact.

Timed native aggregates leave JDBC fetch size at zero. Only the untimed tuple
oracle uses bounded fetching. PostgreSQL can execute a parallel plan without
workers when a client sends a nonzero fetch count; see its
[parallel-query restrictions](https://www.postgresql.org/docs/18/when-can-parallel-query-be-used.html).

A native-only replay can replace a confounded native control without repeating
adapter executions. It checks the SQL corpus and every aggregate answer against
the completed reference report and labels the measurements as separate passes:

```sh
clojure -M:postgres-job:postgres-job-native \
  '{:samples 1 :warmups 0 :timeout-ms 60000}' \
  /tmp/postgres-job-suite.edn /tmp/postgres-job-native.edn
```

Run the suite harness checks with:

```sh
clojure -M:test datalevin.federation.postgres-job-suite-test
POSTGRES_TEST_URL='jdbc:postgresql://127.0.0.1:5432/federation_test' \
  clojure -M:test datalevin.federation.postgres-job-suite-integration-test
```

The [2026-11-09 full-suite summary](../doc/measurements/postgres-job-suite-summary-2026-11-09.edn)
and [per-query CSV](../doc/measurements/postgres-job-suite-queries-2026-11-09.csv)
contain one sample per query, no explicit warmup, a 60-second deadline, an 8 GiB
heap, a 1 GiB retained-memory cap, and a 1,024-request cap:

| Scope | Native client wall time | Adapter client wall time |
| --- | ---: | ---: |
| All 113 original queries | 136.10 s | Incomplete |
| The same 40 verified queries | 36.29 s | 199.44 s |

The initial adapter baseline was **5.50x slower on the verified subset**. The other 73 queries
comprise 42 memory-limit failures, 11 request-limit failures, nine unsupported
plans, nine external `IS NULL` translations, one independent nullable `MIN`,
and one timeout. All source sessions closed, and no result mismatch occurred.
Adapter calls including failed attempts consumed 1,854.38 seconds; this is
attempted work, **not** a full-suite completion time.

The [adapter screen](../doc/measurements/postgres-job-suite-adapter-screen-2026-11-09.edn)
retains its original native timings as explicitly superseded diagnostics.
Those calls used a nonzero JDBC fetch size and could suppress parallel workers.
The [corrected native control](../doc/measurements/postgres-job-suite-native-control-2026-11-09.edn)
replayed all 113 queries with fetch size zero and rechecked every original
aggregate answer. Adapter and corrected native times come from separate passes,
with no cache flush or controlled cache state. Treat this as a baseline screen,
not paired samples or a repeated-win result.

The [current optimization results](../doc/measurements/postgres-job-next-targets-2026-11-09.edn)
repeat every one of those 40 queries, with the same original aggregate answers
and distinct-output tuple hashes. Adapter total time is now **110.42 s**, down
from **199.44 s** (**44.6%**). Native total in this cohort run is **33.43 s**,
leaving a **3.30x** gap. Both cohort totals are single-pass screens. The complete
113-query suite has not been rerun after these changes, so there is no updated
full-suite adapter total or coverage count.

| Target | Initial adapter screen | Optimized median, JVM 1 | Optimized median, JVM 2 |
| --- | ---: | ---: | ---: |
| 1c | 15.64 s | 139 ms | 133 ms |
| 8b | 22.45 s | 381 ms | 370 ms |
| 14b | 24.25 s | 290 ms | 262 ms |
| Sum | 62.34 s | 0.810 s | 0.765 s |

Reproduce the focused comparison with:

```sh
clojure -M:postgres-job:postgres-job-suite \
  '{:queries [:1c :8b :14b] :samples 7 :warmups 2}' /tmp/postgres-job-targets.edn
```

Each optimized target run has two warmups and seven alternating native/adapter
sample pairs in a fresh JVM. All original SQL outputs and distinct matching
tuples pass in both runs. The production adapter reduces eligible compound
text filters and explicit source-native extrema in PostgreSQL, transferring
one aggregate row per target. Baseline diagnostic plans transferred 21,195,
50,791 and 55,675 rows respectively. Retained-memory and request limits are
unchanged. See [JVM 1](../doc/measurements/postgres-job-targets-optimized-run1-2026-11-09.edn),
[JVM 2](../doc/measurements/postgres-job-targets-optimized-run2-2026-11-09.edn),
the [40-query run](../doc/measurements/postgres-job-common40-optimized-2026-11-09.edn)
and [per-query comparison CSV](../doc/measurements/postgres-job-common40-comparison-2026-11-09.csv).

Exact ASCII `LIKE`/`NOT LIKE` calls and bounded same-variable `LIKE`
disjunctions can participate in relational filtering. Advisory disjunction
hints require explicit provider support; unsupported branches keep the complete
local disjunction. `:predicate-prefilters? false` disables those hints in a
PostgreSQL source profile. Ordered text comparisons use C collation only for
ASCII cutoffs, where they preserve ordinary Datalog ordering. Chained integral
comparisons normalize inside complete SQL delegation, preserving existing
scoped fallback plans.

Native text reduction requires an explicit `:native-extrema` declaration that
matches the selected source attribute. It preserves that column's collation and
rejects nondeterministic collations. The optimizer restricts new text-filter
delegation to at most eight table groups, or twelve for exact bound absence,
keeps selective positive-filter probe plans available, and declines new
text-filtered partial joins whose correlated
fan-out is unpriced. These bounds preserve the 6d, 10c and 17c screening wins.
Ordinary Datalog text extrema retain their existing ordering.

Validation covers 284 tests with 7,637 assertions and no failures or errors;
targeted lint has no errors or warnings. All source sessions closed. The next
largest measured excesses in the 40-query cohort are 4c, 18a and 28b.

The [2026-11-10 absence screen](../doc/measurements/postgres-job-absence-summary-2026-11-10.edn)
verifies all nine previously rejected `IS NULL` queries: 11a, 11b, 18b,
21a–21c and 27a–27c. All original aggregates and distinct output tuples agree
with PostgreSQL, and every source session closes. The five regression queries
1c, 6d, 8b, 14b and 17c also pass. Bound absence uses the shared subtraction
operator; eligible SQL plans translate it to `IS NULL`.

Admitting exact absence plans with up to twelve table groups lowers the nine
queries' combined adapter time from 38.77 s to **10.73 s**, including planning,
estimation and cleanup. Native takes **0.57 s** on those same nine queries.
27b falls from 25.82 s to 1.01 s; 27a and 27c rise to 3.60 s and 3.92 s and
remain optimization targets. These are single-pass screens in separate JVMs,
without warmups or a controlled cold-cache claim. The original 113-query suite
has not been rerun, and these subset timings do not form a new full-suite total.
See the [per-query comparison](../doc/measurements/postgres-job-absence-comparison-2026-11-10.csv)
and [test output](../doc/measurements/postgres-job-absence-tests-2026-11-10.txt).

The [OR investigation](../doc/measurements/postgres-job-or-investigation-2026-11-10.edn)
reruns the nine historical planner rejections. All nine now plan. Their original
failure was a missing scoped dependency: provider projection could discard a
column consumed only by `or-join`. Source requests now retain every scoped
operator input. A controlled projection replay reproduces all nine old
rejections with LIKE hints disabled; enabling those hints rescues seven, while
7c and 12b still require the projection fix. Current planning succeeds with
hints either enabled or disabled.

| Result under the earlier 1 GiB / 16 KiB budgets | Queries |
| --- | --- |
| Original aggregates and distinct output tuples verified | 12b, 19a, 19b, 23a, 23c |
| 1 GiB retained-memory limit | 20c, 29a, 29c |
| 16 KiB payload-row limit in a joined biography read | 7c |

These were execution limits in supported scoped programs. Diagnostic traces show
20c materializing 85,941 intermediate rows and 29c reading 106,001 person rows
before narrowing to 50,011. In 29a, the next movie-keyword lookup hits the
memory cap after a frontier of 810 wide tuples, so retained allocations also
need profiling. Raising 7c's payload cap to 256 KiB moves it to the same 1 GiB
memory limit. No default limits changed.

The [current per-query CSV](../doc/measurements/postgres-job-or-current-2026-11-10.csv)
records one sample without warmups; plan-only ablations and execution traces
are separate from timed calls. The five verified queries take 24.76 s in the
adapter versus 0.72 s in native PostgreSQL. This subset does not establish a
new full-suite time. New regression coverage includes private LIKE/equality OR
inputs, nested AND/OR and multiple OR clauses with hints disabled; the targeted
suites pass 95 tests and 802 assertions with clean lint. See the
[test output](../doc/measurements/postgres-job-or-tests-2026-11-10.txt).

The [raised-budget screen](../doc/measurements/postgres-job-raised-limits-summary-2026-11-10.edn)
uses the current JOB defaults: 8 GiB retained memory, 256 KiB payload rows,
16,384 requests, a five-minute timeout, and a 16 GiB JVM. **20c now verifies**
both original minima and all 3,593 distinct output tuples. Its client-wall time
is 133.32 s versus native's 1.16 s in this single-pass screen. Queries 7c, 29a
and 29c still reach the raised 8 GiB memory cap; 7c clears the payload-row cap.
All source sessions close. The configuration and harness tests pass 23 tests
with 192 assertions; see the [test output](../doc/measurements/postgres-job-raised-limits-tests-2026-11-10.txt).
The report preserves the run-start source fingerprints and records a concurrent
estimator source change; these measurements do not isolate a before/after
performance effect or establish a full-suite time.

The [nullable-minimum screen](../doc/measurements/postgres-job-nullable-min-summary-2026-11-10.edn)
verifies **JOB 1a**, including all three original minima and 116 distinct output
tuples. The translator preserves rows with absent projection attributes, so
each minimum can independently ignore NULL. Matching SQL delegation transfers
one aggregate row; the same optional projections also work through the scoped
fallback. This dataset's matching tuples have non-NULL years; isolated fixtures
exercise mixed NULL columns, independent minima, all-NULL groups, empty inputs,
native text ordering, and captured views.

The single-pass client-wall result is 0.84 s for the adapter and 0.054 s for
native PostgreSQL, without warmups. Seven regression queries—1b, 1c, 6d, 8b,
11a, 14b and 17c—also pass their aggregate and tuple checks. The original full
113-query suite has not been rerun. Validation passes 376 tests and 8,774
assertions with clean lint; see the [test output](../doc/measurements/postgres-job-nullable-min-tests-2026-11-10.txt)
and the [nullable projection example](#identity-types-and-absence).

## Workload fit and current gaps

The current fit is selective enrichment of a small set of keys from ordinary
primary-key tables, including joins with native data and other adapters.
Same-entity fusion avoids reading each scalar attribute separately. Flat joins
and supported aggregates within one database snapshot can be delegated as a
single relational access. A flat query joining native or local customer data
to one PostgreSQL session can also delegate its PostgreSQL portion through a
parameterized fragment. Unsupported expressions and independent adapter/session
combinations use ordinary fact accesses and core joins.

The following gaps matter when moving from that workflow to a typical
application schema or a larger reporting query:

| Use case | Current gap | Effect on the workload |
| --- | --- | --- |
| Enrich large key sets | Batches accept at most 256 keys and 2 MiB of bindings; matches arrive in bounded fetch pages | Request and memory budgets apply across all batches. Network latency and nonunique value matches can still dominate. |
| Filter amounts, times, or names | Matching integral, `BigDecimal`, and `Instant` comparisons and eligible binary `like`/`not-like` calls are pushed down | Eligible filters reduce transfer. Mixed representations, unsupported patterns/call forms, and out-of-range timestamp bounds can still require a broad read. |
| Read an existing application schema | Temporal types and enums are supported; domains, `char`, `citext`, arrays, and JSON/JSONB remain unmapped | Supported columns can be selected independently. Collection-valued attributes are not implemented. |
| Read reporting views, partitioned datasets, or tenant tables with RLS | Views need declared keys; partitioned parents need primary keys; RLS needs an enforced authorization profile | These relations capture all mapped columns at acquisition under shared row, request, and retention limits. Subsequent accesses use the captured rows; SQL predicate, join, and aggregate delegation is unavailable for these captures. |
| Aggregate, sort, or join large PostgreSQL relations | Whole-query and mixed-source fragment delegation cover eligible flat joins and aggregates; eligible whole-query ordering/limit windows execute in SQL | Mixed native/SQL spending queries can return customer totals instead of every order. Complete aggregate groups must fit a binding batch; unsupported shapes retain fact accesses. Catalog cardinalities and observed request latency guide fact-access ordering. Index discovery and backend execution-time estimates remain absent. |
| Read large text or binary fields | The default payload-row cap is 16 KiB, measured using SQL text representations | `_` existence checks use presence flags and do not fetch the payload. Values needed for output or local matching remain capped; delegated SQL applies the cap to its final returned row. |
| Discover relationships | Discovery covers one named relation, its columns, key, enum labels, and applicable view/security contracts; foreign-key discovery is absent | Define references manually. Use PostgreSQL policies and explicit runtime role/settings for authorization; query filters do not establish an authorization boundary. |

Supported columns do not require mapping every physical field. Omitting a
JSONB or unsupported domain column is useful when the query does not need
it; it does not make that column queryable. Likewise, a value-lookup alternative
does not guarantee a PostgreSQL index exists for that column.

`timestamptz` is exposed as `java.time.Instant` under logical type `:data` to
preserve microseconds. Native Datalevin `:db.type/instant` uses `java.util.Date`.
Joining those representations needs an explicit conversion and a decision
about precision; they are not interchangeable query keys.

## Planning bounds and follow-up priorities

The [adapter planner](src/datalevin/federation/postgres/plan.clj) preserves a
complete set of independent accesses within `:max-alternatives`, default `32`,
then fills remaining slots with lookup and separate-pattern alternatives.
Same-entity fusion needs one reserved access per distinct entity term;
patterns with an ignored entity each need their own access. With fusion
disabled, each pattern needs its own reserved access. Repeated entity-lookup
anchors within one fused group share a single alternative.

Root-conjunction plans can also consider filtered, dependent joins between two or three
connected entity groups from different aliases in the acquired PostgreSQL session. These alternatives
share the coordinator alias's remaining candidate quota and preserve the
ordinary fact alternatives. Candidate construction considers at most 32 patterns
and 128 attempted joins, with a 50 ms allowance checked between attempts. Rules,
branches, captured relations, and unsupported expressions keep their existing
paths. No provider reads occur during this construction.
Selected dependent joins use parameterized `LATERAL` lookups with PostgreSQL
planning fences, preserving the key-driven dependency order inside each batch.
Subqueries select only accepted mapped/key columns; the original checks and
parameter order remain in the outer query.

The core first finds a complete typed plan, then compares up to 16 distinct
starting sources using propagated `:rows`/`:rows-per-binding`. It estimates
returned-row work across complete paths, with a 20 ms allowance and the remaining
4096-state search budget. Regions with at most eight work items and 32 choices
use a 1 ms allowance and at most 128 additional search checks. The complete
feasibility search retains its original bound; optional lookahead keeps that
complete plan when its smaller allowance expires. It skips the baseline driver
already evaluated. Unknown or expired counts, unmodeled local producers,
and unknown selectivity for a second independent scan retain the original plan.
When every access on the baseline path has fresh time hints, lookahead ranks
complete paths by estimated access time plus local row/column processing, then
row work and peak tuples. Missing time hints retain cardinality ranking. The
model includes logical cursor opens at the effective binding batch size; it
does not treat a backend's per-request binding cap as another cursor open.
PostgreSQL reuses already observed metadata-request latency to price three
fixed cursor requests (declare, terminal fetch, close) and estimated fetch
pages. This adds no estimation I/O. These low-confidence transport estimates
exclude most backend execution work and can underprice skewed joins.
Joined estimates use catalog distinct counts for equality and text membership
filters, retaining the strongest local filter rather than multiplying correlated
predicates. Acquired single-column reference-domain sizes refine sampled
distinct counts under a low-confidence uniform-domain assumption; they never
prove referential integrity or completeness. Fractional fan-out can reduce propagated tuple counts; a floor of
one keeps costing from treating estimated emptiness as proof. Constant filters
in patterns also qualify a connected pair for dependent join pushdown.
Groups whose only live output is their bound key can become existence checks.
PostgreSQL's distinct projection returns at most one match per binding, avoiding
unused company fan-out. Explicit find/with witnesses remain exported and retain
their multiplicity.
Optional filter estimates distinguish tiny dimensions from selective lookups
into large relations when pricing the opportunity to prepare metadata. The
existing time, request, and latency-based preparation caps remain in effect.
Explanations show `:join-cost`, `:cost-search`, and `:selection :join-cost` when
lookahead changes the selected path. Planning time includes provider plan
construction, capture, and scheduling; optional estimation is reported separately.
`:cost-search :model` identifies `:elapsed` or `:cardinality`; `:join-cost`
separates `:access-ns`, `:local-ns`, `:estimated-ns`, and estimated opens/requests.
The default `:query-source :planning` options are `:cost-model :elapsed`,
`:local-row-ns 1000`, and `:local-column-ns 250`. The local coefficients are
nonnegative integer heuristics, configurable for the deployment. Set
`:cost-model :cardinality` to compare the original row-work model.

Execution prepares immutable reference descriptors once per cursor or mapping
projector and reuses validated bound references. Each new key still receives
its own value validation, wire encoding, ownership, and retained-memory charge.
Dependent boundaries collect positional tuples directly instead of first
building owned row maps, and project columns after their last use while keeping
find/with witnesses and shared join keys. The single empty input tuple scans
directly without building a redundant hash join. Complete binding collection
and final boundary materialization remain; these changes do not permit partial
or replayed lookups. The
[fragment execution measurements](../doc/measurements/postgres-job-fragment-work-2026-11-10.md)
cover the retained JOB dependent plans and single-SQL controls.

The provider prepares access alternatives and estimate SQL once per session and
request, sharing them across estimation and final planning. Changed bindings,
available inputs, projection, scope, or candidate bounds require new preparation.
Group column schemas and sort keys are also computed once. Captured descriptors
are checked by identity inside the compiler; public requests and provider
responses retain their structural validation.

Metadata ownership shares unchanged immutable containers after visiting their
children, while copying mutable values and detaching vector views. Byte sizing
uses one ledger snapshot followed by an atomic reservation, preserving concurrent
quota enforcement. Ordinary metadata avoids generic custom-value protocol
hierarchy walks; direct implementations and externally extended values retain
their contracts. Once a complete delegated SQL plan is selected, acquisition
skips optional cardinality statistics while retaining schema, authorization,
locking, and snapshot checks.

Projection analysis retains final outputs, input keys, local operator variables,
and cross-access join variables. Unused internal entity identities need not be
constructed or returned. Physical decoding and conversion guards still run.
Child-region contracts remain conservative across rule and branch boundaries.

An explicit `:query-source` option can request PostgreSQL text ordering for
selected `min`/`max` inputs:

```clojure
{:native-extrema {'?name {:source '$people :attribute :person/name}}}
```

The core reduces each complete aggregate group through the acquired source
session in bounded batches, carrying forward each batch's winner. The provider
inherits the accepted column's exact collation and rejects nondeterministic
ordering. Values remain parameters; requests share the invocation deadline,
authorization, memory, binding, and request budgets. Plan-only explains perform
no extrema reductions. This option uses the optional `IQuerySourceTextExtrema`
protocol in the matching development build. Ordinary extrema retain Datalog
ordering, and unsupported explicit requests fail instead of silently changing it.

Wide queries, including eleven attributes on one bound entity plus a second
entity clause, retain complete coverage under the default cap. If the cap
cannot hold even the reserved accesses, planning fails explicitly with
`:query-source/limit`. Increase the cap for that query, for example with
`{:query-source {:planning {:max-alternatives 64}}}`.

Ordinary tables supply advisory row counts by default. Each acquired mapping
reads `pg_class.reltuples` and the mapped columns' null fractions and distinct
counts from [`pg_stats`](https://www.postgresql.org/docs/current/view-pg-stats.html).
These bounded scalars travel with the existing mapping validation reads during
acquisition, adding no round trips. Pure access planning performs no provider
reads. Catalog statistics fetch no row values or histograms. Statistics remain owned by
the query session and outside accepted mapping fingerprints; running `ANALYZE`
does not require accepting the mapping again. Captured views, partitioned
parents, and RLS relations retain their existing planning behavior.

The adapter estimates total rows for independent accesses and rows per binding
for dependent lookups. Equality uses a uniform distinct-value estimate, null
fractions account for absent facts, and primary-key equality caps the estimate
at one. Correlated equality filters are not multiplied. Missing or unanalyzed
statistics remain unknown; these are low-confidence counts, not exact answers.
These baseline counts do not model ranges, skew, index availability, or transfer costs.

Before pure access planning, an optional estimation phase can refine independent
filtered accesses with `EXPLAIN (ANALYZE FALSE, FORMAT JSON)`. It never executes
the SELECT. Identical filter projections share one probe; only bounded root row
counts survive in the authorized session. SQL and plan text stay private.

Estimation itself has a cost. Providers quote it without I/O; PostgreSQL prices
each probe at twice the mean request latency observed in that session, including
admission waits. The core spends at most one eighth of the cheapest known
independent access's estimated fetch-request work. Only a more expensive source
that could become the driver is probed. A single possible driver, incoming
bindings, missing cost information, or a probe that cannot fit the allowance
keeps the catalog plan. This is a conservative admission heuristic, not a model
of expected savings or complete execution cost.

The shared invocation caps are `:query-source :planning :estimate-budget-ms`
(default `100`) and `:estimate-max-requests` (default `8`). Either set to zero
disables optional probes. Time includes quote and preparation work and is
checked between calls; an individual slow request remains subject to the normal
query deadline and provider statement timeout. Explanations expose each region's
`:estimation` decision, reason, actual requests, elapsed nanoseconds, quoted
provider cost, and opportunity budget. Set `:filtered-statistics? false` on
participating profiles to retain catalog counts without filtered probes.

The matching Datalevin core ranks fresh cardinalities before its structural
tie breaks while preserving dependency, type, coverage, and resource checks.
Counts expire according to `:query-source :planning :estimate-max-age-ms`
(default 300,000 ms) and any provider validity interval. Their observation time
records the catalog read, not the last `ANALYZE`; stale database statistics can
still give poor estimates. Unknown or expired counts follow fresh counts and
use structural ordering among themselves.
An estimated zero never proves a query empty. Set `:statistics? false` on the
participating runtime profiles to compare the previous structural plan. The
explanation reports `:selection :cardinality` when counts influence ordering;
time estimates remain unknown.

Full-data comparisons include connection, snapshot acquisition, planning,
estimation, execution, source-native extrema, and cleanup. Every run checks all
distinct matching output tuples and every original aggregate output. The
[measurement summary](../doc/measurements/postgres-job-target-optimizations-2026-11-09.edn)
verifies 21 current source fingerprints and closed sessions across all reports.

A subsequent [planning comparison](../doc/measurements/postgres-job-cheap-planning-2026-11-10.md)
uses eight queries, two warmups, and five alternating sample pairs in separate
before/after JVMs. Median summed adapter time falls from **4.79 s to 1.67 s**
(65% less). Queries 1a/1b/1c reach 40.2/28.3/35.8 ms against native's
37.7/26.0/34.3 ms. All eight verify their original aggregates and complete tuple
sets; 6d and 17c remain faster than native in every final pair. Native subtotal
variation is retained in the report. This cohort uses the current JOB limits and
does not update the full 113-query coverage count.

All runs below use a 1 GiB retained allowance, 2 MiB binding allowance, and an
8 GiB maximum heap. Queries 17c, 17d, 20b and 6d use two warmups and seven
alternating sample pairs. The 6a/6b regression screens use one warmup and three
pairs. Qualifying 17c and 6d runs are repeated in fresh JVMs.

| Query | Adapter median | Native median | Median speedup | Evidence |
| --- | --- | --- | --- | --- |
| 17c, fresh JVM 1 | 2.53 s | 5.85 s | 2.31x | [Run 1](../doc/measurements/postgres-job-17c-optimized-run1-2026-11-09.edn) |
| 17c, fresh JVM 2 | 2.54 s | 5.82 s | 2.29x | [Run 2](../doc/measurements/postgres-job-17c-optimized-run2-2026-11-09.edn) |
| 17d | 5.82 s | 5.87 s | 1.01x | [Seven pairs](../doc/measurements/postgres-job-17d-optimized-run1-2026-11-09.edn) |
| 20b | 1.70 s | 1.77 s | 1.04x | [Seven pairs](../doc/measurements/postgres-job-20b-optimized-run1-2026-11-09.edn) |
| 6d, fresh JVM 1 | 0.879 s | 4.59 s | 5.21x | [Run 1](../doc/measurements/postgres-job-6d-optimized-run1-2026-11-09.edn) |
| 6d, fresh JVM 2 | 0.877 s | 4.51 s | 5.15x | [Run 2](../doc/measurements/postgres-job-6d-optimized-run2-2026-11-09.edn) |
| 6b regression screen | 1.05 s | 188 ms | 0.18x | [Three pairs](../doc/measurements/postgres-job-6b-optimized-run1-2026-11-09.edn) |
| 6a regression screen | 1.31 s | 34.0 ms | 0.026x | [Three pairs](../doc/measurements/postgres-job-6a-optimized-run1-2026-11-09.edn) |

**JOB 17c adds a verified win alongside 6d.** Each beats native in all fourteen
timed pairs across two fresh JVMs. The 146 distinct 17c tuples and both original
minima agree, including the locale-sensitive `X, Andie` result. The 68 distinct
6d tuples and all three minima also agree. Native timings vary; raw samples and
diagnostics are retained. No additional indexes, Datalevin copy of IMDb, or
query-answer cache are used. Every invocation acquires a fresh read view.

Queries 17d and 20b are faster in six of seven pairs, but neither reaches the
required 1.1x median margin, so they do not qualify as wins. All 668 distinct
17d tuples and all three distinct 20b titles agree with SQL. The earlier
[screening](../doc/measurements/postgres-job-followups-summary-2026-11-09.edn)
failed at memory, request or deadline gates even with 4 GiB retries; both now
complete within the default 1 GiB allowance. The 6b adapter median improves from
its earlier 2.57 s screen, while native remains substantially faster on 6a/6b.

The 17c/17d paths apply the cast/movie-keyword/keyword filter before fetching
company existence. Their executed diagnostics use 13/29 cursors and return
3,074/8,718 rows. The 20b path starts with the two matching actors and probes
complete-cast using known movie keys; it opens six cursors and returns 124 rows.
Its executed explanation reports about 0.89 s of planning, a material remaining
cost. The 6d path now uses three cursors, 129 returned rows and three batches,
compared with four cursors and 369 rows in the
[previous joined run](../doc/measurements/postgres-job-6d-joins-run2-2026-11-09.edn).

The [collation diagnostic](../doc/measurements/postgres-job-17c-collation-2026-11-09.json)
records why ordinary Datalog extrema could not satisfy 17c's original SQL:
PostgreSQL's locale minimum is `X, Andie`, while its binary minimum is
`X'Volaitis, John`. Explicit source-native extrema now preserve the column's
ordering; the original SQL oracle remains unchanged.

Earlier [JFR profiles](../doc/measurements/postgres-job-6d-optimization-profiles-2026-11-09.json)
compare output batching alone with the initial joined implementation. Sampled
application allocation weights fall from about 3.21 GB to 118 MB across three
calls in each recording. These are profiler estimates from the earlier stage,
separate from retained-memory accounting and the current paired comparisons.

Earlier small-memory and cardinality baselines remain under
[measurement artifacts](../doc/measurements/). Complete-path costing is still
heuristic; skew, correlated filters, index-aware time estimates, and realistic
network latency remain further work.

Other follow-ups for ordinary federation are:

1. Measure batched lookups and scans with realistic network latency and
   default query budgets.
2. Add domain mappings and further safe temporal/enum SQL delegation.
3. Measure captured relations and investigate bounded alternatives for larger
   views, partitioned parents, and RLS datasets.
4. Add foreign-key discovery, extend unused-variable pruning in the shared
   compiler, and evaluate further SQL delegation with the core SPI.

These are follow-up work, not current capabilities. The existing integration
suite checks correctness and failure handling on small fixtures; it does not
establish production throughput, large-data behavior, or a multi-version
PostgreSQL compatibility matrix.

## Discover, accept, and query

For a table such as:

```sql
CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer_id bigint,
  status text,
  amount numeric(18, 4)
);
```

Declare a stable source UUID and explicitly choose the attributes to expose:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.postgres :as postgres])

(def profile
  {:compute-context-id :profiles/sales
   :jdbc-url (System/getenv "SALES_JDBC_URL")
   :properties {"user" (System/getenv "SALES_DB_USER")
                "password" (System/getenv "SALES_DB_PASSWORD")}})

;; Discovery performs catalog reads; acceptance and source construction do no I/O.
(def catalog (postgres/discover profile ["public" "orders"]))
(def accepted
  (postgres/accept-mapping
    catalog
    {:source-instance-id #uuid "56627a38-cdb4-44cd-b811-8663b2713621"
     :relation-id :sales/orders
     :identity-attribute :order/id
     :attributes {:order/customer-id "customer_id"
                  :order/status "status"
                  :order/amount "amount"}}))

(def orders (postgres/source accepted profile))

(d/q '[:find ?id ?amount
       :in $orders
       :where [$orders ?order :order/id ?id]
              [$orders ?order :order/status "paid"]
              [$orders ?order :order/amount ?amount]]
     orders)
```

The three patterns can use one fused access. It checks `status = 'paid'` and
requires a non-null amount, then returns the key and amount from the same row.
Shared query equality is rechecked after decoding. `d/explain` shows the covered
patterns, advisory row counts, and actual cursor requests; time and transfer
estimates remain unknown.

Native records can supply selective keys to PostgreSQL. With a native database
containing `:triage/order-id` and `:triage/priority` long attributes:

```clojure
(d/q '[:find ?id ?amount
       :in $ $orders
       :where [?t :triage/order-id ?id]
              [?t :triage/priority ?priority]
              [(>= ?priority 9)]
              [$orders ?order :order/id ?id]
              [$orders ?order :order/amount ?amount]]
     native-db orders)
```

## Identity, types, and absence

Bound `not` and `not-join` fact bodies work over external sources. For example,
this query returns existing orders whose note is absent:

```clojure
(d/q '[:find ?id :in $p
       :where [$p ?e :order/id ?id]
              (not [$p ?e :order/note _])]
     orders)
```

The identity fact establishes that the row exists. An input entity reference
alone can also name a missing row, whose attributes are all absent. Use an
explicit identity or other positive fact when row existence matters.

Absence checks complete their positive fact bodies in the query's acquired
source sessions, then subtract matches using the engine's bounded operator.
They preserve `not-join` locals and share cancellation, memory and request
limits with positive reads. Failed or incomplete reads never prove absence.
External negative bodies currently admit positive fact patterns; external
negative recursion, nested negation and other body operators remain unsupported.

Exact relational plans can push a bound scalar absence into PostgreSQL as
`IS NULL`, retaining a positive row witness. Composite mappings, captured views,
and other shapes use ordinary fact lookup and subtraction. SQL NULL still
produces no EAV fact.

Nullable outputs can preserve a matching row by binding `nil` when its
attribute is absent. Declare SQL-style NULL handling for selected extrema with
`:ignore-nil-extrema`:

```clojure
(d/q '{:find [(min ?amount)] :in [$p]
       :where [[$p ?order :order/id _]
               (or-join [[?order] ?amount]
                 [$p ?order :order/amount ?amount]
                 (and (not-join [?order] [$p ?order :order/amount _])
                      [(ground [nil]) [?amount]]))]
       :query-source {:ignore-nil-extrema #{?amount}}}
     orders)
```

Each declared `min`/`max` ignores only its own nil inputs and returns nil for
an all-NULL group. Other selected columns retain every matching row, and other
aggregates keep their existing behavior. Ordinary Datalog extrema preserve
their original nil handling. Tuple binding in the default branch binds nil as
a value; scalar functions returning nil filter their row.

Eligible scalar projections and extrema delegate to PostgreSQL without an
`IS NOT NULL` filter on optional columns. Text extrema still use explicit
`:native-extrema` declarations for PostgreSQL ordering. The scoped fact,
absence and tuple operators provide the same semantics when SQL delegation
is unavailable, including captured views. Nullable predicates and other
unproven shapes retain core execution. The JOB translator generates these
projections for nullable output columns, retaining all original independent
minima and NULL-bearing tuples.

Ordinary tables and partitioned parents require a validated, nondeferrable
primary key. Views and materialized views require explicit `:key-columns`,
validated for uniqueness and non-null values at every acquisition. Composite
keys are supported. Identity is nominal:
the source UUID, schema/table names, and `:identity-version` (default `0`)
identify its domain. The identity attribute returns a scalar for one key column
or a tuple in primary-key order for a composite key. Entity positions contain
`EntityRef` values, distinct from native database entity IDs.

Entity references normalize JVM `Byte`, `Short`, and `Integer` key values to
`Long`, matching the shared `:long` query-key contract. This includes composite
keys and reference attributes, so caller-created keys use the same identity as
keys decoded from PostgreSQL in delegated and ordinary accesses. See the
[experimental codec notes](../core/test/fixtures/README.md) for canonical wire
encoding and previously encoded narrow-integer references.

Allocate and persist your own source UUID for each PostgreSQL database.
Increment the accepted identity version if a replacement table reuses keys
for different entities. Acquisition detects a dropped/recreated table by its
catalog OID; it never silently accepts a replacement.

| PostgreSQL type | Logical value |
| --- | --- |
| `smallint`, `integer`, `bigint` | `:long` / JVM `Long` |
| `boolean` | `:boolean` |
| `text`, `varchar` | `:string` |
| `uuid` | `:uuid` |
| `bytea` | `:bytes`, copied into owned batches |
| `numeric` | `:bigdec`, preserving decimal scale |
| `real`, `double precision` | `:float`, `:double` |
| finite `timestamptz` | `:data` / `java.time.Instant`, preserving microseconds |
| finite `date` | `:data` / `java.time.LocalDate` |
| finite `timestamp without time zone` | `:data` / `java.time.LocalDateTime` |
| `time without time zone` | `:data` / `java.time.LocalTime` |
| `time with time zone` | `:data` / `java.time.OffsetTime`, retaining the stored offset |
| finite `interval` | `:data` / `{:months long :days long :microseconds long}` |
| enum | `:string` / exact catalog label; enum type OID, namespace, name, and ordered labels are rechecked |

Key columns may be integer, text/varchar, UUID, enum, or the listed date/time
representations. Numeric and interval keys remain unsupported. Unsupported
columns remain visible in discovery but cannot be mapped. Numeric, date,
timestamp, and interval infinities and SQL `24:00` fail execution when decoded.
Intervals retain calendar months and days separately from elapsed microseconds;
they are never approximated as a Java `Duration`. Each
mapped attribute is scalar (`:one`); SQL null contributes no fact, including
when a query uses `_` or omits that value from its results.
An ordinary conjunction over a nullable attribute therefore excludes rows
where that attribute is null; it does not produce SQL outer-join rows with a
null placeholder.

A reference attribute may use `{:columns ["tenant" "customer_id"] :reference
{:domain customer-domain :type customer-entity-type}}` instead of a column
name. The reference type must have matching ordered key types. A null component
makes the reference absent. This declares a nominal relationship; it does not
promise that the target row exists.

## Snapshot and execution guarantees

Each query acquires a connection and a read-only, repeatable-read transaction.
Sources with the same source UUID and compute-context ID share that connection
and snapshot, including mappings of different tables. Use that compute-context
ID only for compatible connection/authorization profiles. Independent databases
do not share a distributed snapshot.

Acquisition takes access-share locks and rechecks the accepted table, key, and
used column metadata. Changes to used types or constraints fail as stale
definitions; unrelated added columns do not invalidate a mapping. Locks remain
until rollback at session close. Concurrent data writes remain permitted, and
subsequent cursor opens continue to observe the original snapshot. Long queries
hold the snapshot and locks until completion, which matters for vacuum and
schema changes. A lost connection fails the query; there is no reconnect or
automatic retry.

SQL uses quoted identifiers and bound parameters. Equality lookups use SQL as
a superset filter and recheck Datalevin equality, including decimal scale and
byte content. Eligible integral, decimal, and temporal comparisons can be
pushed down. Unsupported predicates and scoped programs run in Datalevin. Fusion preserves
existence guards, repeated variables, and the identities needed by `:with`.
Repeated variables use join-key equality: `1.0M` and `1.00M` remain distinct
with or without fusion. Literal constants and explicit `=` predicates retain
numeric equivalence.

Ordinary tables use `ONLY`, excluding inheritance children. Partitioned parents
include all attached partitions. Arrays, JSON mapping, arbitrary SQL declarations,
and collection expansion remain outside the supported scope.

## Views, partitions, and row-level security

Accept a view or materialized view with an explicit business key:

```clojure
(postgres/accept-mapping
  (postgres/discover profile ["reporting" "orders"])
  {:source-instance-id source-uuid
   :relation-id :reports/orders :identity-attribute :report/id
   :key-columns ["tenant_id" "order_id"]
   :attributes {:report/total "total"}})
```

The declaration promises that those keys identify the same entities over time.
Acquisition checks every captured row for null or duplicate keys, even when the
query only needs a selective subset. A declared view key is not inferred from a
unique index. Update `:identity-version` when a view replacement changes entity
meaning. Discovery fingerprints view definitions, owners, options, nested
relation/policy definitions, and catalog-visible function dependencies; drift
requires explicit rediscovery and acceptance. Dependencies hidden inside dynamic
SQL or function bodies remain the profile owner's compatibility responsibility.
Materialized views must be populated.

With table-level privileges, acquisition first locks ordinary tables, partitioned
parents, and regular views without taking a data snapshot. It then issues zero-row
reads to lock materialized views, including dependencies reached through regular views, before checking the
accepted catalog. A column-type change committed before its table lock is acquired
fails as `:query-source/stale-definition`. A physical rewrite between snapshot
creation and a later materialized-view lock fails as `:query-source/snapshot-lost`
before any capture is published. The guard compares snapshot-visible storage
generations with the current locked relations. A refresh completed before the
snapshot, or an MVCC-safe concurrent refresh, does not trigger that guard. Lock
acquisition and storage checks share the original authorized transaction.

Column-level `SELECT` grants on mapped attributes and identity columns are also
sufficient. If explicit locking lacks permission, acquisition rolls back its
lock attempts to a savepoint and locks the group through zero-row reads of just
those columns. Unmapped columns require no grant. Missing access to a mapped
column still fails as `:query-source/unauthorized`; underlying privileges for
security-invoker views follow PostgreSQL's normal rules.

This fallback checks ordinary-table storage generations too, then validates the
accepted schema and mapped column identities against a fresh snapshot before
publishing the session. Rewrites, type changes, and a drop/add of a same-name
column cannot silently use an older catalog snapshot. It retains the original
data snapshot for SQL cursors.

Before publishing a session, acquisition revalidates every accepted security
scope against a fresh catalog snapshot taken after all captures. Mismatches in
the effective role, inherited memberships, or RLS enforcement fail as
`:query-source/unauthorized`, including grants of `BYPASSRLS` during acquisition.
This check also runs for policies and views with no referenced functions and
readers with full table SELECT permission.

Acquisition also checks referenced function catalog row versions in that fresh
snapshot. Function changes during capture fail as `:query-source/stale-definition`,
including a replacement restored to its original body before verification. SQL
text and function bodies stay in PostgreSQL. The version check uses the same
catalog-visible dependency set as the accepted function fingerprint, including
functions in nested views and policies.

When all relations are captured, the adapter reuses its connection for this fresh
metadata check; captured rows retain their original data snapshot. Sessions that
still require SQL reads use a second read-only connection for security-scope,
function, or column-grant fallback verification, keeping their original
transaction. Their pool must permit that simultaneous checkout. The validation
connection reserves another 8 MiB
from the shared memory budget, obeys query/connection deadlines and request limits,
and closes before session publication. Cancellation or verification failure
releases the connections and retained capture reservations.

Acquisition phases sharing a DataSource instance use fair admission before the
first data checkout, held through post-capture verification. Concurrent
acquisitions cannot each occupy a data connection while waiting for another
connection to validate. Ready sessions execute outside admission, and unrelated
DataSources have independent admission. Reuse the same DataSource instance in
profiles backed by the same pool. A mixed session still needs capacity for its
data connection and a temporary validation connection.

Views, materialized views, partitioned parents, and RLS tables capture the
accepted projection once per session, before planning fact accesses. Each
non-key column contributes its value when it fits the cap, or an exact
size/presence marker when it is oversized. Oversized payloads stay in PostgreSQL;
the adapter retains no truncated value. Keys must fit the cap at acquisition.
A cursor can check a marked value's existence, while an access that needs its
payload fails as `:query-source/limit`. An access also checks the combined size
of the key and materialized columns, so splitting a captured row into individual
bounded columns does not bypass its payload cap.

Capture uses bounded server-cursor pages and shared request, retained-memory,
and deadline budgets. No session is returned after a partial capture or failed
key/conversion check. Capture freezes volatile view expressions and policy
results for identical replay, including presence and size markers. It can
transfer and retain substantially more data than a selective query returns.
Captured lookups build lazy hash indexes for bound entity, identity, and attribute
values, then reuse them across input batches and cursor opens in that session.
Each index scans the captured rows once; subsequent probes visit matching buckets
instead of rescanning every row for each key. Variable joins use query-key
equality, including decimal scale and byte-array contents; literal lookups use
constant equivalence. Every candidate still passes through the ordinary matcher.
Size markers remain residual candidates and cannot hide a payload-limit failure.

Index keys, table growth, buckets, row references, and bookkeeping reserve shared
retained memory before insertion. Admission also leaves 1 MiB for output pages.
If a complete index cannot fit, its partial state is released and that lookup
shape uses the scan path for the rest of the session. Index build visits and
candidate visits contribute to cursor `:read-rows`; a reused index has no build
visits. Predicates, cross-entity joins, and aggregates over captures still run in
core. Closing the session drops captured rows and indexes and releases their
reservations. Capture itself remains a full bounded read of the accepted projection.

For RLS, discover and reopen with the same authorization context:

```clojure
(def tenant-profile
  (assoc profile :role "application_reader"
                 :session-settings {"app.tenant_id" "tenant-a"}))
```

`:role` uses transaction-local `SET ROLE`; `:session-settings` applies up to 32
custom dotted settings with string values, also transaction-local. Identifier
and value quoting prevents them from becoming SQL. `row_security` is explicitly
on. RLS acceptance requires `row_security_active` under the effective role;
superuser, `BYPASSRLS`, and owner bypass are rejected. Owners using `FORCE ROW
LEVEL SECURITY` can be accepted when enforcement is active. Empty policies can
produce an empty accepted relation.

Persisted security scopes contain role, membership/context identifiers, and
setting fingerprints, without setting values or credentials. The scope also
participates in nominal entity identity, so different tenant scopes cannot
alias tenant-local view keys. A role, membership, or configured-setting mismatch
fails acquisition as unauthorized; policy/definition drift is stale metadata.
Keep every policy-relevant custom setting explicit in `:session-settings` and
use a compute-context ID only for compatible database, pool, role, settings, and
function behavior. Arbitrary policy functions can depend on external state that
catalog discovery cannot infer.

View security follows PostgreSQL's declared behavior: ordinary views use owner
permissions for base relations, while `security_invoker` views use the caller.
An owner view over an RLS table can therefore expose rows the caller cannot read
directly. The adapter does not reinterpret that database authorization contract.
Both kinds capture under the configured profile and retain a restricted scope.

## Filter and relational delegation

Ordinary binary `=`, `not=`, `<`, `<=`, `>`, `>=`, `like`, `not-like`, `in`, and `not-in` predicates are exported by
core only within their conjunction and source-variable scope. Captured scalar
`:in` bounds may become constants; potential outputs from another operator do
not become constants. The adapter accepts matching integral/decimal bounds
and `Instant`, `LocalDate`, and `LocalDateTime` bounds from years 0001 through
9999, plus `LocalTime` comparisons. `OffsetTime` equality uses a SQL superset
with local offset-aware rechecking; its ordering and interval comparisons stay
in core. Other representations remain
residual predicates. Temporal cutoffs retain nanosecond comparisons against
PostgreSQL's microsecond values by adjusting the bound and strictness before
binding. No rounding through `java.util.Date` occurs. Pattern and batched-key
equality also support eligible `Instant` values.

Ordinary `text` and `varchar` fact accesses push binary `like`/`not-like` calls
when the pattern is a literal or captured scalar containing only ASCII
characters. `%` and `_` still match Unicode input by code point, including
newlines. SQL uses C collation and an explicit `!` escape to preserve the native
case-sensitive matcher. Escaped `%`, `_`, and `!` keep their literal meaning;
an unescaped trailing `!` is ignored, matching the native behavior. Patterns
are bound parameters. The original core predicate remains a residual check.

Non-ASCII or NUL-containing patterns, invalid escapes, custom escape/options
arguments, dynamic patterns, reversed operands, and scalar output bindings
retain local execution. Invalid patterns retain their existing local errors.
Captured views, partitioned parents, and RLS sources retain their captured-row
behavior. This is fact-access prefiltering; the complete relational compiler's
eligibility rules remain unchanged. Budgeted EXPLAIN probes can estimate these
filters before driver selection; declined probes retain catalog cardinalities.

Text `in`/`not-in` filters can execute inside relational and dependent join
plans. Constant or captured collections contain at most 256 text/nil members;
SQL uses C collation and bound parameters. Nil members cannot match present text
facts; empty collections retain their native membership behavior. Other
collections stay local. Ordinary single-table fact alternatives do not yet
apply membership prefilters.

For example, an actor access can supply typed person references to a cast/title
join, then supply movie references to a movie-keyword/keyword join with `IN`.
Input ordinals preserve binding associations; original core predicates remain
residual checks. Joined steps report their participating `:covered-sources`;
their `:source` identifies the session coordinator. Current joined plans require
an explicit boundary key and stay within the root conjunction.

The full IMDb [actor-filter probe](../doc/measurements/postgres-like-actor-filter-2026-11-09.edn)
for JOB 6d's `%Downey%Robert%` pattern returns exactly two rows through the
adapter, matching SQL, at a 314 ms median across seven invocations. An equivalent
local-only call with explicit nil options exceeded that run's 64 MiB budget.
This measures the actor access, not the complete 6d query. The existing
[6a regression](../doc/measurements/postgres-job-6a-like-256m-2026-11-09.edn)
still verifies the complete answer at 256 MiB and takes about 2.18 s versus
24.5 ms native; LIKE pushdown alone has not reached the performance milestone.

Flat scalar arithmetic (`+`, `-`, `*`, `/`, `quot`, `rem`, `mod`, `inc`, and
`dec`), `like`/`not-like`, and `in`/`not-in` execute locally when SQL delegation
does not cover them. They support predicates and scalar output bindings and can
feed later filters, joins, and aggregates, including in mixed native/PostgreSQL
queries. Decimal results keep native scale and math-context behavior; membership
uses native Clojure set semantics. For example:

```clojure
[:find ?order ?adjusted :in $p :where
 [$p ?order :order/status ?status] [(in ?status ["paid" "held"])]
 [$p ?order :order/amount ?amount] [(+ ?amount 1M) ?adjusted]]
```

Arithmetic scratch, compiled patterns, and membership indexes use the shared
memory budget. Constant patterns and collections are prepared once per scalar
step; dynamic ones release scratch after each call. These expressions can still
require fetching matching fact values. Nested calls, arbitrary host functions,
and collection output bindings remain outside this bounded scalar path.

The optional relational path delegates a complete flat conjunction when every
source belongs to one acquired PostgreSQL session and all expressions have
proven SQL semantics. It supports self joins, cross-table joins, and typed
references. SQL uses binary text equality and preserves decimal scale where
query-key equality requires it. Unsupported types and expressions decline
delegation before acquisition and use the existing fact-access path.

Whole-query SQL is the default candidate across the supported graph size,
including text filters and explicit native text extrema. The former eight-table
text restriction is removed. Eligible filters, joins, distinctness, grouping,
and aggregation execute together on PostgreSQL, returning final result rows.
This is one data query; snapshot and catalog validation retain their own bounded
requests.
The source aliases must share one source-instance/compute-context group and
compatible authorization scopes; matching host names alone is insufficient.

This follows the same locality principle as
[`postgres_fdw` remote joins](https://www.postgresql.org/docs/18/postgres-fdw.html#POSTGRES-FDW-REMOTE-QUERY-OPTIMIZATION):
PostgreSQL can ship joins within one foreign server, retaining individual reads
when they are expected to cost less and keeping different user mappings separate.
Its [upper-path callback](https://www.postgresql.org/docs/18/fdw-callbacks.html#FDW-CALLBACKS-UPPER-PATHS)
also supports remote aggregation. Our SQL compiler additionally proves the
Datalevin value and aggregate semantics before accepting a plan.

The automatic policy retains dependent MIN/MAX plans for two bounded structural
shapes: a selective text driver paired with membership filters, and a selective
text driver feeding several identity-only existence lookups. Graphs larger than
eight table/entity groups require at least two independent text drivers on leaf
dimensions, with at most twelve groups. A filter on a bridge carrying outgoing
references does not count as a selective dimension driver. Existence probes
require at most eight groups and an anchored
prefix or at least four literal pattern characters; a broad substring such as
`%B%` alone does not establish a selective driver. These include the measured
6d, 10c, 17c, 17d, and 20b paths. The policy is a cheap graph heuristic, rather
than an elapsed-time or selectivity estimate. It uses no query IDs or stored
answers. Queries with nullable projections or absence checks prefer complete
SQL instead of these exceptions.

Bound scalar AND/OR filters can execute with the joins and aggregation when
every branch uses already bound fact values and supported scalar comparisons.
The planner bounds the logical forest to 128 nodes and depth 16, with at most
32 branches per OR. Chained comparisons, including string BETWEEN predicates,
normalize to adjacent comparisons; PostgreSQL still checks each leaf's types,
constants, NULL behavior, and collation. Branches that introduce facts, functions,
negation, rules, or optional nullable values keep core execution. Overlapping
Boolean branches filter a witness once and do not duplicate aggregate inputs.

Set `:whole-query-mode` consistently on the participating runtime profiles:

| Mode | Selection |
| --- | --- |
| `:auto` (default) | Attempt complete SQL, preserving the selective graph exceptions |
| `:sql` | Prefer complete SQL whenever the compiler proves it eligible |
| `:dependent` | Keep whole-query execution in core; retain filtered and dependent join pushdown |

The existing `:relational? false` setting disables relational delegation in every
mode. SQL preference never bypasses type, collation, source, authorization, or
expression checks. Host functions and unsupported scoped branches still use
core execution. `d/explain` reports `:policy :relational-v1` for a selected
whole-query plan; `{:run? false}` acquires metadata without executing its data
query. Use the mode override to compare complete SQL against dependent plans on
your workload, including connection and planning time.

Supported aggregates are `count`, `count-distinct`, decimal `sum`, integral
`sum` with an explicit nonnegative range predicate, and eligible scalar
`min`/`max`. Decimal min/max require a fixed column scale; floating-point
aggregation, unrestricted integral sums, and other reducers remain in core.
The SQL projection establishes distinct `:find`/`:with` input tuples before
grouping. All-MIN/MAX projections omit this intermediate DISTINCT because both
extrema and validation flags are insensitive to duplicates. Mixed reducers keep
the distinct witness relation. Empty inputs yield no groups, matching ordinary
query behavior.
Input conversion guards still apply when aggregation returns a small value.
The row-size guard applies to the final SQL projection: values used only inside
PostgreSQL for joins, filters, distinctness, or aggregation do not consume that
wire payload cap.

Scalar aggregation without grouping or a result window is proven to return at
most one row. It uses a direct SELECT with JDBC fetch size zero, permitting
PostgreSQL parallel plans while retaining a one-row decoder limit. This avoids
the parallelism restriction of
[`DECLARE CURSOR` and nonzero protocol fetch counts](https://www.postgresql.org/docs/18/when-can-parallel-query-be-used.html).
Grouped and unbounded results retain owned, bounded cursor pages. The same
snapshot, cancellation, output-size, and reservation rules apply to both forms.

Large SQL join graphs preserve numeric
alias creation order rather than sorting `r10` before `r2`, which would change
the input to PostgreSQL's bounded join search. Whole-query SQL uses a comma
`FROM` list so that an explicit `CROSS JOIN` tree does not accidentally constrain
large graphs through
[`join_collapse_limit`](https://www.postgresql.org/docs/18/runtime-config-query.html#GUC-JOIN-COLLAPSE-LIMIT).
Key-dependent LATERAL fragments keep their deliberate planning fences.

The initial relational contract accepts up to 32 patterns and 128 physical
output components. Exact scalar absence and eligible LIKE unions can delegate.
Other scoped programs, functions, `:having`, collection inputs, and queries
spanning independent sessions retain core execution.
Public result shaping uses the shared result processor; ordering combined with
return maps currently retains core execution. Finite relational windows use a
distinct top-k collector that retains at most `offset + limit` candidate rows,
charges their memory, and releases replaced candidates. The same collector
bounds finite mixed-fragment results. An offset increases the retained prefix.
Unlimited results still require full collection under the shared memory ledger;
this path does not spill core rows to disk.

Core fallback supports relational `:order-by` and `:having` for captured views,
mixed native/PostgreSQL queries, and other shapes that decline delegation.
HAVING evaluates the supported scalar predicates over projected aggregates
after grouping, before sorting, offset, and limit. The fallback also supports
`count-distinct`; return maps sort using their projected column values.
For example, a mixed-source spending query can add:

```clojure
:having [(> (sum ?amount) 100M)]
:order-by [1 :desc ?name :asc]
:limit 20
```

Fallback joins and aggregation still materialize their inputs under the shared
memory budget. A small result limit does not bound those upstream rows or
PostgreSQL transfer. Sorting and HAVING honor cancellation and deadlines, and
exceeding a quota fails the query instead of returning a partial ranking.

The optional `IRelationalWindowSource` extension lets PostgreSQL apply a final
distinct projection, ordering, offset, and limit before returning rows. It
also covers eligible single-entity queries such as:

```clojure
(d/q '[:find ?id ?stamp
       :in $orders
       :where [$orders ?e :order/id ?id]
              [$orders ?e :order/stamp ?stamp]
       :order-by [?stamp :desc ?id :desc]
       :limit 20]
     orders)
```

Integral, decimal, boolean, and `Instant` sort keys have established SQL ordering.
String, UUID, byte-array, and nominal-reference ordering stays in core; for
example, PostgreSQL C collation orders UTF-8 while Java strings compare UTF-16.
For those orders the bounded collector reduces retained memory but still reads
the delegated result. Dynamic scalar limits/offsets resolve before planning.
SQL windows preserve find/with distinctness, original decimal scales, grouping,
and conversion/row-size errors even outside the returned window. Ties can select
any matching boundary rows; include a unique final sort key for a unique window.

The 50,000-row latest-orders regression returns and decodes only 20 rows within
a 16 MiB query budget. This bounds client transfer and retained result rows;
PostgreSQL can still scan, group, validate, and sort a larger input, using its
own memory/spill policy and statement timeout. `d/explain` includes a structural
`:result-window` on accepted whole-query window plans: column positions,
directions, offset, and limit, without SQL or data values.

Set `:relational? false` on participating runtime profiles to compare the
fact-access path. `d/explain` identifies selected relational plans with
`:policy :relational-v1`; raw SQL and parameters remain private.

## Delegation inside mixed-source queries

The shared compiler can isolate a flat PostgreSQL portion when the other
patterns read native Datalevin databases or owned local facts. For example:

```clojure
(d/q '[:find ?name (sum ?amount)
       :with ?order
       :in $customers $orders
       :where [$customers ?customer :customer/id ?id]
              [$customers ?customer :customer/name ?name]
              [$orders ?order :order/customer-id ?id]
              [$orders ?order :order/amount ?amount]]
     native-customers orders)
```

Core compiles the native prefix through its ordinary shared operators and pins
its native views for the invocation. It exports distinct typed boundary keys to
PostgreSQL in bounded `VALUES` batches. The SQL portion can join multiple accepted
mappings in the same session and apply its supported predicates and aggregates.
Opaque group/witness IDs carry native grouping and `:with` multiplicity; native
names and unrelated payloads do not need to enter SQL. Repeated amounts, duplicate
native witnesses, and customers sharing a name retain the ordinary find/with
distinctness rules. As with ordinary Datalog aggregation, `:with ?order` is needed
when equal spending amounts from different orders should contribute separately.

A spending fixture with 300 selected customers and 30,000 order/item rows returns
300 totals through two cursor opens under the default binding limits. This is a
transfer/count regression, not a production throughput benchmark.

Every SQL batch shares the single acquired authorization context and repeatable
read snapshot. Boundary equality uses query-key semantics, including original
decimal scale, rather than PostgreSQL's numeric equivalence alone. All inputs,
pages, native intermediate rows, and public results use the shared resource
ledger. Cursor closure drops owned binding buffers. Plan-only explain compiles
the prefix and validates the SQL session without opening a data cursor.
`d/explain` marks the path with `:policy :relational-fragment-v1`; it reports typed
boundaries and counts without exposing SQL or parameter values.

This first fragment path accepts one PostgreSQL session group plus native/local
sources, a shared boundary variable, and a flat conjunction of patterns and
supported predicates. Aggregate arguments must belong to the SQL portion.
Selectors support integral, string, decimal, UUID, boolean, and byte-array values;
unsupported types, functions, scopes, cross-boundary residual expressions, or
incompatible sessions retain the existing compiler path. Captured views,
partitioned parents, and RLS sources continue to use their local captures.

A fragment open admits at most 256 bindings and 2 MiB, further limited by the
query's `:binding-rows`/`:binding-bytes`. Core packs complete native aggregate
groups into batches; it never splits a group and combines partial aggregates.
If a complete group cannot fit, execution falls back before SQL acquisition.
This can happen for a global total over more than 256 selected keys or many
customers sharing one projected name. Plan-only explanations mark the choice
`:conditional-inputs true` because actual group sizes are checked at execution.
Set `:relational? false` on participating profiles to compare ordinary fact reads.

## Bounds and lifecycle

Runtime profiles accept either `:jdbc-url` plus optional string `:properties`,
or a caller-owned JDBC `:data-source`. Connections must be returned in
autocommit mode; the adapter rolls back and closes each acquired connection,
returning pooled connections without closing the pool. Pool checkout is
bounded, and a connection arriving after cancellation is closed.
Admission waits honor cancellation and query deadlines, perform no checkout or
SQL, and share one `:connect-timeout-ms` budget with the initial checkout.
Validation checkouts have their own bounded wait within the invocation deadline.
Idle admission entries are removed rather than retaining caller-owned pools.

| Option | Default | Meaning |
| --- | --- | --- |
| `:connect-timeout-ms` | `10000` | Maximum admission plus initial-checkout wait; also bounds each validation checkout |
| `:statement-timeout-ms` | `10000` | PostgreSQL statement timeout |
| `:fetch-size` | `128` | Maximum rows per explicit server-cursor fetch; up to 256, reduced when needed to admit cursor workspace |
| `:max-row-bytes` | `16384` | Maximum combined SQL text size of materialized values and keys per fact-access row, or the final returned row for delegated SQL |
| `:fusion?` | `true` | Set false to compare separate EAV accesses |
| `:relational?` | `true` | Allow eligible whole-conjunction joins and aggregation |
| `:whole-query-mode` | `:auto` | Prefer whole-query SQL automatically, or select `:sql` / `:dependent` for workload comparisons; semantic checks always apply |
| `:statistics?` | `true` | Read session-local catalog cardinalities for fact-access ordering |
| `:filtered-statistics?` | `true` | Allow cost-budgeted EXPLAIN row estimates before pure access planning; requires `:statistics?` |
| `:role` | unset | Transaction-local PostgreSQL role |
| `:session-settings` | `{}` | Explicit policy/view context: custom dotted GUC names mapped to strings |

Use `_` or an omitted value position for existence checks:

```clojure
(d/q '[:find ?id :in $p :where
       [$p ?e :row/id ?id]
       [$p ?e :row/large-payload _]] source)
```

Ordinary fact reads select non-null presence flags for ignored values and keep
all component guards for references. A field also needed by another clause is
materialized once and remains subject to the cap. Null is absent; empty text and
empty byte arrays are present. Conversion checks still reject unsupported
non-finite temporal/numeric values, even in existence-only patterns.

A named value variable can still require materialization for joins, predicates,
`:with`, or intermediate fact-access output, even when absent from `:find`.
Eligible delegated SQL avoids transferring fields absent from its final
projection. Use `_` for an explicit existence-only pattern on any execution path.

Oversized payloads produce an error or size marker before their values cross
the network; values are never silently truncated. Reserved workspace includes
the fetch page, decoding, and pending row; returned batches have separate charges.
Output batches use the requested byte allowance (1 MiB by default), shrinking
when the shared retained-memory budget cannot admit it. They no longer have an
additional 64 KiB window. Row limits, byte limits, cancellation, and ownership
checks still apply to every batch.
The query's `:timeout` and `:query-source :limits` apply across all accesses.
Cancellation aborts blocked database I/O and releases owned resources.

The default retained-memory allowance is 1 GiB per invocation. It includes
provider state, intermediate joins, and results; callers can override it through
`:query-source {:limits {:retained-bytes ...}}`.

The default query budget allows 1,024 external requests. At 128 rows per fetch,
a scan returning 100,000 rows needs 782 fetch requests, plus setup and cursor
declaration. A thousand distinct lookup keys can share four SQL cursors when
the bindings fit the byte cap. Smaller fetch settings still increase request
pressure: at 32 rows per fetch, that scan needs at least 3,125 fetch requests.
More accesses, aliases, or other sources share the same query budget. These
counts describe requests, not throughput or backend work. Result
materialization has separate memory charges; fitting the request budget does
not guarantee that the whole query fits the memory budget.

For larger workloads, inspect executed `d/explain` metrics and adjust both the
profile's `:fetch-size` and the query's `:query-source :limits` deliberately.
Larger fetches reserve more memory. Increasing `:max-row-bytes` also increases
the reserved fetch workspace for variable-width fields: a 1 MiB row cap with
128 rows per fetch would require over 2 GiB of cursor workspace. Before SQL
runs, the adapter reduces the fetch page until its reservation fits the shared
memory budget. It still fails if even one row's workspace cannot be admitted.
Pages selecting only fixed-width physical scalars and presence flags reserve a
smaller workspace based on those types. Captures reserve up to one capped value
per mapped column, plus size markers, and can need smaller pages than ordinary
fact reads. Smaller admitted pages can increase request pressure;
inspect both request and memory limits when accepting larger fields.

Dependent inputs use bound parameters and a generated ordinal to correlate
each match with its binding ID. Composite keys and typed references retain
their key components; every match rechecks Datalevin equality. When a key has
no safe SQL equality filter, its batch uses local matching and can still scan
the underlying table. Key batching and whole-conjunction relational delegation
have separate eligibility checks.

Connection setup, catalog reads, cursor declarations, and fetches consume the
shared request budget. Resource cleanup (`CLOSE`, rollback, connection close)
runs even after budget exhaustion. Cursor metrics report decoded rows, output batches, requests, and
elapsed time. Wire byte counts are unknown. `postgres/statistics` reports
acquisition requests and live sessions separately.

## Save mappings and test

Persist `accepted` with the existing [definition installer](../README.md#persisted-definitions),
using source provider `:postgres` and a runtime profile reference. After
`install/load-definitions`, pass the saved relation's `:accepted` value to
`postgres/source` with runtime credentials. Refresh explicitly by discovering
and accepting new metadata, then installing it with the expected definition
version. No credentials, connections, or rows belong in saved definitions.

The default federation suite includes pure mapping/planning tests. Real-driver
checks require a disposable PostgreSQL database and create/drop uniquely named
test schemas:

```sh
export POSTGRES_TEST_URL='jdbc:postgresql://127.0.0.1:5432/federation_test?user=test'
clojure -M:test:postgres
```

The [integration suite](test/datalevin/federation/postgres_integration_test.clj)
compares fused, separate, and eager results, runs the shared scan/lookup checks,
and exercises native joins, rules, generated data, exact types, composite keys,
references, saved definitions, concurrent writes, drift, quotas, pool return,
late checkout, blocked fetch cancellation, and snapshot loss. Large-field tests
cover presence flags, composite references, empty/null values, dependent binding
IDs, captured replay, combined caps, and final SQL output limits. Scaling checks
exercise 1,200 dependent keys through ordinary queries and stream 100,000
rows under the default request and memory limits. The fault checks
need permission to terminate their own test connection.
