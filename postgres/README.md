# PostgreSQL relations

`org.clojars.huahaiy/datalevin-federation-postgres` queries PostgreSQL tables and accepted views
through ordinary `d/q` and `d/explain`, alongside native Datalevin data and other
adapters. It supports table scans, entity and value lookups, and same-entity
fusion: several scalar attributes of one entity can be read by one SQL cursor.
Dependent lookups combine up to 256 input keys in a parameterized `VALUES`
join, with results associated with their original binding IDs.
Eligible decimal/time filters and complete same-snapshot relational queries
can execute in PostgreSQL, including joins between accepted mappings and
supported aggregates.

This development snapshot requires the matching **Datalevin 1.2.10** build
with the optional whole-query and mixed-source fragment SPI extensions. The module declares `1.2.10` as a
provided dependency; the development workspace uses the sibling checkout.
SPI revision 2 alone does not establish availability of the new protocols.

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
| Enrich large key sets | Batches accept at most 256 keys and 256 KiB of bindings; matches arrive in bounded fetch pages | Request and memory budgets apply across all batches. Network latency and nonunique value matches can still dominate. |
| Filter orders by amount or events by time | Matching integral, `BigDecimal`, and `Instant` comparisons are pushed down; mixed representations and arbitrary functions stay local | Eligible ranges and equality reduce transfer. Bounds outside the supported timestamp parameter range and unsupported comparisons can still require a broad read. |
| Read an existing application schema | Temporal types and enums are supported; domains, `char`, `citext`, arrays, and JSON/JSONB remain unmapped | Supported columns can be selected independently. Collection-valued attributes are not implemented. |
| Read reporting views, partitioned datasets, or tenant tables with RLS | Views need declared keys; partitioned parents need primary keys; RLS needs an enforced authorization profile | These relations capture all mapped columns at acquisition under shared row, request, and retention limits. Subsequent accesses use the captured rows; SQL predicate, join, and aggregate delegation is unavailable for these captures. |
| Aggregate, sort, or join large PostgreSQL relations | Whole-query and mixed-source fragment delegation cover eligible flat joins and aggregates; SQL ordering/limit, arbitrary reducers, and scoped fragments stay in core | Mixed native/SQL spending queries can return customer totals instead of every order. Complete aggregate groups must fit a binding batch; unsupported shapes retain fact accesses. Backend work remains subject to timeouts; no index discovery or cost-based access choice is implemented. |
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

Wide queries, including eleven attributes on one bound entity plus a second
entity clause, retain complete coverage under the default cap. If the cap
cannot hold even the reserved accesses, planning fails explicitly with
`:query-source/limit`. Increase the cap for that query, for example with
`{:query-source {:planning {:max-alternatives 64}}}`.

The highest-value follow-ups for ordinary federation are:

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
patterns and actual cursor requests; cost estimates remain unknown.

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

Ordinary tables and partitioned parents require a validated, nondeferrable
primary key. Views and materialized views require explicit `:key-columns`,
validated for uniqueness and non-null values at every acquisition. Composite
keys are supported. Identity is nominal:
the source UUID, schema/table names, and `:identity-version` (default `0`)
identify its domain. The identity attribute returns a scalar for one key column
or a tuple in primary-key order for a composite key. Entity positions contain
`EntityRef` values, distinct from native database entity IDs.

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

Ordinary binary `=`, `not=`, `<`, `<=`, `>`, and `>=` predicates are exported by
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

The optional relational path delegates a complete flat conjunction when every
source belongs to one acquired PostgreSQL session and all expressions have
proven SQL semantics. It supports self joins, cross-table joins, and typed
references. SQL uses binary text equality and preserves decimal scale where
query-key equality requires it. Unsupported types and expressions decline
delegation before acquisition and use the existing fact-access path.

Supported aggregates are `count`, `count-distinct`, decimal `sum`, integral
`sum` with an explicit nonnegative range predicate, and eligible scalar
`min`/`max`. Decimal min/max require a fixed column scale; floating-point
aggregation, unrestricted integral sums, and other reducers remain in core.
The SQL projection establishes distinct `:find`/`:with` input tuples before
grouping. Empty inputs yield no groups, matching ordinary query behavior.
Input conversion guards still apply when aggregation returns a small value.
The row-size guard applies to the final SQL projection: values used only inside
PostgreSQL for joins, filters, distinctness, or aggregation do not consume that
wire payload cap.

The initial relational contract accepts up to 32 patterns and 128 physical
output components. Rules, unions, negation, functions, `:having`, collection
inputs, and queries spanning independent sessions retain core execution.
Public result shaping, ordering, offset, and limit use the shared result
processor; ordering combined with return maps currently retains core execution.
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

A fragment open admits at most 256 bindings and 256 KiB, further limited by the
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

| Option | Default | Meaning |
| --- | --- | --- |
| `:connect-timeout-ms` | `10000` | Maximum connection/checkout wait |
| `:statement-timeout-ms` | `10000` | PostgreSQL statement timeout |
| `:fetch-size` | `128` | Maximum rows per explicit server-cursor fetch; up to 256, reduced when needed to admit cursor workspace |
| `:max-row-bytes` | `16384` | Maximum combined SQL text size of materialized values and keys per fact-access row, or the final returned row for delegated SQL |
| `:fusion?` | `true` | Set false to compare separate EAV accesses |
| `:relational?` | `true` | Allow eligible whole-conjunction joins and aggregation |
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
The query's `:timeout` and `:query-source :limits` apply across all accesses.
Cancellation aborts blocked database I/O and releases owned resources.

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
