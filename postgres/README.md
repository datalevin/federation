# PostgreSQL relations

`org.clojars.huahaiy/datalevin-federation-postgres` queries PostgreSQL tables and
views with `d/q` and joins them with native Datalevin data or other sources.
It supports scans, key and value lookups, and SQL delegation for eligible
filters, joins, and aggregates.

This development snapshot requires the matching **Datalevin 1.2.10** build with
relational query-source support. Follow the repository's
[setup instructions](../README.md#getting-started) to run these examples. The
module includes the PostgreSQL JDBC driver; your application supplies Datalevin.

## Discover, accept, and query

For a table such as:

```sql
CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer_id bigint,
  status text,
  amount numeric(18, 4),
  note text
);
```

Set `SALES_JDBC_URL` to a JDBC URL such as
`jdbc:postgresql://localhost:5432/sales`, and set `SALES_DB_USER` and
`SALES_DB_PASSWORD` for a role with SELECT access. Allocate and persist a source
UUID for this database, then choose the attributes to expose:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.postgres :as postgres])

(def source-uuid #uuid "56627a38-cdb4-44cd-b811-8663b2713621")

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
    {:source-instance-id source-uuid
     :relation-id :sales/orders
     :identity-attribute :order/id
     :attributes {:order/customer-id "customer_id"
                  :order/status "status"
                  :order/amount "amount"
                  :order/note "note"}}))

(def orders (postgres/source accepted profile))

(d/q '[:find ?id ?amount
       :in $orders
       :where [$orders ?order :order/id ?id]
              [$orders ?order :order/status "paid"]
              [$orders ?order :order/amount ?amount]]
     orders)
```

The query returns IDs and amounts for paid orders with a non-null amount. The
adapter can combine these patterns into one SQL access. Use `d/explain` to
inspect the plan, or `d/explain` with `{:run? true}` to include execution metrics.

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

<a id="identity-types-and-absence"></a>

## Identity and supported types

Ordinary tables and partitioned parents require a validated, nondeferrable
primary key. Views and materialized views require declared key columns, checked
for uniqueness and non-null values whenever a query acquires the source.
Composite keys are supported.

Entity positions contain `EntityRef` values, distinct from native Datalevin
entity IDs. The identity attribute returns a scalar for a single key column
or a tuple in primary-key order for a composite key. The source UUID, relation,
and `:identity-version` (default `0`) identify the entity domain. Increment the
version when a replacement relation reuses keys for different entities.
A dropped and recreated table requires explicit rediscovery and acceptance.

| PostgreSQL type | Logical value |
| --- | --- |
| `smallint`, `integer`, `bigint` | `:long` / JVM `Long` |
| `boolean` | `:boolean` |
| `text`, `varchar` | `:string` |
| `uuid` | `:uuid` |
| `bytea` | `:bytes` / byte array |
| `numeric` | `:bigdec`, preserving decimal scale |
| `real`, `double precision` | `:float`, `:double` |
| finite `timestamptz` | `:data` / `java.time.Instant`, preserving microseconds |
| finite `date` | `:data` / `java.time.LocalDate` |
| finite `timestamp without time zone` | `:data` / `java.time.LocalDateTime` |
| `time without time zone` | `:data` / `java.time.LocalTime` |
| `time with time zone` | `:data` / `java.time.OffsetTime`, retaining the stored offset |
| finite `interval` | `:data` / `{:months long :days long :microseconds long}` |
| enum | `:string` / exact catalog label |

Key columns may use integer, text/varchar, UUID, enum, or the listed date/time
types. Numeric and interval keys are unsupported. Domains, `char`, `citext`,
arrays, and JSON/JSONB are not mapped. You can select supported columns without
mapping the other columns in a table.

Numeric, date, timestamp, and interval infinities, and SQL `24:00`, fail when
read. Intervals preserve months and days separately from microseconds.
`timestamptz` uses `java.time.Instant`, while native Datalevin `:db.type/instant`
uses `java.util.Date`; joining them requires an explicit conversion and a choice
about precision.

Every mapped attribute is scalar. For a table with `tenant` and `customer_id`
columns, a composite reference mapping can use:

```clojure
{:order/customer
 {:columns ["tenant" "customer_id"]
  :reference {:domain customer-domain :type customer-entity-type}}}
```

The target type must declare matching key types in the same order. A null
component makes the reference absent. This declaration does not guarantee that
the referenced row exists; foreign-key relationships are not discovered
automatically.

## Nulls and absence

SQL NULL contributes no fact. An ordinary pattern on a nullable attribute
excludes rows where that attribute is null, even when its value is `_`.
Use a positive identity pattern and `not` to find existing rows with no value:

```clojure
(d/q '[:find ?id :in $orders
       :where [$orders ?order :order/id ?id]
              (not [$orders ?order :order/note _])]
     orders)
```

Bound `not` and `not-join` support positive fact bodies over external sources.
Failed or incomplete reads never prove absence. Nested negation and external
negative recursion are unsupported. Eligible scalar absence checks execute as
SQL `IS NULL`; other supported shapes run in Datalevin.

To keep a row with a missing value, bind `nil` explicitly. Use
`:ignore-nil-extrema` when selected `min` or `max` aggregates should ignore nil
inputs and return nil for an all-null group:

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

Each declared aggregate ignores only its own nil inputs. Other aggregates keep
their usual behavior. The tuple binding in the default branch binds nil as a
value; a scalar function returning nil would filter out the row.

## Snapshot and execution guarantees

Each query uses a read-only, repeatable-read PostgreSQL transaction. Mappings
with the same source UUID and compatible `:compute-context-id` share one
connection and snapshot, including mappings of different tables. Independent
databases do not share an atomic snapshot.

Acquisition verifies the accepted relation, key, and mapped column metadata.
Incompatible changes fail as stale definitions; unrelated added columns do not
invalidate a mapping. Queries hold access-share locks and their snapshot until
completion, which can affect vacuum and schema changes. Concurrent writes remain
permitted. A lost connection fails the query without reconnecting or retrying.

SQL uses quoted identifiers and bound parameters. Repeated query variables use
Datalevin key equality, including decimal scale: `1.0M` and `1.00M` are distinct
keys. Literal constants and explicit `=` predicates use numeric equivalence.
Ordinary tables exclude inheritance children; partitioned parents include their
attached partitions.

## Views, partitions, and row-level security

Accept a view or materialized view by declaring its business key:

```clojure
(postgres/accept-mapping
  (postgres/discover profile ["reporting" "orders"])
  {:source-instance-id source-uuid
   :relation-id :reports/orders :identity-attribute :report/id
   :key-columns ["tenant_id" "order_id"]
   :attributes {:report/total "total"}})
```

Views, materialized views, partitioned parents, and RLS-protected tables capture
all mapped rows once per query session. This freezes view expressions and policy
results for repeatable reads, but can transfer and retain substantially more
data than the result contains. Their filters, joins, and aggregates execute on
the captured rows in Datalevin. SQL delegation is unavailable for these captures.

View keys must remain stable across queries; null or duplicate keys fail
acquisition. Materialized views must be populated. Changes to accepted view,
policy, or catalog-visible function definitions require rediscovery and
acceptance. Dependencies hidden inside dynamic SQL or function bodies remain
the application's responsibility.

For RLS, discover, accept, and reopen with the same authorization context:

```clojure
(def tenant-profile
  (assoc profile :role "application_reader"
                 :session-settings {"app.tenant_id" "tenant-a"}))
```

`:role` applies a transaction-local role. `:session-settings` accepts up to 32
custom dotted setting names with string values. Declare every policy-relevant
custom setting explicitly. RLS must be active for the effective role; superuser,
`BYPASSRLS`, and owner bypass are rejected. An owner using `FORCE ROW LEVEL
SECURITY` can be accepted when enforcement is active.

Security scope participates in entity identity so tenant-local keys from
different scopes do not alias. Role or setting mismatches fail as unauthorized;
policy changes fail as stale metadata. Query filters do not establish an
authorization boundary. Ordinary views use their owner's permissions;
`security_invoker` views use the caller's, following PostgreSQL behavior.

Column-level SELECT grants on mapped columns and keys are sufficient. Sessions
that combine captured relations with SQL reads can need a second connection
for metadata and authorization verification. Allow for that simultaneous
checkout, and reuse the same `DataSource` instance for profiles backed by the
same pool.

## Filter and relational delegation

The adapter can execute eligible filters, joins, grouping, and aggregates in
PostgreSQL when their sources share one snapshot and authorization context.
Unsupported shapes use ordinary fact reads and Datalevin operators. A lookup
does not imply that the PostgreSQL column has an index.

| Operation | SQL support |
| --- | --- |
| Comparisons | Matching integral and decimal values; eligible `Instant`, `LocalDate`, `LocalDateTime`, and `LocalTime` bounds. |
| `like` / `not-like` | Binary calls on text/varchar with literal or captured ASCII patterns, preserving case-sensitive matching. |
| `in` / `not-in` | Up to 256 text/nil members in relational or dependent join plans. |
| Joins | Eligible flat conjunctions, self joins, and typed references within one acquired PostgreSQL session. |
| Aggregates | `count`, `count-distinct`, decimal `sum`, integral `sum` with an explicit nonnegative range predicate, and eligible scalar `min`/`max`. |
| Ordering and windows | Eligible whole-query projections with integral, decimal, boolean, or `Instant` sort keys, offset, and limit. |

Temporal bounds for `Instant`, `LocalDate`, and `LocalDateTime` must be in years
0001–9999. Offset-time ordering and interval comparisons execute locally.
Unsupported LIKE patterns, custom escape arguments, dynamic patterns, and other
membership collections also stay local. Decimal min/max require fixed column
scale; floating-point aggregation and unrestricted integral sums stay local.

Text extrema normally use Datalevin ordering. To explicitly request PostgreSQL's
native text ordering for an aggregate, add a declaration to the quoted query:

```clojure
:query-source {:native-extrema {?status {:source $orders
                                        :attribute :order/status}}}
```

This changes the ordering contract for that variable; use it only when
PostgreSQL's ordering is the intended result.

Set `:whole-query-mode` consistently on the participating runtime profiles:

| Mode | Behavior |
| --- | --- |
| `:auto` (default) | Consider complete SQL, retaining selective dependent plans for eligible query shapes. |
| `:sql` | Prefer complete SQL whenever the query is eligible. |
| `:dependent` | Keep whole-query execution in Datalevin while allowing filtered and dependent join pushdown. |

Set `:relational? false` to disable relational delegation. All modes preserve
type, collation, authorization, and expression checks. `d/explain` identifies
whole-query plans with `:policy :relational-v1`. Plan-only explanation still
acquires and validates sources; captured relations can require data reads.

String, UUID, byte-array, and entity-reference ordering execute in Datalevin.
A finite result window retains at most `offset + limit` candidate rows, but
local joins and aggregation may still need much larger inputs. Include a unique
final sort key when you need a deterministic result window:

```clojure
(d/q '[:find ?id ?amount
       :in $orders
       :where [$orders ?order :order/id ?id]
              [$orders ?order :order/amount ?amount]
       :order-by [?amount :desc ?id :asc]
       :limit 20]
     orders)
```

Supported local scalar arithmetic, filters, `:having`, and result ordering can
also be used when SQL delegation is unavailable. Arbitrary host functions,
nested calls, and collection output bindings are outside the supported scalar
expression path. A small result limit does not bound upstream scans or joins.

## Delegation inside mixed-source queries

A flat query joining native or local data to one PostgreSQL session can delegate
its SQL portion. For example, with a native customer database:

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

Native customer IDs become bounded lookup batches. PostgreSQL can join accepted
tables and return totals without transferring every order. `:with ?order`
ensures equal amounts from distinct orders contribute separately. All batches
share the acquired PostgreSQL snapshot.

This path requires a shared boundary variable and supported flat patterns and
predicates; aggregate arguments must belong to the SQL portion. Incompatible
sessions, unsupported expressions, and captured relations use ordinary reads.
A batch accepts at most 256 bindings and 2 MiB of binding data, further limited
by the query's own limits.

Complete aggregate groups must fit in a batch. A global total across more than
256 customer keys, for example, can fall back to ordinary execution.
`d/explain` marks the delegated path with `:policy :relational-fragment-v1`;
`:conditional-inputs true` means group sizes will be checked at execution.

## Runtime configuration

Profiles require `:compute-context-id` and either `:jdbc-url` with optional
string `:properties`, or a caller-owned JDBC `:data-source`. Connections must
be supplied in autocommit mode. The adapter rolls back and closes acquired
connections, returning them to the pool without closing the pool itself.
Use the same context ID only for compatible database, role, and settings.

| Option | Default | Meaning |
| --- | --- | --- |
| `:connect-timeout-ms` | `10000` | Maximum admission plus initial-checkout wait; also bounds each validation checkout |
| `:statement-timeout-ms` | `10000` | PostgreSQL statement timeout |
| `:fetch-size` | `128` | Maximum rows per explicit server-cursor fetch; up to 256, reduced when needed to admit cursor workspace |
| `:max-row-bytes` | `16384` | Maximum combined SQL text size of materialized values and keys per fact-access row, or the final returned row for delegated SQL |
| `:fusion?` | `true` | Combine scalar attributes of one entity into one SQL access |
| `:relational?` | `true` | Allow eligible SQL joins and aggregation |
| `:whole-query-mode` | `:auto` | Prefer whole-query SQL automatically, or select `:sql` / `:dependent` for workload comparisons; semantic checks always apply |
| `:statistics?` | `true` | Use catalog row estimates to plan accesses |
| `:filtered-statistics?` | `true` | Allow bounded EXPLAIN probes for filtered row estimates; requires `:statistics?` |
| `:role` | unset | Transaction-local PostgreSQL role |
| `:session-settings` | `{}` | Explicit policy/view context: custom dotted GUC names mapped to strings |

<a id="planning-bounds-and-follow-up-priorities"></a>

## Bounds and lifecycle

Set the query's `:timeout` and `:query-source :limits` to bound the whole
invocation. Defaults allow 1,024 external requests and 1 GiB of retained memory,
shared by provider state, joins, and results. Connection setup, catalog reads,
and cursor fetches consume requests. At 128 rows per fetch, a 100,000-row scan
needs 782 fetches plus setup; multiple accesses share the budget.

Lookup batches accept at most 256 keys and 2 MiB of bindings. Larger fetches
reduce request counts but reserve more memory. Raising `:max-row-bytes` also
increases workspace needs; the adapter reduces fetch pages to fit the memory
budget and fails if even one row cannot fit. Captured relations retain all
mapped rows and can need substantially more memory than ordinary reads.

Use `_` when only a value's existence matters:

```clojure
(d/q '[:find ?id :in $orders
       :where [$orders ?order :order/id ?id]
              [$orders ?order :order/note _]]
     orders)
```

An existence check can avoid fetching a large payload. A named variable needed
for a join or predicate may still require its value, even if absent from
`:find`. Oversized values fail when their payload is needed; they are never
silently truncated. Delegated SQL applies the row cap to its final returned
projection.

Cancellation and failures release cursors, connections, and captured rows.
Cleanup also runs after a budget is exhausted. Use executed `d/explain` metrics
to inspect decoded rows, output batches, request counts, and elapsed time.
`postgres/statistics` separately reports acquisition requests and live sessions.

<a id="save-mappings-and-test"></a>

## Save and reopen mappings

Persist `accepted` with the [definition installer](../README.md#persisted-definitions),
using source provider `:postgres` and a runtime profile reference. After
`install/load-definitions`, pass the saved relation's `:accepted` mapping to
`postgres/source` with runtime credentials.

Refresh explicitly by discovering and accepting new metadata, then installing
it with the current definition version. Keep credentials, connections, and
fetched rows outside saved definitions.
