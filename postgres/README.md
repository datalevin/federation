# PostgreSQL tables

`org.clojars.huahaiy/datalevin-federation-postgres` queries PostgreSQL tables
through ordinary `d/q` and `d/explain`, alongside native Datalevin data and other
adapters. It supports table scans, entity and value lookups, and same-entity
fusion: several scalar attributes of one entity can be read by one SQL cursor.

This is a development snapshot requiring **Datalevin 1.2.9** for same-entity
fusion. The module still declares `1.2.8` as a provided dependency; applications
must explicitly supply `1.2.9`. The development workspace uses the sibling
checkout. Both versions use SPI revision 2, so the revision number alone does
not establish fusion compatibility.

## Workload fit and current gaps

The current fit is selective enrichment of a small set of keys from ordinary
primary-key tables, including joins with native data and other adapters.
Same-entity fusion avoids reading each scalar attribute separately. Ordinary
joins between mapped PostgreSQL tables also work, with Datalevin performing
the join and each mapping supplying its own accesses.

The following gaps matter when moving from that workflow to a typical
application schema or a larger reporting query:

| Use case | Current gap | Effect on the workload |
| --- | --- | --- |
| Enrich hundreds or thousands of keys | One SQL cursor per distinct input key; no bulk `VALUES`, `IN`, or array binding | At least a declaration and a fetch per key, plus cleanup. Network latency and the request budget can dominate even indexed lookups. |
| Filter orders by amount or events by time | Only eligible `Long` comparisons are pushed down; decimal and temporal ranges stay in Datalevin | A selective final result can still require fetching a large PostgreSQL population. Pattern equality has broader pushdown support, but `timestamptz` equality currently also stays local. |
| Read an existing application schema | No `date`, `timestamp without time zone`, `time`, `interval`, enums, domains, `char`, `citext`, arrays, or JSON/JSONB mapping | These fields cannot be exposed, although supported columns of the same table can still be selected. Collection-valued attributes are not implemented. |
| Read reporting views, partitioned datasets, or tenant tables with RLS | Only ordinary tables with validated nondeferrable primary keys are accepted; queries use `ONLY` | Views, materialized views, partitioned parents, and RLS-enabled tables are rejected. Inheritance children are excluded. A unique index or a user-supplied key assertion cannot substitute for the primary key. |
| Aggregate, sort, or join large PostgreSQL relations | Cross-entity joins, aggregates, ordering, and SQL `LIMIT` are not delegated | Datalevin must consume the rows needed for those operations. A small final result does not imply small transfer or bounded backend work. Access choice is structural, with no index discovery or comparable cost estimates. |
| Read large text or binary fields | The default selected-row cap is 16 KiB, measured using SQL text representations | Oversized values fail the query. Referenced fields are fetched even when their value is `_` or not projected, so an existence-only query can hit this limit too. |
| Discover relationships or restrict a saved mapping to a tenant | Discovery covers one named relation's columns and primary key; foreign keys and restricted-scope mappings are absent | Define references manually and put ordinary filters in the query. Query filters are not an authorization boundary. |

Supported columns do not require mapping every physical field. Omitting a
JSONB or unsupported temporal column is useful when the query does not need
it; it does not make that column queryable. Likewise, a value-lookup alternative
does not guarantee a PostgreSQL index exists for that column.

`timestamptz` is exposed as `java.time.Instant` under logical type `:data` to
preserve microseconds. Native Datalevin `:db.type/instant` uses `java.util.Date`.
Joining those representations needs an explicit conversion and a decision
about precision; they are not interchangeable query keys.

## Known planning limit and follow-up priorities

The [adapter planner](src/datalevin/federation/postgres/plan.clj) currently takes
the first `:max-alternatives` candidates, default `32`, without preserving
coverage of every pattern. A review probe with eleven attributes on one bound
entity and a second entity clause generated 32 candidates, all omitting the
second clause. Such a request has no complete plan even though the adapter
could execute it. This is a planning defect, not an unsupported SQL type or
an empty result.

Using `{:query-source {:planning {:max-alternatives 64}}}` restored coverage
in that probe. It is a workaround for that request, not a general guarantee:
candidate pruning must preserve a complete set of accesses. Disabling fusion
can also change candidate pressure but is not a general fix.

The highest-value follow-ups for ordinary federation are:

1. Preserve complete coverage when bounding alternatives and avoid redundant
   fused candidates. Wide queries should not fail because earlier candidates
   consumed the response budget.
2. Batch dependent keys into SQL requests, and measure scans and lookups with
   realistic network latency and default query budgets.
3. Add explicit temporal conversions and safe decimal/temporal predicate
   pushdown, followed by common enum/domain mappings.
4. Extend relation support with proven keys and read guarantees for partitioned
   tables and views; define authorization behavior before supporting RLS.
5. Add foreign-key discovery, prune unneeded payload columns while retaining
   existence checks, and then evaluate further SQL delegation with the core SPI.

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

Acceptance requires an ordinary PostgreSQL table with a validated,
nondeferrable primary key. Composite keys are supported. Identity is nominal:
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

Key columns must be integer, text/varchar, or UUID types. Unsupported columns
remain visible in discovery but cannot be mapped. Numeric and timestamp
infinities that lack an exact mapped representation fail execution. Each
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
byte content. Eligible long comparisons can be pushed down. Other predicates,
cross-entity joins, rules, and aggregates run in Datalevin. Fusion preserves
existence guards, repeated variables, and the identities needed by `:with`.

This first adapter reads `ONLY` the accepted table, excluding inheritance
children. Views, partitioned parents, row-level security, arrays, JSON mapping,
arbitrary SQL, collection expansion, and cross-entity SQL join delegation are
outside its supported scope.

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
| `:fetch-size` | `32` | Rows per explicit server-cursor fetch; maximum 256 |
| `:max-row-bytes` | `16384` | Maximum combined byte size of selected physical values rendered as SQL text |
| `:fusion?` | `true` | Set false to compare separate EAV accesses |

Oversized rows produce an error marker before their values cross the network;
they are never silently truncated or omitted. Reserved workspace includes the
fetch page, decoding, and pending row; returned batches have separate charges.
The query's `:timeout` and `:query-source :limits` apply across all accesses.
Cancellation aborts blocked database I/O and releases owned resources.

The default query budget allows 1,024 external requests. At 32 rows per fetch,
a scan returning 100,000 rows needs at least 3,125 fetch requests before counting
setup and cursor declaration, so it cannot complete under that budget. Likewise,
1,000 distinct lookup keys need at least 2,000 declaration/fetch requests. These
are request-count calculations, not measured throughput limits. More accesses,
aliases, or other sources share the same query budget.

For larger workloads, inspect executed `d/explain` metrics and adjust both the
profile's `:fetch-size` and the query's `:query-source :limits` deliberately.
Larger fetches reserve more memory. Increasing `:max-row-bytes` also increases
the reserved fetch workspace: a 1 MiB row cap with the default fetch size alone
requires over 512 MiB of cursor workspace, exceeding the default 64 MiB query
retention budget. Lower the fetch size or provide an appropriate memory budget
when accepting larger fields. These settings do not add bulk lookup or broader
predicate pushdown.

Each dependent input is executed with bound parameters; this initial version
does not combine multiple keys into one SQL request. Connection setup, catalog
reads, cursor declarations, and fetches consume the shared request budget.
Resource cleanup (`CLOSE`, rollback, connection close) runs even after budget
exhaustion. Cursor metrics report decoded rows, output batches, requests, and
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
late checkout, blocked fetch cancellation, and snapshot loss. The fault checks
need permission to terminate their own test connection.
