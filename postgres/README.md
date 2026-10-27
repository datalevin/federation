# PostgreSQL tables

`org.clojars.huahaiy/datalevin-federation-postgres` queries PostgreSQL tables
through ordinary `d/q` and `d/explain`, alongside native Datalevin data and other
adapters. It supports table scans, entity and value lookups, and same-entity
fusion: several scalar attributes of one entity can be read by one SQL cursor.

This is a development snapshot. Use the sibling Datalevin checkout with the
current SPI v2 fusion implementation. The module's provided `1.2.8` dependency
does not by itself establish compatibility with later SPI changes.

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
subsequent cursor opens continue to observe the original snapshot. A lost
connection fails the query; there is no reconnect or automatic retry.

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
| `:max-row-bytes` | `16384` | Maximum combined UTF-8 SQL text size of selected physical values |
| `:fusion?` | `true` | Set false to compare separate EAV accesses |

Oversized rows produce an error marker before their values cross the network;
they are never silently truncated or omitted. Reserved workspace includes the
fetch page, decoding, and pending row; returned batches have separate charges.
The query's `:timeout` and `:query-source :limits` apply across all accesses.
Cancellation aborts blocked database I/O and releases owned resources.

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
