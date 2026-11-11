# datalevin-federation

Data federation allows using Datalevin to query heterogeneous data sources
without having to copying all data into Datalevin. This repository hosts the
data source adapters. Each is released as its own clojars artifact.

Currently, we support query PostgreSQL, file exports, GitHub, and native
Datalevin data together with Datalog. Federation adapters expose external
records as entities and attributes, so one can join them using `d/q` without
first importing all the data into Datalevin. Bindings and supported filters
constrain external reads; `d/explain` shows the selected plan and execution
metrics.

**Status:** development snapshots (`0.1.0-SNAPSHOT`) for embedded JVM applications.
The checkout instructions below use the matching Datalevin **1.2.10** development
build, which includes the query-source extensions required by PostgreSQL.

## Data sources

| Source | Supported access |
| --- | --- |
| [PostgreSQL](postgres/README.md) | Tables with primary keys, views with declared keys, partitioned tables, and RLS-protected tables; eligible filters, joins, and aggregates execute in SQL. |
| [EDN Lines files](file/README.md) | Verified snapshots of exports, with scalar, collection, and reference attributes. |
| [GitHub](github/README.md) | Live issue and pull-request lookups by entity, repository/issue key, or issue number. |
| Native Datalevin | Pass databases directly to `d/q`; no adapter is needed. |
| [Local relations](local/src/datalevin/federation/local.clj) | Application-owned `[entity attribute value]` facts with a declared schema. |

GitHub supports keyed lookups, not repository listings or search. Files are
scanned in full when their snapshots are acquired. See each adapter's guide for
supported values, configuration, and limits.

## Getting started

You need the Clojure CLI and a compatible Datalevin checkout at `../datalevin`.
The root `deps.edn` loads that checkout and all adapter sources. From this
repository's root, compile Datalevin's Java classes:

```sh
cd ../datalevin
clojure -T:build compile-java
cd ../federation
```

Run the [release-triage example](examples/README.md):

```sh
clojure -M:workflow-live
```

This joins priorities stored in Datalevin, release assignments from a temporary
file export, and live issue titles from the public `octocat/Hello-World`
repository. It saves and reopens the mappings, prints result rows and query
diagnostics, and removes its temporary files. `GITHUB_TOKEN` is optional for
public repositories. For a local fixture check without GitHub requests:

```sh
clojure -M:test:workflow
```

To try the examples below, start a REPL with the workspace's sources and JVM
options:

```sh
clojure -Sdeps '{:aliases {:repl {:main-opts ["-r"]}}}' -M:test:repl
```

## Join GitHub issues with local priorities

First, declare the repository and fields you want to query, then create a source
with your runtime credentials. The UUID identifies the GitHub installation;
allocate and persist your own once when setting up an application.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.github :as github])

(def accepted
  (github/accept-mapping
    {:relation-id :github/work-items
     :source-instance-id #uuid "b7bca594-d387-4a21-8cf4-60379ecb5098"
     :repository {:id 1296269 :owner "octocat" :name "Hello-World"}
     :attributes #{:github.issue/number :github.issue/title}}))

(def live
  (github/source accepted
    {:compute-context-id :profiles/github-hello-world
     :token (System/getenv "GITHUB_TOKEN")}))
```

Create a local priority record and join it with the live issue title. This
example stores the native database in `triage-db` under your working directory.

```clojure
(let [conn (d/get-conn "triage-db"
             {:triage/repository {:db/valueType :db.type/long}
              :triage/number {:db/valueType :db.type/long}
              :triage/priority {:db/valueType :db.type/long}})]
  (try
    (d/transact! conn [{:triage/repository 1296269
                       :triage/number 1
                       :triage/priority 10}])
    (d/q '{:find [?number ?priority ?title]
           :in [$ $github]
           :where [[?t :triage/repository 1296269]
                   [?t :triage/number ?number]
                   [?t :triage/priority ?priority]
                   [(>= ?priority 9)]
                   [$github ?issue :github.issue/number ?number]
                   [$github ?issue :github.issue/title ?title]]
           :timeout 30000
           :query-source {:consistency :best-effort
                          :limits {:external-requests 64 :batch-rows 32}}}
         @conn live)
    (finally
      (d/close conn))))
```

`$` is the native database; `$github` is the source passed after it in `:in`.
Each external clause has the form `[$source entity attribute value]`. The
native records supply issue numbers for GitHub lookups. Results contain the
issue number, local priority, and current GitHub title.

Mappings define which fields become attributes, their types, and their entity
identities. Missing optional values contribute no facts. Collection attributes
such as labels contribute one fact per distinct value. You can use these facts
in joins, aggregates, and supported Datalog rules. Bound `not` and `not-join`
fact bodies check attribute absence over external sources; see the
[absence example](postgres/README.md#nulls-and-absence). Ordinary tuple
inputs still work through `:in`.

See the [GitHub guide](github/README.md) for all supported attributes and the
[file guide](file/README.md) for a complete export-mapping example.

## Persisted definitions

Save accepted mappings in your Datalevin database to reuse them after reopening
it. Runtime credentials, file paths, and client objects are supplied separately.
The following uses the `accepted` mapping and database from the example above:

```clojure
(require '[datalevin.federation.install :as install])

(let [conn (d/get-conn "triage-db")]
  (try
    (install/install!
      conn
      {:github
       {:datalevin/definition-kind :source
        :datalevin.federation/definition
        {:format-version 1
         :source-instance-id (:source-instance-id accepted)
         :provider :github/issues
         :profile :profiles/github-hello-world}}
       :github/work-items
       {:datalevin/definition-kind :relation
        :datalevin.federation/definition
        {:format-version 1 :source :github :accepted accepted}}}
      {:expected-versions {:github nil :github/work-items nil}})
    (finally
      (d/close conn))))

;; Later, reopen the database and reconstruct the source with runtime credentials.
(def reopened-source
  (let [conn (d/get-conn "triage-db")]
    (try
      (let [loaded (install/load-definitions conn)]
        (github/source
          (get-in loaded [:compiled :github/work-items :accepted])
          {:compute-context-id :profiles/github-hello-world
           :token (System/getenv "GITHUB_TOKEN")}))
      (finally
        (d/close conn)))))
```

For each definition, `nil` in `:expected-versions` means it must not already
exist. To update an existing definition, supply its current positive version
instead. Stale versions fail without overwriting newer definitions. Reopening
uses the saved identities and mappings without discovering a new schema.

## Read guarantees and limits

- GitHub reads are best-effort: separate requests can observe different updates.
  Local relations and accepted file exports provide snapshots. PostgreSQL
  mappings sharing a source and runtime profile use one read-only,
  repeatable-read transaction. Independent sources do not share an atomic
  snapshot.
- Views, partitioned parents, and RLS-protected PostgreSQL tables capture all
  mapped rows before querying. File queries copy and validate the entire
  accepted export. Allow for that startup work and memory or temporary storage.
- Use `:timeout` and `:query-source :limits` to bound work. Quota exhaustion,
  incomplete reads, and provider failures fail the query instead of returning
  partial results. GitHub and PostgreSQL requests are not automatically retried.
- Changed file contents or incompatible database schema changes require
  explicitly accepting a new mapping. Credentials, connections, and file paths
  are supplied at runtime.
- Live adapters support embedded JVM `d/q` and `d/explain`. External writes,
  remote execution of live adapters, native images, and language bindings are
  outside the supported scope.

<a id="subprojects-and-releases"></a>

## Modules

Each module has version `0.1.0-SNAPSHOT` and the Maven group
`org.clojars.huahaiy`:

| Artifact | Purpose |
| --- | --- |
| `datalevin-federation-core` | Mappings, entity identities, and saved definitions. |
| `datalevin-federation-local` | Application-owned fact sources. |
| `datalevin-federation-github` | GitHub work-item lookups; includes core. |
| `datalevin-federation-file` | EDN Lines exports; includes core. |
| `datalevin-federation-postgres` | PostgreSQL access; includes core and the JDBC driver. |

Use the modules your application needs. Datalevin has Maven `provided` scope,
so your application must supply a compatible runtime. PostgreSQL requires the
matching 1.2.10 build with relational query-source support. The checkout setup
above supplies the runtime for all modules.

## Guides

- [Release-triage example](examples/README.md): join native data, an export, and GitHub.
- [PostgreSQL](postgres/README.md): connect, map tables, query, and configure SQL delegation.
- [GitHub](github/README.md): map work items and configure API lookups.
- [Files](file/README.md): accept and query EDN Lines exports.

## Benchmarks

- [JOB baselines](benchmarks/job/README.md): reproduce comparisons with native
  PostgreSQL, Trino, Drill, DuckDB and Accio.

Licensed under [EPL-2.0](LICENSE).
