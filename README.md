# datalevin-federation

Query external data alongside your Datalevin database with ordinary Datalog.
Federation adapters expose external records as entities and attributes, so you
can join GitHub issues, file exports, and application-owned data with native
Datalevin facts using `d/q`.

For example, a release-triage query can combine priorities stored in Datalevin,
release assignments from an export, and live issue titles from GitHub.

**Status:** development snapshots (`0.1.0-SNAPSHOT`). The adapters currently
require a matching Datalevin checkout with the v2 query-source SPI. The
Datalevin `1.2.5` dependency declared in the subprojects does not include that
SPI and cannot run these adapters on its own.

## Supported sources

| Source | What you can query | Read behavior |
| --- | --- | --- |
| [GitHub](github/README.md) | Issues and pull requests by entity, repository/issue key, or issue number; titles, labels, assignees, and other fields. | Live, best-effort lookups. No listings, search, or pagination. |
| [EDN Lines files](file/README.md) | UTF-8 exports containing one EDN map per line, with scalar and collection attributes. | Verified snapshot scans. File contents must match the accepted export. |
| [Local relations](local/src/datalevin/federation/local.clj) | Application-owned `[entity attribute value]` facts with a declared schema. | Snapshot scans and entity/value lookups. |
| Native Datalevin | Your existing database. | Supplied directly to `d/q`; no adapter needed. |

PostgreSQL, Parquet, dataset, and DataFusion adapters are planned and are not
available in this repository.

## Getting started

Use the Clojure CLI and matching checkouts of this repository and Datalevin,
with Datalevin at `../datalevin`. The root `deps.edn` loads that checkout and
all adapter sources.

From this repository's root, compile Datalevin's Java classes:

```sh
cd ../datalevin
clojure -T:build compile-java
cd ../federation
```

Try the [release-triage example](examples/README.md):

```sh
clojure -M:workflow-live
```

This makes read-only requests to the public `octocat/Hello-World` repository,
creates a temporary export and native database, saves and reopens the mappings,
and joins all three sources. It prints the result rows and query diagnostics,
then removes its temporary files. `GITHUB_TOKEN` is optional for public
repositories. To exercise the workflow with local fixtures instead:

```sh
clojure -M:test:workflow
```

For the examples below, start a REPL with the workspace's sources and JVM options:

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
in joins, aggregates, and supported Datalog rules. Ordinary tuple inputs still
work through `:in`.

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

- GitHub reads are best-effort: separate requests may observe different updates.
  Local relations and accepted file exports provide snapshots. There is no
  atomic snapshot across independent sources.
- GitHub queries need an entity, key, or issue number to start a lookup. A query
  that only asks for every title or label cannot run against this adapter.
- File queries copy and validate the entire accepted export before scanning it.
  Changed contents require explicitly accepting a new export. File row identities
  are specific to that capture.
- Use `:timeout` and `:query-source :limits` to bound work. Quota exhaustion,
  incomplete reads, and provider failures fail the query. GitHub requests are
  not automatically retried.
- Live adapters currently target embedded JVM `d/q` and `d/explain`. External
  writes, remote execution of live adapters, and native-image or language-binding
  support are outside the current supported scope.

## Subprojects and releases

Each module has its own artifact at version `0.1.0-SNAPSHOT`:

| Module | Artifact | Purpose |
| --- | --- | --- |
| [Core](core/project.clj) | `org.clojars.huahaiy/datalevin-federation-core` | Mapping compilation, entity identities, and saved definitions. |
| [Local](local/project.clj) | `org.clojars.huahaiy/datalevin-federation-local` | Application-owned fact sources. |
| [GitHub](github/project.clj) | `org.clojars.huahaiy/datalevin-federation-github` | GitHub work-item lookups; includes core. |
| [File](file/project.clj) | `org.clojars.huahaiy/datalevin-federation-file` | EDN Lines exports; includes core. |

Use the modules your application needs. The local adapter takes a complete
source descriptor and mapped facts; add core if you also need mapping APIs.
Datalevin has Maven `provided` scope, so your application must supply a
compatible runtime. For now, use the matching checkouts described above.

## Development and further reading

Run the full suite or an individual adapter's tests from the repository root:

```sh
clojure -M:test
clojure -M:test:core
clojure -M:test:local
clojure -M:test:file
clojure -M:test:github
```

Tests use local fixtures and require no external service. The full and local
suites need `sqlite3` on `PATH`; HTTP fixtures need permission to bind a temporary
loopback port.

- [Release-triage example](examples/README.md): combine native data, an export,
  and GitHub.
- [Adapter development guide](doc/adapter-development.md): build a new source
  adapter, including mapping, execution, and packaging.
- [Design and roadmap](federation.md): architecture and planned capabilities.
- [Datalevin query-source SPI](../datalevin/doc/query-source-spi.md): detailed
  query-source contracts; this link assumes the sibling checkout.

Licensed under [EPL-2.0](LICENSE).
