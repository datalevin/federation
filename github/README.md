# GitHub work-item lookups

`org.clojars.huahaiy/datalevin-federation-github` joins live GitHub issues and
pull requests with native Datalevin data and other sources. It reads titles,
labels, assignees, and other fields through keyed lookups. Repository listings,
search, and pagination are not supported.

Use the repository's [setup instructions](../README.md#getting-started) to run
the examples. The adapter uses the issues endpoint, which includes both issues
and pull requests, and sends REST API version `2026-03-10`.

## Map a repository and query

Supply the repository's numeric ID, owner, and name. Allocate and persist your
own source UUID for the GitHub installation. Creating or reopening a mapping
performs no API requests; the runtime profile supplies the host and credentials.

This example creates a local database in `triage-db` and joins a priority record
with its live issue title:

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.github :as github])

(def accepted
  (github/accept-mapping
    {:relation-id :github/work-items
     :source-instance-id #uuid "b7bca594-d387-4a21-8cf4-60379ecb5098"
     :repository {:id 1296269 :owner "octocat" :name "Hello-World"}
     :attributes #{:github.issue/number :github.issue/title
                   :github.issue/label :github.issue/assignee}}))

(def live
  (github/source accepted
    {:compute-context-id :profiles/github-hello-world
     :token (System/getenv "GITHUB_TOKEN")}))

(let [conn (d/get-conn "triage-db"
             {:triage/repository {:db/valueType :db.type/long}
              :triage/number {:db/valueType :db.type/long}
              :triage/priority {:db/valueType :db.type/long}})]
  (try
    (d/transact! conn [{:triage/repository 1296269
                       :triage/number 1
                       :triage/priority 10}])
    (d/q '{:find [?priority ?title]
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

## Attributes and identity

The identity attribute `:github.issue/key` is `[repository-id issue-number]`.
Entity values are `EntityRef`s identified by the source UUID, the `:github/issue`
domain, and that key. A key or entity can start a lookup directly. Use the scalar
`:github.issue/number` for joins with native long attributes, and filter native
records to the accepted repository as shown above.

Omitting `:attributes` accepts all supported fields:

| Attribute | Value / cardinality |
| --- | --- |
| `:github.issue/key` | Identity tuple, one |
| `:github.issue/id`, `:github.issue/number` | Long, one |
| `:github.issue/title`, `:github.issue/state`, `:github.issue/url` | String, one |
| `:github.issue/locked?`, `:github.issue/pull-request?` | Boolean, one |
| `:github.issue/label`, `:github.issue/assignee-login` | String, many |
| `:github.issue/assignee` | Nominal reference, many |

Assignee identity uses the source UUID and the `:github/user` domain, keyed by
GitHub's numeric user ID. Duplicate collection elements become one fact; empty
collections emit no facts. Missing or mistyped required fields fail the query.
GitHub's issue `id` for a pull request is not its separate pull-request ID.

## Runtime configuration

A profile requires `:compute-context-id`, which identifies a compatible host
and credential configuration. Reuse a source object for aliases that should
share a session; use different context IDs for independent profiles.

| Option | Default | Purpose |
| --- | --- | --- |
| `:token` | unset | GitHub token; optional for public repositories. |
| `:api-base` | `https://api.github.com` | API root, including an Enterprise API prefix when needed. |
| `:request-timeout-ms` | `10000` | Total timeout for each request, including its body. |
| `:max-response-bytes` | `262144` | Maximum response body size. |
| `:max-json-nodes` | `4096` | Maximum parsed JSON nodes. |

HTTPS is required except for loopback HTTP. Set the query's top-level `:timeout`
in milliseconds to bound the whole invocation. Request and memory limits apply
across all sources in that query. The default response/parser settings reserve
about 18.1 MiB of workspace, in addition to query results and other retained data.

## Read behavior and limits

Queries need a bound entity, issue key, or issue number. Titles and labels have
no lookup index, so a query asking for every title cannot run. Lookup batches
accept at most 256 unique keys and 256 KiB of binding state, but the adapter
still performs individual issue GETs. Requests are serialized.

Reads are **best-effort**. Separate attribute accesses can observe different
updates; snapshot and statement consistency requirements are rejected. Each
query first checks that the repository name still resolves to the accepted
numeric ID. Renames or reused names require verifying and updating the mapping.
Redirects fail without forwarding credentials.

Authorization failures, rate limits, timeouts, malformed or incomplete responses,
and oversized responses fail the query. There are no automatic retries. An
issue 404 also fails: it can mean that access was denied, so it cannot prove the
issue is absent. Applications decide when to run another query.

`d/explain` reports request counts, decoded work items, emitted rows, and fetch
time. `github/statistics` reports cumulative repository acquisition work and
live sessions. Diagnostics omit tokens and response bodies.

## Save and reopen mappings

Use the [definition installer](../README.md#persisted-definitions) with provider
`:github/issues`, the accepted source UUID, and a runtime profile reference.
After `install/load-definitions`, pass the saved relation's `:accepted` mapping
to `github/source` with the runtime profile. Store credentials separately from
definitions.

## Try the live example

From the repository root:

```sh
clojure -M:github-live
# Optional owner, repository, numeric repository ID, and issue number:
clojure -M:github-live octocat Hello-World 1296269 1
```

This makes read-only GitHub requests and joins the result with temporary native
triage data. Set `GITHUB_TOKEN` for authenticated access. To also join release
assignments from a file export, see the [release-triage example](../examples/README.md).
