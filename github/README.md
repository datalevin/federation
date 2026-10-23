# GitHub work-item lookups

`org.clojars.huahaiy/datalevin-federation-github` is an optional, read-only
adapter for a concrete workflow: join local triage priorities with live GitHub
work-item titles, labels and assignees. It uses the repository issues endpoint,
which includes issues and pull requests. It pins REST API version `2026-03-10`.
See GitHub's [issues contract](https://docs.github.com/en/rest/issues/issues)
and [API version policy](https://docs.github.com/en/rest/about-the-rest-api/api-versions).

## Mapping and native join

Supply an explicit repository ID, locator and stable deployment UUID. Accepting
or reopening a mapping performs no I/O and generates no new identity. Choose a
different deployment UUID for another GitHub installation. Runtime profiles
must bind that deployment to its intended API host and credentials.

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

;; The native schema declares repository, number and priority as db.type/long.
;; The live source is scoped to repository 1296269, including closed items.
(d/q '{:find [?priority ?title ?label]
       :in [$ $github]
       :where [[?t :triage/repository 1296269]
               [?t :triage/number ?number]
               [?t :triage/priority ?priority]
               [(>= ?priority 9)]
               [$github ?issue :github.issue/number ?number]
               [$github ?issue :github.issue/title ?title]
               [$github ?issue :github.issue/label ?label]]
       :timeout 30000
       :query-source {:consistency :best-effort
                      :limits {:external-requests 64 :batch-rows 32}}}
     @conn live)
```

The identity attribute `:github.issue/key` is a tuple
`[repository-id issue-number]`. Entity values are durable nominal `EntityRef`s
in `[deployment-uuid :github/issue 0]`. A key or entity can seed a lookup directly;
repository-local `:github.issue/number` also supports typed scalar joins with
native long attributes. Native tuples currently have a broad SPI type, so use
the scalar number join for native triage records. A key from another repository
in the same accepted entity domain produces no facts and no issue request.

Omitting `:attributes` accepts all supported fields:

| Attribute | Value / cardinality |
| --- | --- |
| `:github.issue/key` | Identity tuple, one |
| `:github.issue/id`, `:github.issue/number` | Long, one |
| `:github.issue/title`, `:github.issue/state`, `:github.issue/url` | String, one |
| `:github.issue/locked?`, `:github.issue/pull-request?` | Boolean, one |
| `:github.issue/label`, `:github.issue/assignee-login` | String, many |
| `:github.issue/assignee` | Nominal reference, many |

Assignee identity is `[deployment-uuid :github/user 0]`, keyed by GitHub's numeric
user ID. Duplicate collection elements become one fact; empty collections emit
no facts. Missing/mistyped required fields fail. Unused response fields do not
change accepted metadata or fingerprints. These fields describe work items;
GitHub's issue `id` for a pull request is not its separate pull-request ID.

## Persist and reopen

Use `datalevin.federation.install/install!` with the normal source/relation
metadata envelopes. The source body records `:provider :github/issues`, the
accepted `:source-instance-id`, and a qualified `:profile` reference. The relation
body records `:source` and `:accepted accepted`. Supply expected definition
versions as in the [installer example](../README.md#persisted-definitions).
After `load-definitions`, pass the saved relation's accepted mapping to
`github/source` with an explicit runtime profile. Credentials, HTTP clients,
and fetched records never belong in schema definitions. Used API/conversion
contracts are verified on reopen; the installer's dependency manifest is allowed.

Acquisition makes one bounded repository GET and checks its numeric ID against
the accepted mapping. A reused repository name fails as a stale definition.
Redirects fail explicitly; refresh the accepted locator after verifying the
numeric ID. Credentials are never forwarded to a redirect target.

## Access, consistency and failure behavior

Advertised paths are entity lookup and value lookup on `:github.issue/key` or
`:github.issue/number`. Arbitrary label/title filtering has no value index.
Unbound scans, scoped listings, pagination, search, API batch endpoints and
fusion are not implemented. A dependent open accepts at most 256 typed unique
keys and 256 KiB of binding state, then performs bounded per-key GETs. The engine
preserves repeated input witnesses and result multiplicity. A full output row
window does not speculatively request the next key.

Reads are **best-effort**, with unstable replay and no data-version token.
Separate attribute accesses or cursor reopens can observe different updates.
Snapshot and statement requirements reject. No response cache invents a stronger
view. Configure the same source object for aliases that must share a session.
Use separate public compute-context IDs for independently acquired profiles.

Every actual request, including repository verification, acquires a permit from
one session limiter and the shared query ledger. Requests are serialized. There
are no adapter retries: authorization, rate limits, redirects, transport failures,
malformed/truncated JSON, mismatched work-item numbers and over-limit responses
fail the query. Rate-limit responses stop immediately, following GitHub's
[rate-limit guidance](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api#handle-rate-limit-errors-appropriately).
Direct SPI errors include a bounded numeric `Retry-After` when available; the
engine redacts provider details. Applications decide when to run another query.

An issue 404 is an incomplete read, never certified absence: GitHub can mask an
authorization failure with 404. Repository acquisition 401/403/404 fails access.
A 403 with explicit rate-limit headers and a 429 report a limit failure. This
conservative missing-key policy is intentional; a batch containing an unknown
item fails instead of silently dropping it.

## Bounds and accounting

Runtime profile options, in addition to `:compute-context-id` and optional
`:token`, are `:api-base`, `:request-timeout-ms` (10,000),
`:max-response-bytes` (262,144) and `:max-json-nodes` (4,096).
HTTPS is required except for loopback HTTP fixtures. API bases may include a
GitHub Enterprise API prefix. Query timeouts use the ordinary top-level
`:timeout` in milliseconds.

Both headers and bodies obey the total request timeout. Explicit session
cancellation, query cancellation and deadline expiry cancel pending HTTP work
and close streams promptly. The parser caps nesting at 32, nodes, number length
and strings; duplicate object fields and trailing data reject. Responses with
non-identity content encoding reject. A conservative workspace of
`65,536 + 8 * max-response-bytes + 4,096 * max-json-nodes` bytes is reserved before
network/body/parser work. This covers the body, parsed nodes and projected
references; it is about 18.1 MiB with defaults. A cursor retains only the current
work item's selected attribute, bounded inputs and one pending row. Output
windows usually reserve at most 64 KiB; one wider admissible row can expand the
window within the caller's batch cap. All sessions/cursors release owned state
idempotently, including partial acquisition failures.

Cursor metrics report actual requests, consumed success-body bytes, decoded
work items, emitted rows, exact owned batch charges and elapsed fetch time.
`github/statistics` reports cumulative repository acquisition requests/bytes and
live sessions separately. Estimates remain unknown. Error bodies are closed
without being decoded or counted as consumed success bytes. No paths, tokens
or response bodies appear in diagnostics.

## Checks and live evidence

From the companion root:

```sh
clojure -M:test:github
clojure -M:github-live
# Optional explicit public repository/work-item inputs:
clojure -M:github-live octocat Hello-World 1296269 1
```

The live command performs GitHub GETs and creates only a temporary in-memory
native triage database. `GITHUB_TOKEN` is optional for public resources. It is
excluded from the default suite. Per-module `lein test` and `lein jar` exercise
and package the standalone optional artifact with Datalevin provided by the
application. A compatible Datalevin checkout is required for the unreleased SPI.

On 2026-10-22 the live native join returned work item 1, title
`Edited README via GitHub`, state `closed`. The first query took 1,382 ms including
cold startup/acquisition. A second execution's three cursors each made one GET
and consumed 3,262 response bytes; one repository verification consumed 5,085
bytes per invocation. Fetch times were about 31–41 ms per cursor. Both invocations
closed every session. These are observations of a mutable public API, not
calibrated cost estimates or comparative benchmarks.

The deterministic loopback HTTP suite compares advertised lookups with eager
facts and the shared `mapped-lookup-semantics!` gate. It checks scalar/reference
many fields, native selection, persisted identity/reopen, separate repositories,
repeated witnesses, binding IDs, exact narrow/wide batches, changing live replay,
quotas, rate/authentication errors, redirects, malformed bodies, partial setup,
blocked-body timeouts, cancellation and cleanup. The initial F3 API/export/
native workflow and bounded baseline are implemented; same-syntax required-head
parity remains open. See the [workflow](../examples/README.md). Scoped listing and
pagination require a separate advertised-path gate if added later.

Validation (2026-10-22): the complete companion suite passes **116 tests /
23,271 assertions**, including **11 / 151** GitHub checks. Standalone GitHub
`lein test`, `lein jar`, companion lint, and shared-helper lint pass. The
read-only public GitHub/native check passed separately.
