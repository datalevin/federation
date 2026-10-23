# Release triage across native data, an export and GitHub

[`release-triage`](src/datalevin/federation/release_triage.clj) is an executable
F3 example using the existing `d/q` engine and production file/GitHub adapters.
It joins native repository/issue priorities, immutable release assignments and
live GitHub titles, labels and assignees. Ordinary input tuples supply release
windows and distinct caller witnesses. The example and development tooling do
not add a release artifact or another evaluator.

## Run it

From the companion root, using a compatible Datalevin checkout:

```sh
clojure -M:test:workflow
clojure -M:workflow-live
# Optional explicit public repository/work-item inputs:
clojure -M:workflow-live octocat Hello-World 1296269 1
```

The live command makes read-only GitHub requests; `GITHUB_TOKEN` is optional for
public repositories and stays in its runtime profile. It writes a temporary
UTF-8 EDN Lines assignment, installs both mappings with native schema, closes and
reopens the database, then executes a three-source query and an executed
explanation. All native/export files and private copies are removed at exit.
This is an actual API/file/native join; controlled HTTP fixtures separately
establish complete eager equivalence under reproducible observations.

The live run on 2026-10-22 returned
`[1 10 "Edited README via GitHub" "now"]` from `octocat/Hello-World`, including
priority 10 from native data and release window `now` from the pinned export.
First composed-query latency was 1,286 ms including acquisition. Each invocation
verified the repository once, performed two issue GETs and captured one 64-byte
export. API consistency was `best-effort` with no data version; the file session
reported its content-bound snapshot token. Reopened definitions supplied both
adapters. No live session or temporary copy remained after execution.

## Data and identity

Native triage attributes are `:triage/repository`, `:triage/number` and
`:triage/priority`, all longs. An external assignment record looks like:

```clojure
{:issue [42 1] :owners [7 7 nil 8] :window "now" :tags ["release"]}
```

`triage/accept` requires an explicit GitHub declaration with relation ID
`:github/work-items`, a persisted export-service UUID and an inspected export
manifest. It derives the file's issue and owner reference types from GitHub's
accepted issue/user domains. Both sources therefore publish the same complete
nominal types and keys for the join. Different repository IDs remain different
issue keys. The file's own assignment-row keys stay export-scoped: repeated
physical records remain separate entities and a new capture does not invent
continuity with an earlier capture.

`triage/definitions` returns source/relation envelopes with qualified runtime
profile references. The release relation depends on the GitHub relation, so
its recorded contract is revalidated during catalog updates. `triage/install!`
uses the existing revision/version-checked installer; callers supply explicit
expected versions and may atomically include native schema edits. Runtime paths,
credentials and fetched records remain outside the catalog.

After `install/load-definitions`, pass the saved catalog and explicit profiles
to `triage/sources`. This constructor performs no I/O. Reopening uses recorded
identity and accepted contracts without new discovery. An export refresh is
explicit acceptance of a new manifest and a version-checked replacement of
`:release/assignments`. Previously constructed handles still name their old
accepted captures; opening a source against bytes that drifted fails.

## Queries and guarantees

`triage/query` is the direct conjunction; `triage/summary-query` retains export
row, label, owner and input-witness identity through `:with` before counting.
Missing windows, absent/empty API labels, absent/empty assignees and empty export
owner collections contribute no matching facts. Duplicate values within one
collection do not multiply facts. Duplicate physical export records can affect
aggregates when their row identity is retained.

`triage/rule-query` composes source-qualified positive rules with native clauses.
The integration gate also uses a native required-argument rule alongside both
mapped rules. Rule heads deduplicate complete head tuples before the caller's
aggregate: duplicate assignment rows supporting the same owner do not create
extra rule answers. These are ordinary queries through the existing compiler,
planner and rule engine. The ordinary eager rule path currently mishandles
required-head annotations; the oracle erases only those execution hints while
preserving bodies and argument order, and compares full rows as well as counts.
The [same-syntax parity gap](../doc/measurements/release-triage-required-heads-2026-10-22.md)
remains open; this slice does not claim the complete F3 parity gate passed.

The invocation permits best-effort API reads and observes a pinned file snapshot.
Per-source overrides can require snapshot consistency for file aliases while
allowing best-effort API access. A global snapshot requirement rejects before
backend I/O. Aliases of the same file source share one capture. There is no
cross-source atomic snapshot or API snapshot token. Controlled fixture
observations stay fixed during oracle comparisons; the public GitHub check
makes no equality claim across separate mutable API reads.

The current structural planner can request more API keys than the final result
contains. The baseline isolates the runtime-threshold predicate from literal
threshold controls. Request counts, file scans and input batch boundaries remain
visible in raw measurements. There are no fused accesses or calibrated costs;
known quota exhaustion and incomplete reads fail the complete query.

## Validation and measurements

The integration gate compares direct conjunctions, aggregates and rules against
complete eager EAV observations using the same accepted nominal identities.
It exercises one-row and larger batches, repeated input witnesses, missing and
many-valued attributes, repository separation, saved catalog reopen/refresh,
shared aliases, source-specific guarantees, request quotas, malformed HTTP,
export drift, blocked-body deadlines and release of every view/reservation.

The [baseline report](../doc/measurements/release-triage-2026-10-22.md) records
sequential fresh-JVM measurements and links raw samples and source snapshots.
To create a new archive without overwriting an existing one:

```sh
python3 dev/measure_workflow.py /tmp/release-triage-repeat \
  --forks 2 --samples 3 --warmups 1
```

The harness fails on wrong/incomplete results, leaked views, mismatched request
accounting, source changes during the run, crashed JVMs or timeout. It includes
small/large native sides, small/large exports, one/many keys, split bindings,
controlled request latency and a large ordinary input relation with one result.
The accepted measurement query explicitly requests window `now`. An initial
unqualified 1,024-row input case exceeded the retained-state quota; its failure
and original snapshot remain archived separately. The scoped 1,024-row case
returns one complete result, and the regression also passes under a 64 MiB cap.
Native-only and eager-local controls are recorded separately from composed
query work; eager capture/materialization and plan-only acquisition costs are
explicit. Existing native entrypoint parity evidence remains in the main
[2026-10-19 report](../../datalevin/doc/measurements/query-source-native-2026-10-19.md).

Validation (2026-10-22): the full companion suite passes **126 tests / 23,379
assertions**, including **10 / 108** integrated workflow checks. Companion,
example and tooling lint reports zero errors/warnings. The public three-source
workflow and 14-JVM bounded baseline passed; 378 source files match the current
checkout and archived hashes. Same-syntax required-head parity remains open.
