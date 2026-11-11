# Release triage across Datalevin, a file export, and GitHub

This example joins local issue priorities, release assignments from an EDN Lines
export, and live GitHub work items. It shows how to save mappings, reopen them,
and query all three sources with ordinary `d/q`.

## Run it

Follow the repository's [setup instructions](../README.md#getting-started), then
run from the repository root:

```sh
clojure -M:workflow-live
```

The default repository is `octocat/Hello-World`, with issue number `1`. To choose
another issue, supply the owner, repository name, numeric repository ID, and
issue number:

```sh
clojure -M:workflow-live octocat Hello-World 1296269 1
```

Set `GITHUB_TOKEN` for authenticated access; it is optional for public
repositories. The command makes read-only GitHub requests and creates a
temporary Datalevin database and export. It saves and reopens both mappings,
runs the query and an executed explanation, prints results and diagnostics,
then removes the temporary files.

Each result row contains `[issue-number priority title release-window]`.
The example supplies priority `10` and release window `"now"`; the title comes
from GitHub and can change between runs.

To check the workflow with local fixtures instead of live API requests:

```sh
clojure -M:test:workflow
```

## Adapt the example

The [workflow source](src/datalevin/federation/release_triage.clj) contains the
mappings and queries; the [live runner](src/datalevin/federation/release_triage_live.clj)
shows connection setup and cleanup.

Native records use `:triage/repository`, `:triage/number`, and `:triage/priority`,
all long attributes. An export record looks like:

```clojure
{:issue [1296269 1] :owners [7 8] :window "now" :tags ["release"]}
```

`:issue` is `[repository-id issue-number]`; `:owners` contains GitHub numeric
user IDs. The file mapping uses the same issue and user reference types as the
GitHub mapping, allowing direct joins between them.

| Function or query | Use |
| --- | --- |
| `triage/accept` | Accept a GitHub declaration, a stable export-source UUID, and an inspected export manifest. |
| `triage/install!` | Save both mappings with explicit expected definition versions. |
| `triage/sources` | Reconstruct sources from loaded definitions and runtime profiles. |
| `triage/lookup-query` | Join priorities, titles, and release windows; used by the live runner. |
| `triage/query` | Also require matching labels, assignees, and release owners. |
| `triage/summary-query` | Count matches while retaining export rows and input witnesses through `:with`. |
| `triage/rule-query` | Express the joins through source-qualified Datalog rules. |

Missing values and empty collections contribute no facts. The fuller query
therefore excludes work items without matching labels or owners. Duplicate
values within a collection do not multiply facts; separate export records
remain separate entities and can affect counts when their row identity is
retained.

After `install/load-definitions`, pass the saved catalog and explicit GitHub
and file profiles to `triage/sources`. Keep tokens and file paths in those
profiles. To refresh release assignments, accept a new export manifest and
replace the saved relation using its current definition version. Existing
source objects still refer to their previously accepted export.

## Read behavior

The file is a verified snapshot; GitHub reads are best-effort. The query allows
that combination with `:query-source {:consistency :best-effort}`. Requiring a
global snapshot rejects the GitHub source. There is no atomic snapshot across
all three sources.

Local priority filters narrow the issue numbers sent to GitHub. File acquisition
still copies and validates the complete export. Changed export bytes, incomplete
API responses, and exhausted request or memory budgets fail the query.
See [read guarantees and limits](../README.md#read-guarantees-and-limits) for the
shared behavior and the [GitHub](../github/README.md) and [file](../file/README.md)
guides for adapter configuration.
