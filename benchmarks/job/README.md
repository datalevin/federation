# JOB federation baselines

Compare Datalevin's PostgreSQL adapter, native PostgreSQL, Trino's PostgreSQL
connector, and Apache Drill's JDBC plugin on the original 113 JOB queries.
All four read the same static PostgreSQL database. The launcher starts one
competitor at a time, measures client wall time, retains answers and plans,
and stops the service when the pass finishes or fails.

The [first full comparison](../../doc/measurements/job-competitors-full113-2026-11-11.md)
found 113 matching answers for Datalevin, 77 for Trino (36 text-collation
differences), and 106 for Drill (seven five-minute timeouts after equivalent
inequality spelling conversion). Those are historical coverage measurements,
not repeated performance estimates. Use this workflow for new runs; the dated
report retains its original harness snapshots and evidence.

## Prerequisites

- Linux or macOS, Python **3.12+**, Clojure CLI, and a compatible Datalevin
  **1.2.10 development checkout** at `../datalevin`. Follow the repository's
  [checkout/build instructions](../../README.md#getting-started) before running.
- Set `JAVA_HOME` to **Java 21** for the client, `TRINO_JAVA_HOME` to **Java 25**
  (25.0.1 or later), and `DRILL_JAVA_HOME` to **Java 17**. These reproduce the
  baseline JVM majors; JDKs are supplied by the operator. Exact JVM versions
  are recorded. Trino's [deployment requirements](https://trino.io/docs/current/installation/deployment.html)
  describe its supported runtime.
- An existing PostgreSQL JOB import with its primary keys, foreign-key lookup
  indexes, and collected statistics. The reference source is PostgreSQL 18.6,
  UTF-8, `en_US.UTF-8` collation. Use a SELECT-only account where available.
- Memory for PostgreSQL, the client, and one competitor together. The reference
  machine had 36 GiB RAM and 12 logical CPUs. The configured ceilings below
  are substantial; they are not measurements of actual peak use.
- Free loopback HTTP ports 18080 (Trino), 18047 (Drill), and Drill RPC ports
  31010–31012. The HTTP ports can be changed with CLI flags. No Docker required.

[versions.json](versions.json) pins Trino **484**, Drill **1.22.0**, and Drill's
PostgreSQL JDBC **42.7.13** distribution URLs and SHA-256 digests. Trino includes
its own JDBC 42.7.14. The client JDBC version is pinned in [deps.edn](../../deps.edn).
The installer verifies downloaded/cached bytes before extraction. It installs
only Trino's PostgreSQL and required geospatial plugins; no other connectors
participate. Installed directories are reused, so keep them unmodified.

## Data and source configuration

Use the May 2013 IMDb export used by the
[original JOB](https://github.com/gregrahn/join-order-benchmark), available as
[imdb.tgz](https://event.cwi.nl/da/job/imdb.tgz), with the 21-table
[schema](../../../datalevin/benchmarks/JOB-bench/data/schema.sql) and
[lookup indexes](../../../datalevin/benchmarks/JOB-bench/data/fkindexes.sql)
in the matching Datalevin checkout. The queries are in
`../datalevin/benchmarks/JOB-bench/queries`; `--corpus` can select another copy
of those 113 files. The launcher verifies every SQL file against
[corpus.sha256](corpus.sha256) before starting a service. This benchmark uses the original SQL outputs, including
repeated columns and SQL NULL behavior.

For a new import, prepare an **empty, dedicated database** using the upstream
JOB loading instructions or the sibling
[PostgreSQL loader](../../../datalevin/benchmarks/JOB-bench/src/datalevin_bench/postgres.clj).
That loader expects CSVs under `benchmarks/JOB-bench/data`, defaults to the
database `postgres`, drops/recreates its tables, loads CSV with a backslash
escape, builds the supplied indexes, and runs `ANALYZE`. Use it only against
an isolated installation intended for that import. Choose and record encoding
and collation when creating the database. Do not reload or tune an existing
comparison database between engine passes.

Preserve the dataset archive/CSV checksums, schema/index checksums, import
procedure, and exact row counts in a public text manifest. For example,
`shasum -a 256 /path/to/job-data/*.csv` produces the CSV checksum portion.
Pass the manifest with `--dataset-provenance`; its contents and SHA-256 are
embedded in each run. A descriptive import identifier is also accepted for
local exploration, but is weaker provenance. Source catalog estimates in the
raw report are explicitly labeled estimates; they do not verify exact data
identity. Publish the import manifest for a reproducible performance claim.

From this repository's root:

```sh
export JAVA_HOME=/path/to/jdk-21
export TRINO_JAVA_HOME=/path/to/jdk-25
export DRILL_JAVA_HOME=/path/to/jdk-17
export POSTGRES_JOB_URL='jdbc:postgresql://127.0.0.1:5432/job'
export POSTGRES_JOB_USER=job_reader
export POSTGRES_JOB_PASSWORD='your-local-password'
```

Omit the password variable for local authentication without a password. A JDBC
`user` parameter is also accepted. The runner removes user/password URL
parameters before writing configuration and puts credentials into private
generated configuration. Simple database/schema identifiers are supported;
use `--schema public` to override the schema. Other JDBC parameters, such as
`sslmode`, are retained. The runner owns `currentSchema` and `options`, enforcing
`default_transaction_read_only=on` and the selected source statement timeout.
It never loads data, creates indexes, or runs maintenance `ANALYZE`.

## Run

Download and verify the distributions once (about 1.3 GB compressed):

```sh
python3 benchmarks/job/baselines.py fetch all
```

By default, downloads and installations live under ignored
`benchmarks/job/.work`. Global `--home /path/to/work` and
`--downloads /path/to/cache` flags go **before** `fetch` or `run`. A shared cache
must contain the archive names in `versions.json`; existing archives are
checksum-checked. A changed/unrecognized installation requires a fresh home.

Start with two queries, including one requiring Drill's inequality conversion:

```sh
python3 benchmarks/job/baselines.py run all \
  --queries 6a,11a --timeout-ms 30000 \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/smoke
```

Run the full 113-query coverage pass:

```sh
python3 benchmarks/job/baselines.py run all \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/full113
```

Defaults are **one sample, zero explicit warmups, and 300 seconds per call**.
The historical full coverage pass took about an hour, dominated by Drill
timeouts; allow substantially longer for repeated samples. To measure repeated
observations, use a new output directory:

```sh
python3 benchmarks/job/baselines.py run all --samples 3 --warmups 1 \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/repeated113
```

`run trino` and `run drill` run individual comparisons, each with fresh paired
native/Datalevin controls. Neither output directories nor raw result files are
overwritten. Progress and exceptions go to each engine's `client.log`; its
checkpointed `run.edn` grows after each query. Ctrl-C stops owned processes.
An interrupted pass remains incomplete; start a new run instead of silently
mixing old and new samples. An engine query failure is retained and the pass
continues; a failed native oracle or setup aborts the pass.

## Read and regenerate reports

The launcher creates `OUTPUT/summary/README.md`, `summary.json`, `queries.csv`,
`samples.csv`, and `plans.edn`. The per-engine directories retain the full
`run.edn`, options, environment manifest, and service/client logs. Raw runs
include query hashes, original/submitted SQL, answer rows, sample order,
warmups, failures, PostgreSQL version/collation/settings, source table/index/
statistics metadata, adapter budgets, JVM/host details, repository revisions,
and 33 measured implementation fingerprints. The environment records pinned
artifacts and config/runner hashes. Credentials are excluded from the manifest;
generated catalog/storage credentials are removed on normal cleanup. Keep run
directories private and inspect engine logs/profiles before publishing them.

Regenerate a report from any compatible version-2 runs into a new directory:

```sh
clojure -M:job-baselines-report /path/to/new-summary \
  /path/to/trino/run.edn /path/to/drill/run.edn
```

The report rejects incomplete runs, duplicate engines, changed SQL hashes,
inconsistent answer flags, and differences in paired protocols, source
metadata, expected answers, or measured implementation. Runs from different
hosts/settings/implementations should be reported separately. Version-1 dated
runs retain their archived analysis scripts.

A query enters the common comparison only when **every timed sample in every
mode and run returns the original native answer**. The report sums per-query
medians over that identical subset, preserving separate native/Datalevin
controls for each competitor. It also reports all attempted call time, including
failures and mismatches, with a different denominator. It never labels an
incomplete matching subset as a full-suite speed comparison. Individual samples
and exclusions stay visible. Three samples support a better descriptive estimate
than one; they do not by themselves establish statistical significance.

## Measurement contract

- Timed client calls include submission, planning, execution, result consumption,
  decoding, and normal cleanup. Native uses fresh JDBC connections; competitors
  use their HTTP query APIs and consume all response pages. Datalevin times `d/q`
  through source cleanup. JVM/service startup, mapping discovery, SQL translation,
  answer oracles, warmups, plan/profile capture, and report writing are excluded.
- An untimed native answer oracle precedes each query's measurements and can warm
  PostgreSQL's caches. Mode order rotates by query and sample; the order is in
  every record. This is not a cold-cache test. Each competitor pass gets a fresh
  client JVM and fresh service state. Avoid other workloads during measurement.
- Native/Datalevin use read-only repeatable-read transactions. Connector
  transaction behavior can differ; the source must remain static throughout.
  The oracle checks final aggregate results, not all intermediate tuples.
- Datalevin requests PostgreSQL's native text-extrema semantics. Trino's text
  ordering can differ. Such differences remain mismatches; no collation rewrite
  is applied to the oracle or measured SQL.
- Drill receives token-level `!=` → `<>` normalization, preserving literals,
  quoted identifiers and comments. These are equivalent PostgreSQL
  [comparison operators](https://www.postgresql.org/docs/18/functions-comparison.html).
  Both original and submitted SQL are retained. Native and Datalevin continue
  using the original corpus. All 113 parsed query structures are regression-tested.
- Connector optimization uses default automatic settings. There are no eager
  join-pushdown overrides, passthrough queries, or per-query source tuning.
  Trino EXPLAIN and Drill executed profiles (EXPLAIN fallback after failure) are
  captured outside timing. Inspect them before attributing work to an engine.
- [Resource settings](config/resources.json) and [Trino JVM flags](config/trino-jvm.config)
  set 8 GiB heap and 8 GiB direct-memory ceilings per competitor; the client has
  the same ceilings. Trino query memory is 6 GiB with 2 GiB heap headroom.
  Datalevin allows 8 GiB retained query state and 16,384 external requests.
  Configured ceilings are not equal measured RSS budgets. Actual wire bytes,
  source request counts, and whole-process peak RSS are not measured.
- Client deadlines and source statement timeouts use the selected limit;
  cancellation/cleanup can add elapsed time. Failures are never counted as fast
  successful answers. Diagnostic failures are retained independently.

This is a single-source delegation/overhead baseline. Distributed scaling,
split PostgreSQL placement, and native EAV/SQL/API workloads remain separate
[federation milestones](../../federation.md#job-federation-comparisons).

## Development checks

```sh
python3 -m unittest discover -s benchmarks/job -p 'test_*.py'
clojure -M:test datalevin.federation.job-competitors-test
```

The two-query smoke command above validates real installation, service startup,
read-only execution, plan capture, report generation, and shutdown. Use the full
suite when changing query semantics or making performance claims. Publish both
repository revisions (and any uncommitted source changes), the data manifest,
raw runs, and summary together.

The [workflow smoke evidence](../../doc/measurements/job-baselines-smoke-2026-11-11/README.md)
records successful runs of both engines and the focused validation checks.
