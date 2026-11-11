# JOB federation benchmark

Compare Datalevin's PostgreSQL adapter with native PostgreSQL, Trino, Apache
Drill, DuckDB, and Accio on the original 113 Join Order Benchmark (JOB) queries.
Datalevin runs equivalent Datalog through its PostgreSQL adapter. Every engine
reads the same existing PostgreSQL database. The runners measure
client wall time, check answers against PostgreSQL, save results and plans,
and stop the services they start.

## Current baseline results

Reference measurements from **2026-11-11**, on macOS ARM64 with 12 logical CPUs,
36 GiB RAM, and PostgreSQL 18.6 using UTF-8 and `en_US.UTF-8` collation:

| Baseline | Matching answers / 113 | Queries in timing total | Native PostgreSQL, s | Datalevin, s | Baseline, s |
| --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB 2.0.0 + source-libc | 113 | 113 | 134.836 | 120.128 | 240.034 |
| Accio + DuckDB 2.0 + source-libc | 113 | 113 | 126.619 | 115.443 | 174.528 |
| Trino 484 | 77 | 70 | 52.106 | 41.834 | 131.152 |
| Apache Drill 1.22.0 | 106 | 70 | 50.086 | 41.840 | 316.702 |

Answer coverage refers to all 113 queries. Timing totals sum per-query client
wall times over the stated subset, with separate native/Datalevin controls for
each row. The Trino and Drill totals use the **same 70 matching queries**;
DuckDB and Accio totals use **all 113**. Every native and Datalevin control
matches the original answer on all 113 queries.

DuckDB and Accio use the **source-libc compatibility profile** to preserve
PostgreSQL's text-minimum ordering. Its local Python sort-key cost is included.
Accio uses its unmodified optimizer through a standard DuckDB execution bridge;
this differs from its original custom scanner. The profile requires a source
on the same host using a compatible libc locale.

Trino has 36 text-collation mismatches. Drill has seven five-minute timeouts;
its coverage includes documented syntax-equivalent inequality reruns. Failed
and mismatching queries remain in the raw results and are excluded from the
matching-subset timing totals above. Dingo has no measurement because a runnable
artifact was unavailable.

These are exploratory measurements with **one sample per query and no explicit
warmups**. The native answer oracle can warm caches, and timings vary between
passes.

## Requirements

- Linux or macOS, Python **3.12+**, Clojure CLI, and **Java 21** for the benchmark
  client. Use a compatible Datalevin **1.2.10 development checkout** at
  `../datalevin` and follow the [checkout/build instructions](../../README.md#getting-started).
- An existing PostgreSQL JOB database with primary keys, foreign-key lookup
  indexes, and collected statistics. Use a SELECT-only account where available.
- Enough memory for PostgreSQL, the client and one competitor together. The
  reference JVM ceilings are 8 GiB heap and 8 GiB direct memory; DuckDB has an
  8 GiB memory limit and 16 GiB spill limit. Accio also starts a JVM. These are
  configuration limits; actual peak RSS and network bytes are not measured.

Install the runtimes needed for the engines you want to run:

| Engine | Additional runtime | Default service ports |
| --- | --- | --- |
| Trino 484 | Java 25, version 25.0.1 or later | 18080 |
| Apache Drill 1.22.0 | Java 17 | 18047 and RPC 31010–31012 |
| DuckDB 2.0.0 | Python environment and PostgreSQL extension | 18081 |
| Accio + DuckDB | DuckDB environment, Java 21 and Maven | 18081 |

Engines run sequentially and can share a service port across passes. No Docker
is required. [versions.json](versions.json) pins the Trino/Drill distributions
and JDBC download checksums. [research-requirements.txt](research-requirements.txt)
and the [DuckDB/Accio installation guide](RESEARCH.md#install) pin their Python
packages and Accio source revision.

## Prepare the data and connection

Use the May 2013 IMDb export from the [original JOB](https://github.com/gregrahn/join-order-benchmark),
available as [imdb.tgz](https://event.cwi.nl/da/job/imdb.tgz), with the supplied
21-table [schema](../../../datalevin/benchmarks/JOB-bench/data/schema.sql) and
[lookup indexes](../../../datalevin/benchmarks/JOB-bench/data/fkindexes.sql).
Import once into a dedicated database and collect statistics before measuring.
The sibling [PostgreSQL loader](../../../datalevin/benchmarks/JOB-bench/src/datalevin_bench/postgres.clj)
drops/recreates tables, loads CSVs, builds indexes and runs `ANALYZE`; use it
only for an isolated import. Keep data, indexes and statistics fixed between
engine passes.

The runners use `../datalevin/benchmarks/JOB-bench/queries` by default and verify
all 113 SQL files against [corpus.sha256](corpus.sha256). `--corpus` can select
another copy of the same pinned queries; `--schema` defaults to `public`.

From this repository's root, configure the client and source:

```sh
export JAVA_HOME=/path/to/jdk-21
export POSTGRES_JOB_URL='jdbc:postgresql://127.0.0.1:5432/job'
export POSTGRES_JOB_USER=job_reader
export POSTGRES_JOB_PASSWORD='your-local-password'
```

Omit the password variable for local authentication without a password. Simple
database/schema identifiers are supported. Both runners enforce read-only
source connections and the selected statement timeout. They do not load data,
create indexes or run maintenance `ANALYZE`.

Prepare a text import manifest containing data-file checksums, schema/index
checksums, import procedure, encoding/collation and exact row counts. Supply its
path through `--dataset-provenance`; the runner embeds its contents and hash.
A descriptive import identifier is accepted for local exploration. Recorded
catalog row estimates are not exact data-identity checks.

## Run Trino or Drill

Set the service JVMs and download the pinned distributions:

```sh
export TRINO_JAVA_HOME=/path/to/jdk-25
export DRILL_JAVA_HOME=/path/to/jdk-17
python3 benchmarks/job/baselines.py fetch all
```

Use `fetch trino` or `fetch drill` to install one engine. Downloads for both
are about 1.3 GB compressed and are verified before extraction. Downloads and
installations default to ignored `benchmarks/job/.work`. Global `--home` and
`--downloads` flags go before `fetch` or `run`.

Start with a two-query smoke run:

```sh
python3 benchmarks/job/baselines.py run all \
  --queries 6a,11a --timeout-ms 30000 \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/trino-drill-smoke
```

Run all 113 queries:

```sh
python3 benchmarks/job/baselines.py run all \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/trino-drill-full113
```

Replace `run all` with `run trino` or `run drill` for a single comparison.
Each competitor gets fresh native and Datalevin controls.

## Run DuckDB or Accio

Follow the [installation guide](RESEARCH.md#install) to create the Python
environment, install DuckDB's PostgreSQL extension and build Accio if needed.
Then run all 113 queries with matching source text-minimum semantics:

```sh
python3 benchmarks/job/research_baselines.py all --text-min-order source-libc \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/duckdb-accio-full113
```

Replace `all` with `duckdb` or `accio` for one engine. For a smoke run, add
`--queries 1a,1b,10c,15d,18a,27a --timeout-ms 30000` and use a new output directory.
`--text-min-order binary` selects the earlier binary text ordering, which can
differ from the source's answers. [Profile details and limits](RESEARCH.md#postgresql-text-minimum-compatibility)
explain the required locale/provider checks and the measured local sorting cost.

## Sampling and timing

Both runners default to **one sample, zero explicit warmups, and a 300-second
per-call deadline**. Add `--samples 3 --warmups 1` to either run command to retain
repeated observations. Use a new output directory for every run. Allow about an
hour for the reference Trino/Drill coverage pass, dominated by timeouts, and
longer for repeated samples.

Client wall time includes planning, cardinality estimation, connector work,
execution, result consumption, decoding and cleanup. DuckDB/Accio local SQL
adaptation and source-libc sorting are included. Service/JVM startup, schema/
mapping discovery, Datalevin SQL-to-Datalog translation, native answer oracles,
explicit warmups and diagnostic plan capture are outside the timed calls.

An untimed native answer oracle precedes each query's measurements and can warm
source caches. Mode order rotates by query and sample. The source must remain
static: native and Datalevin use read-only repeatable-read transactions,
while connector transaction behavior can differ. Correctness checks compare
final aggregate answers, including NULLs, repeated output columns and collation.

## Read the results

Each run creates a summary and retains the evidence used to calculate it:

| Output | Contents |
| --- | --- |
| `summary/README.md` and `summary/summary.json` | Coverage, exclusions and totals over the common matching query set |
| `summary/queries.csv` and `summary/samples.csv` | Per-query and per-sample timings, statuses and call order |
| `summary/plans.edn` | Captured plans and generated SQL |
| `<engine>/run.edn` | Raw answers, samples, failures and diagnostics |
| `<engine>/environment.json` | Versions, resources, source identity and implementation hashes |
| `<engine>/client.log` and `service.log` | Progress and service diagnostics |

A query enters a matching-subset total only when every timed sample in every
compared mode/run returns the original native answer. Failures and mismatches
stay visible. The reports also retain all attempted time, including failures;
that total can have a different denominator from correct-answer timings.

Regenerate a report from compatible version-2 runs into a new directory:

```sh
clojure -M:job-baselines-report /path/to/new-summary \
  /path/to/duckdb/run.edn /path/to/accio/run.edn
```

The report checks SQL hashes, answer flags, sampling, source settings and
implementation compatibility. Historical format-1 runs retain their dated
analysis programs in the linked reports. Use the [before/after guide](BASELINES.md)
when comparing optimizer revisions or preserving a new reference.

Output directories are never overwritten. Progress is checkpointed after each
query. A competitor query failure is recorded and the pass continues; a failed
native oracle or setup aborts the pass. Ctrl-C stops owned services. Retain an
interrupted checkpoint and start a new run. Keep run directories private and
review logs/profiles before publishing them.
