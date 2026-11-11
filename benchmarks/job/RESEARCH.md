# DuckDB, Accio, and Dingo on the PostgreSQL JOB source

This extends the [JOB baseline workflow](README.md) with DuckDB's PostgreSQL
extension and Accio's optimizer. Both read the same existing, static PostgreSQL
database and use the same original 113 queries, native answer oracle, rotating
call order, and paired native/Datalevin measurements. No database import or
source tuning is performed by this runner.

The [B0 preservation and attribution protocol](BASELINES.md) freezes the current
results and measured-source snapshots before further optimizer changes.

The [C01 full validation](../../doc/measurements/job-duckdb-accio-source-libc-full113-2026-11-11.md)
matches **113/113 original answers in both engines**, with zero failures or
mismatches, using the source-libc profile below. It retains fresh full passes,
paired controls and the added local sort-key cost separately from B0.

The [historical first full-corpus measurement](../../doc/measurements/job-duckdb-accio-full113-2026-11-11.md)
retains both original passes and reserved-alias reruns. Both engines ultimately
match 77 queries; 36 text-minimum answers differ by collation. On the same 77
matching queries, DuckDB totals 83.5 s and Accio 65.3 s, versus their paired
Datalevin totals of 49.8 s and 45.4 s. These are single observations across two
explicit phases, not a repeated performance estimate.

## Install

Use Python 3.12, Java 21, Maven, and the Clojure/Datalevin prerequisites in the
main benchmark README. From the repository root, provision a private workspace:

```sh
mkdir -p benchmarks/job/.work
python3.12 -m venv benchmarks/job/.work/python-engines
benchmarks/job/.work/python-engines/bin/python -m pip install \
  -r benchmarks/job/research-requirements.txt
git clone https://github.com/sfu-db/accio.git benchmarks/job/.work/accio-src
git -C benchmarks/job/.work/accio-src checkout c997e2b26bc04a4a6e82d74c73e4d90a38160acb
mvn -B -ntp -f benchmarks/job/.work/accio-src/rewriter/pom.xml \
  -Dmaven.repo.local="$PWD/benchmarks/job/.work/m2" -Dmaven.test.skip=true package
benchmarks/job/.work/python-engines/bin/python - <<'PY'
from pathlib import Path
import duckdb
path = Path('benchmarks/job/.work/duckdb-extensions').absolute()
path.mkdir(parents=True, exist_ok=True)
conn = duckdb.connect(config={'extension_directory': str(path)})
conn.execute("INSTALL postgres FROM 'https://extensions.duckdb.org'")
conn.execute('LOAD postgres')
conn.close()
PY
```

[research-requirements.txt](research-requirements.txt) pins DuckDB 2.0.0 and
JPype 1.7.1. The runner verifies the clean Accio source commit and records the
built jar hash, extension hashes, Python packages, runtime settings, source
identity, and implementation hashes. Accio uses its bundled Calcite 1.36.0 and
PostgreSQL JDBC 42.3.3. Its Maven transitive dependencies/plugins are upstream's;
the recorded jar hash identifies the actual build. Preserve those build
artifacts when publishing results. Do not run upstream database setup scripts
against an existing comparison source.

## Run

Set `POSTGRES_JOB_URL`, `POSTGRES_JOB_USER`, and optionally
`POSTGRES_JOB_PASSWORD` as in the main README. The bridge uses free loopback
port 18081 by default. Start with representative queries:

```sh
python3 benchmarks/job/research_baselines.py all \
  --queries 1a,1b,10c,15d,18a,27a --timeout-ms 30000 \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/research-smoke
```

Run a full coverage pass, sequentially, with fresh paired controls per engine:

```sh
python3 benchmarks/job/research_baselines.py all \
  --dataset-provenance /path/to/import-manifest.txt \
  --output benchmarks/job/.work/research-full113
```

Use `duckdb` or `accio` instead of `all` for one engine. Defaults are one sample,
zero explicit warmups, and 300 seconds per query, including optimizer work.
`--samples 3 --warmups 1` retains repeated observations. Output directories
must be new. The same report generator validates correctness and compares only
the common matching subset. Raw answers, exclusions, plans, timings, failures,
and environment manifests remain available in the output directory.

## PostgreSQL text-minimum compatibility

The runner now defaults to `--text-min-order source-libc` for the same-host
PostgreSQL JOB setup. It uses the source database's libc locale to compare text
minima locally. Source probes cover punctuation, currency, case, accents,
Unicode normalization and long strings. The profile requires a loopback source,
matching OS family, UTF-8, PostgreSQL's libc collation provider and database-default
column collations. Unsupported configurations fail explicitly; this is not a
general remote-collation implementation.

[source_order.py](source_order.py) registers a DuckDB Python UDF that calls
`strxfrm_l` with a private locale object. The local execution SQL replaces `MIN`
with a typed macro: text uses `arg_min` over libc sort keys with UTF-8 byte ties;
numeric arguments keep native `MIN`. Each minimum independently ignores NULLs;
empty/all-NULL inputs return NULL. DuckDB and Accio keep their selected joins,
source SQL fragments, predicates and projections. The original corpus and
PostgreSQL oracle remain unchanged. Actual rewritten local SQL and ordering
metadata are retained in every plan. Key computation is local, without looking
up query answers or sending result rows back to PostgreSQL.

NumPy 2.3.5 is pinned because DuckDB requires it for Python UDF registration.
Sort-key computation and macro binding/execution are inside measured query time.
Locale/source metadata discovery and fixed compatibility probes are startup.
The profile can cost more than binary ordering; measure that cost with the
original answers rather than changing their collation to match the engine.

Use `--text-min-order binary` to retain the earlier binary-order semantics.
The chosen mode, helper hash and runtime details are explicit in manifests.
Reports reject comparisons between different modes as optimizer improvements.
The reserved-alias fixes apply in both modes. Frozen B0 results and artifacts
are unchanged; the source-libc profile is a separately measured compatibility
change that requires a new measurement reference before optimizer attribution.

## Integration and timing

**DuckDB** attaches PostgreSQL read-only and runs JOB SQL over live remote
tables using its default PostgreSQL extension optimization. Data is not
copied into persistent DuckDB tables. The PostgreSQL extension's
[documentation](https://duckdb.org/docs/2.0/core_extensions/postgres/overview)
describes this execution model.

**Accio + DuckDB 2.0 compatibility integration** calls the unmodified Java
`benefit` optimizer from [Accio](https://github.com/sfu-db/accio) through JPype.
The runner qualifies JOB tables into Accio's `job` source schema and converts
inequality tokens `!=` to equivalent `<>` for its Calcite parser. Original SQL,
submitted SQL, generated fragments, and cardinality estimates are retained.
Source costing and partition settings follow upstream's JOB DuckDB 10 Gbit/s
configuration: join/aggregate/sort cost 2, transfer cost 1, maximum partition
parallelism 8. This is a configuration assumption, not measured link bandwidth.

Both bridges quote the reserved aliases `at` and `character` where used as
identifiers. This changes only 10b, 10c, and 15a–15d in the pinned corpus and
preserves literals, comments, data predicates, and outputs. Original SQL stays
in the raw run; actual engine input stays in its diagnostic plan. Regression
checks cover all 113 queries. The first measurement's unquoted parser failures
remain archived alongside the successful follow-up measurements.

Accio's original Python DuckDB integration depends on an old custom scanner
with a three-argument `postgres_query`. Stock DuckDB 2.0 has a
[different interface](https://duckdb.org/docs/2.0/core_extensions/postgres/functions).
The bridge registers Accio's generated source SQL as local temporary views of
stock `postgres_query`, expressing semicolon-separated partition queries as
`UNION ALL`. It preserves source SQL and CTID partition predicates. Stock
DuckDB does not accept the old cardinality hint and controls scheduling of
these branches. This is **not an exact reproduction of Accio's custom scanner
or published two-source experiment**. There is no manually chosen SQL
delegation or fallback to the native answer in either measured engine.

Client wall time includes HTTP/IPC overhead, query optimization and cardinality
estimation, view registration, execution, result consumption, and cleanup.
Accio's rewrite duration and total worker duration are also retained separately.
Service/JVM startup, table/column discovery, native answer oracles, and diagnostic
plan capture are outside timing. The upstream Accio session and estimate cache
persist between queries. Setup and any recovery are logged; query failures
remain failed measurements. After a hard deadline, the bridge stops its worker
and cancels only source work with that run's unique application name, then
starts a fresh worker before the next timed call.

Outside benchmark passes, exercise the real deadline, source cancellation, and
worker recovery against the configured read-only source with:

```sh
benchmarks/job/.work/python-engines/bin/python benchmarks/job/check_research_worker.py
```

This checks an owned ten-second `pg_sleep` query against a one-second client
deadline, then verifies that the replacement worker can answer a new query.

DuckDB has an 8 GiB memory limit, a 16 GiB spill limit, and defaults to the host's
logical CPU count (12 on the reference machine). Accio additionally has an
8 GiB JVM heap and 8 GiB direct-memory ceiling. The Clojure client retains its
8 GiB heap/direct ceilings. These are configuration limits, not equal RSS
budgets; actual peak memory and network bytes are not measured. PostgreSQL
connections enforce read-only mode and the selected statement timeout.

Binary local text aggregation can differ from the source's `en_US.UTF-8`
collation. Every answer in either mode is checked against the original native
result. Any remaining mismatches stay visible and are excluded from correct-result
speed comparisons. An ICU `en_US` substitution is insufficient here: it differs
from Darwin libc on currency strings in JOB 18a.

## Dingo availability

The [Dingo paper](https://www.vldb.org/2025/Workshops/VLDB-Workshops-2025/CDMS/CDMS25_07.pdf)
and [author's project page](https://gsvic.github.io/) did not provide a runnable
artifact in the search on 2026-11-11. Public repository searches found none,
and the user had no artifact link. Dingo is **not run: artifact unavailable**,
not an execution failure or a measured performance result. Adding a Dingo
baseline requires an identifiable implementation and a compatible source setup;
an independent reconstruction of the paper would be a different system.
