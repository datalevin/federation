# Frozen JOB baselines and optimization attribution

**B0: `job-single-source-b0-2026-11-11`** preserves the results available before
the next federation optimizations. Its [baseline directory](../../doc/baselines/job-single-source-b0-2026-11-11/)
contains a compressed snapshot, a per-file SHA-256 manifest, and `SHA256SUMS`.
It includes all current measurement evidence (including earlier optimization
experiments), Trino/Drill and DuckDB/Accio raw results, original failures and
reruns, plans, answers, configuration, source snapshots, analysis programs, and
the 113 original SQL files. [baseline-b0.json](baseline-b0.json) defines its scope.

Measured-resource snapshots and run fingerprints identify what was measured.
Additional source snapshots capture the working trees at freeze time, including
uncommitted source changes. They preserve the code even if repository history
later moves; they do not claim every captured file participated in a measurement.
Database files, data CSVs, dependency caches and engine distributions are excluded.
Their existing provenance limits and artifact/version hashes remain explicit.

## Preserve and verify

Treat a frozen archive and its evidence as append-only. Corrections belong in
a new dated report linked to B0. Future optimizations get new experiment IDs and
output directories. Do not regenerate B0 or overwrite its raw runs.

```sh
python3 benchmarks/job/freeze_baseline.py verify \
  doc/baselines/job-single-source-b0-2026-11-11 --working-evidence
```

This verifies the compressed archive, every archived member and all original
working evidence. Omit `--working-evidence` to verify the preserved archive
independently after moving or modifying the workspace. Preserve `SHA256SUMS`
alongside the manuscript artifacts/version history as the integrity anchor.
To inspect or replay sources, extract into a new private directory:

```sh
mkdir -p benchmarks/job/.work/b0-extracted
tar -xzf doc/baselines/job-single-source-b0-2026-11-11/baseline.tar.gz \
  -C benchmarks/job/.work/b0-extracted
```

The top-level `federation/` and `datalevin/` trees preserve relative layout.
Existing measurement snapshots inside the federation tree retain earlier
versions of the harness and worker. Replay instructions and runtime prerequisites
are in the archived benchmark documentation. Dependency/build outputs must be
recreated using the recorded versions; the archive is not a machine image.

## Fixed optimization reference

The primary Datalevin reference is the first full 113-query format-2 pass in
the current research sequence:

```
doc/measurements/job-duckdb-accio-full113-2026-11-11/duckdb/run.edn
```

It contains matching native and Datalevin answers for **113/113** queries.
Datalevin's total is **137.078 s**, native's **150.422 s**. The subsequent Accio
pass provides context: Datalevin **116.506 s**, native **128.513 s**, also 113/113.
Keep both controls visible. Choose the primary by chronology, not the fastest
baseline or most favorable query subset. Competitor comparisons retain their
own matching subsets (70 or 77 queries) and separate syntax-rerun phases;
those subsets do not define the Datalevin optimization reference.

B0 is an **exploratory reference**, with one timed observation per query and
mode and no explicit warmups. Its totals preserve the evidence; they cannot
by themselves establish statistical significance or causal attribution.

[C01 compatibility validation](../../doc/measurements/job-duckdb-accio-source-libc-full113-2026-11-11.md)
subsequently restores all 113 original answers in DuckDB and Accio using local
source-libc text minima. It is a separately recorded protocol/compatibility
change with additional runtime cost, not an optimizer speedup over B0. The
ledger preserves its parent and source/evidence digest. Use the final chosen
compatibility profile when establishing repeated B1 measurements.

## Compare a subsequent optimizer revision

Copy [experiment-template.json](experiment-template.json) into a new experiment
directory and state the mechanism and hypothesis before measuring. Set `kind`
to `single-change`, `ablation`, `cumulative`, or `validation`. Record the parent
baseline/experiment, both source revisions and source-archive hash. A cumulative
change can establish combined behavior; it cannot isolate one mechanism's gain.

Run the original 113 SQL files with fresh native controls, fixed data/indexes/
statistics, resources, timeout, sample count, warmups, and timing scope. Retain
all raw results, plans, failures, and the candidate source snapshot. Use the
[standard runner](README.md) or [research runner](RESEARCH.md), one competitor
at a time. Then compare:

```sh
clojure -M:job-progress \
  doc/measurements/job-duckdb-accio-full113-2026-11-11/duckdb/run.edn \
  /path/to/candidate/duckdb/run.edn /path/to/experiment.json \
  /path/to/new-progress-report
```

The report validates answers and hashes, permits optimizer/adapter changes,
and rejects changes in the source, host/resources, corpus, sampling, runtime
configuration or timing harness. It keeps every baseline query and reports
per-query time deltas, total elapsed-time speedup, native-control drift, and
changed implementation fingerprints. Any native or Datalevin correctness
regression blocks the total speedup. Inputs are SHA-256 identified, and existing
report directories cannot be overwritten.

Replay the original harness/worker for a direct B0 comparison. The current
worker includes reserved-alias quoting added between the full passes and their
six-query reruns; that changed worker hash is a protocol difference. A new
sample/warmup design or harness/connector change requires a new baseline under
that design. Do not edit old manifests to make incompatible runs compare.

## Paper experiment sequence

1. Preserve B0 as the initial evidence and record it in the
   [experiment ledger](../../doc/experiments/job/index.json).
2. Before the next optimizer change, establish B1 by rerunning the preserved
   implementation under the chosen final harness with repeated, balanced runs.
   Use at least three samples per query to expose variation, repeat independent
   process runs, and balance baseline/candidate run order. Separate cold setup
   from service queries. Freeze the design before comparative tuning. A sample
   count alone does not establish significance.
3. Add O01, O02, ... with one mechanism per step and the preceding variant as
   parent. Keep regressions, all 113 correctness results, sample distributions,
   paired controls and both absolute and relative changes. Label cumulative
   improvements separately from each step's marginal change.
4. Add ablations that disable each mechanism in the combined system; use
   factorial combinations where interactions matter. Retain earlier variants
   so cumulative optimization does not erase the contribution evidence.
5. Attribute gains with recorded planning, source SQL/requests, transfer and
   local-intermediate metrics where available. Missing metrics stay unavailable;
   do not infer bytes, memory, or planning time from total elapsed time. Keep
   protocol validation comparisons distinct from measured optimization runs.
6. Repeat on fixed split-source placements and mixed native EAV/SQL/API
   workloads. JOB over one PostgreSQL source establishes delegation behavior;
   EAV-specific differentiation needs its own controlled mechanisms and ablations.

The ledger begins with B0 and no completed optimization rows. Add entries only
when the corresponding experiment has concrete raw evidence and a source
snapshot; every entry points to its parent and archive digest.
