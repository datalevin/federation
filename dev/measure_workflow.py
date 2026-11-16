#!/usr/bin/env python3
"""Sequential fresh-JVM correctness gate and raw baseline archive, stdlib only."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parent.parent
CASES = [
    dict(name="one-key", native_rows=16, export_rows=16, selected_keys=1, latency_ms=0, batch_rows=64, binding_rows=64),
    dict(name="many-keys", native_rows=16, export_rows=16, selected_keys=8, latency_ms=0, batch_rows=64, binding_rows=64),
    dict(name="split-bindings", native_rows=16, export_rows=16, selected_keys=8, latency_ms=0, batch_rows=1, binding_rows=1),
    dict(name="large-native", native_rows=2048, export_rows=16, selected_keys=1, latency_ms=0, batch_rows=64, binding_rows=64),
    dict(name="large-export", native_rows=16, export_rows=512, selected_keys=1, latency_ms=0, batch_rows=64, binding_rows=64),
    dict(name="request-latency", native_rows=16, export_rows=16, selected_keys=4, latency_ms=5, batch_rows=64, binding_rows=64),
    dict(name="large-input", native_rows=16, export_rows=16, selected_keys=1, latency_ms=0, batch_rows=64, binding_rows=64, input_rows=1024),
]
for case in CASES:
    case.setdefault("input_rows", 1)

def edn_options(options):
    return "{" + " ".join(f":{key.replace('_', '-')} " + (f":{value}" if key == "name" else str(value))
                          for key, value in options.items()) + "}"


def source_manifest():
    files = []
    for root, label, directories in [(ROOT, "federation", ["core/src", "local/src", "file/src", "github/src", "examples/src", "dev/src"]),
                                     (ROOT.parent / "datalevin", "datalevin", ["src", "src/java"])]:
        selected = set()
        for directory in directories:
            selected.update(p for p in (root / directory).rglob("*") if p.is_file() and p.suffix in {".clj", ".java", ".edn"})
        for name in ["deps.edn", "project.clj", "build.clj"]:
            if (root / name).is_file():
                selected.add(root / name)
        if label == "federation":
            selected.add(Path(__file__))
        for path in sorted(selected):
            data = path.read_bytes()
            files.append(dict(path=f"{label}/{path.relative_to(root)}", bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
    return files


def rss_bytes(pid):
    result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=False)
    try:
        return int(result.stdout.strip()) * 1024
    except ValueError:
        return 0


def validate(data):
    first = data["first-query"]
    expected = first["result-sha256"]
    native_digest = None
    for sample in [dict(runs=[first])] + data["samples"]:
        for run in sample["runs"]:
            if run["mode"] != "native":
                if run["result-sha256"] != expected or run["result-rows"] != first["result-rows"]:
                    raise RuntimeError("Complete result digest mismatch")
            else:
                native_digest = native_digest or run["result-sha256"]
                if native_digest != run["result-sha256"]:
                    raise RuntimeError("Native control digest mismatch")
            if run["mode"] in {"composed", "composed-literal", "native-api"}:
                work = run["source-work"]
                requests = work["http"]["repository"] + work["http"]["issues"] + work["export"]["acquisitions"]
                if requests != run["observed-ledger-peak"]["external-requests"]:
                    raise RuntimeError("Observed requests do not match the shared ledger")
                if sum(run["http-key-counts"].values()) != work["http"]["issues"]:
                    raise RuntimeError("Per-key work does not match actual HTTP requests")
    return expected


def summarize(output, runs):
    rows = []
    for case in CASES:
        cases = [data for data in runs if data["options"]["name"] == case["name"]]
        if not cases:
            continue
        expected = {validate(data) for data in cases}
        if len(expected) != 1:
            raise RuntimeError("Cross-fork result digest mismatch")
        for mode in ["composed", "composed-literal", "eager", "native-api", "native"]:
            samples = [run for data in cases for sample in data["samples"] for run in sample["runs"] if run["mode"] == mode]
            latency = sorted(run["elapsed-ns"] / 1e6 for run in samples)
            first = [data["first-query"]["elapsed-ns"] / 1e6 for data in cases]
            row = dict(scenario=case["name"], mode=mode, samples=len(samples), rows=samples[0]["result-rows"],
                       median_ms=statistics.median(latency), min_ms=min(latency), max_ms=max(latency),
                       sample_p95_ms=latency[min(len(latency) - 1, int(.95 * len(latency)))],
                       first_query_median_ms=statistics.median(first) if mode == "composed" else "",
                       composed_plan_only_median_ms=statistics.median(data["plan-only-ns"] / 1e6 for data in cases),
                       http_requests=statistics.median(run["source-work"]["http"]["issues"] + run["source-work"]["http"]["repository"] for run in samples),
                       http_distinct_keys=statistics.median(sum(value > 0 for value in run["http-key-counts"].values()) for run in samples),
                       http_body_bytes=statistics.median(sum(cursor["metrics"]["read-bytes"] or 0 for cursor in run.get("cursors", []) if cursor["source"] == "github") + run["source-work"]["github"]["read-bytes"] for run in samples),
                       file_capture_bytes=statistics.median(run["source-work"]["export"]["read-bytes"] for run in samples),
                       cursor_rows=statistics.median(sum(cursor["metrics"]["rows"] or 0 for cursor in run.get("cursors", [])) for run in samples),
                       observed_retained_bytes=statistics.median(run.get("observed-ledger-peak", {}).get("retained-bytes", 0) for run in samples),
                       observed_spill_bytes=statistics.median(run.get("observed-ledger-peak", {}).get("spill-bytes", 0) for run in samples),
                       sampled_heap_peak_bytes=statistics.median(run["sampled-heap-peak-bytes"] for run in samples),
                       caller_allocated_bytes=statistics.median(run["caller-allocated-bytes"] for run in samples),
                       first_source_batch_ms=statistics.median((run.get("first-batch-ns") or 0) / 1e6 for run in samples) if mode in {"composed", "composed-literal", "native-api"} else "",
                       result_sha256=samples[0]["result-sha256"])
            rows.append(row)
    with (output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--forks", type=int, default=2)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--case", choices=[case["name"] for case in CASES])
    args = parser.parse_args()
    if not 1 <= args.forks <= 4 or not 1 <= args.samples <= 20 or not 0 <= args.warmups <= 10:
        parser.error("fork/sample/warmup bounds exceeded")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    before = source_manifest()
    (output / "sources.json").write_text(json.dumps(before, indent=2) + "\n")
    with tarfile.open(output / "source-snapshot.tar.gz", "w:gz") as archive:
        for entry in before:
            label, relative = entry["path"].split("/", 1)
            root = ROOT if label == "federation" else ROOT.parent / "datalevin"
            archive.add(root / relative, arcname=entry["path"])
    metadata = dict(format_version=1, started_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    platform=platform.platform(), python=sys.version, command=sys.argv,
                    topology="loopback HTTP, Connection: close, serial per-key requests; immutable EDNL; persisted native DB",
                    heap_sample_ms=2, rss_sample_ms=50, freshness="controlled API records fixed for each invocation; export pinned snapshot",
                    cold="fresh JVM and new native/source handles, warm filesystem caches; no cold-machine claim",
                    cpu=platform.processor(), forks=args.forks, samples=args.samples, warmups=args.warmups)
    if sys.platform == "darwin":
        metadata["cpu"] = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
    (output / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    runs = []
    for fork in range(1, args.forks + 1):
        for case in CASES:
            if args.case and case["name"] != args.case:
                continue
            options = dict(case, fork=fork, warmups=args.warmups, samples=args.samples)
            path = output / f"{case['name']}-fork-{fork}.edn"
            command = ["clojure", "-M:workflow-measure", edn_options(options), str(path)]
            print(f"Running {case['name']} fork {fork}", flush=True)
            started = time.monotonic()
            peak = 0
            with path.with_suffix(".log").open("w") as log:
                process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                try:
                    while process.poll() is None:
                        peak = max(peak, rss_bytes(process.pid))
                        if time.monotonic() - started > 900:
                            raise TimeoutError("Measurement JVM exceeded 15 minutes")
                        time.sleep(.05)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
            if process.returncode:
                raise RuntimeError(f"Incomplete measurement: inspect {path.with_suffix('.log')}")
            data = json.loads(Path(str(path) + ".json").read_text())
            validate(data)
            data["process"] = dict(wall_seconds=time.monotonic() - started, sampled_peak_rss_bytes=peak)
            Path(str(path) + ".json").write_text(json.dumps(data, indent=2) + "\n")
            runs.append(data)
    if source_manifest() != before:
        raise RuntimeError("Measured source changed during the run")
    summary = summarize(output, runs)
    print(f"Verified {len(runs)} fresh JVMs, {len(summary)} scenario/mode summaries in {output}", flush=True)

if __name__ == "__main__":
    main()
