#!/usr/bin/env python3
"""Pinned, sequential, single-source JOB baselines. Python 3.12+, POSIX."""

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
VERSIONS = json.loads((HERE / "versions.json").read_text())
RESOURCES = json.loads((HERE / "config/resources.json").read_text())


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def private_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        stream.write(text)


def write_json(path, value):
    private_write(path, json.dumps(value, indent=2) + "\n")


def fetch(name, downloads):
    spec = VERSIONS[name]
    downloads.mkdir(parents=True, exist_ok=True)
    archive = downloads / spec["archive"]
    if not archive.exists():
        print(f"Downloading {name} {spec['version']}", flush=True)
        temporary = archive.with_suffix(".part")
        try:
            with urlopen(spec["url"], timeout=60) as response, temporary.open("wb") as out:
                shutil.copyfileobj(response, out)
            if sha256(temporary) != spec["sha256"]:
                raise ValueError(f"Checksum mismatch: {name}")
            temporary.replace(archive)
        finally:
            temporary.unlink(missing_ok=True)
    if sha256(archive) != spec["sha256"]:
        raise ValueError(f"Checksum mismatch: {archive}; remove it and fetch again")
    return archive


def install(name, home, downloads):
    spec = VERSIONS[name]
    destination = home / "engines" / spec["directory"]
    marker = destination / ".job-artifact-sha256"
    if destination.exists():
        if not marker.exists() or marker.read_text().strip() != spec["sha256"]:
            raise ValueError(f"Unrecognized installation: {destination}; choose a new --home")
        return destination
    archive = fetch(name, downloads)
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Extracting {name} {spec['version']}", flush=True)
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        with tarfile.open(archive) as tar:
            # Only these Trino plugins are used. Geospatial is a required built-in.
            def selected(member):
                parts = Path(member.name).parts
                return (name != "trino" or len(parts) < 3 or parts[1] != "plugin"
                        or parts[2] in {"postgresql", "geospatial"})
            tar.extractall(temporary, members=(m for m in tar if selected(m)), filter="data")
        extracted = Path(temporary) / spec["directory"]
        (extracted / ".job-artifact-sha256").write_text(spec["sha256"] + "\n")
        extracted.rename(destination)
    return destination


def source_config(url, user, password, schema, timeout_ms):
    if not url or not url.startswith("jdbc:postgresql://"):
        raise ValueError("Set POSTGRES_JOB_URL=jdbc:postgresql://host:port/database")
    parsed = urlsplit(url[5:])
    # Keep source identity unambiguous and credentials out of URLs and manifests.
    if parsed.username or parsed.password or not parsed.hostname or parsed.fragment:
        raise ValueError("Use POSTGRES_JOB_USER/PASSWORD, not credentials in the URL authority")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", schema):
        raise ValueError("--schema must be a simple PostgreSQL identifier")
    database = parsed.path.removeprefix("/")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", database):
        raise ValueError("Use a simple database name in POSTGRES_JOB_URL")
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    if len(dict(pairs)) != len(pairs):
        raise ValueError("Duplicate JDBC URL parameters are ambiguous")
    params = dict(pairs)
    url_user, url_password = params.pop("user", None), params.pop("password", None)
    user = user if user is not None else url_user
    password = password if password is not None else url_password
    if not user:
        raise ValueError("Set POSTGRES_JOB_USER (or the user JDBC parameter)")
    if "options" in params:
        raise ValueError("The runner owns JDBC 'options' to enforce read-only statements")
    params.update(currentSchema=schema,
                  options=f"-c default_transaction_read_only=on -c statement_timeout={timeout_ms}")
    clean = "jdbc:" + urlunsplit(parsed._replace(query=urlencode(params)))
    return {"url": clean, "user": user, "password": password or "",
            "identity": {"host": parsed.hostname, "port": parsed.port or 5432,
                         "database": database, "schema": schema}}


def properties(values):
    def escape(value):
        # ASCII Java .properties, including non-ASCII and leading spaces.
        out = []
        for char in str(value):
            if char in "\\ =:#!":
                out.append("\\" + char)
            elif char in "\r\n\t":
                out.append({"\r": "\\r", "\n": "\\n", "\t": "\\t"}[char])
            elif ord(char) > 127:
                raw = char.encode("utf-16-be")
                out.extend(f"\\u{int.from_bytes(raw[i:i+2], 'big'):04x}" for i in range(0, len(raw), 2))
            else:
                out.append(char)
        return "".join(out)
    return "".join(f"{escape(k)}={escape(v)}\n" for k, v in values.items())


def java_version(home, expected=None):
    java = str(Path(home) / "bin/java") if home else "java"
    text = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT, text=True)
    match = re.search(r'version "(\d+)', text)
    if not match or (expected and int(match[1]) != expected) or int(match[1]) < 17:
        raise ValueError(f"Expected Java {expected or '17+'}: {java}")
    return text.strip()


def revision(path):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()
    return {"commit": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain"))}


def physical_memory():
    if sys.platform == "darwin":
        return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))
    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")


def dataset_provenance(value):
    path = Path(value)
    if path.is_file():
        return {"manifest_sha256": sha256(path), "manifest": path.read_text()}
    return {"description": value}


def verify_corpus(corpus, queries):
    expected = dict(line.split() for line in (HERE / "corpus.sha256").read_text().splitlines())
    names = set(expected.values())
    if {p.name for p in corpus.glob("*.sql")} != names:
        raise ValueError("--corpus must contain the pinned 113 JOB SQL files")
    for digest, name in expected.items():
        if sha256(corpus / name) != digest:
            raise ValueError(f"JOB SQL checksum mismatch: {name}")
    if queries and not {q + ".sql" for q in queries}.issubset(names):
        raise ValueError("Unknown JOB query ID")


def free_port(port):
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError as error:
            raise ValueError(f"Port {port} is in use; stop that service or choose another port") from error


def http(endpoint, path, body=None):
    request = Request(endpoint + path, data=None if body is None else json.dumps(body).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def ready(process, engine, endpoint, log):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{engine} exited during startup; inspect {log}")
        try:
            response = http(endpoint, "/v1/info" if engine == "trino" else "/profiles.json")
            if engine != "trino" or not response.get("starting", True):
                return
        except (OSError, ValueError):
            pass
        time.sleep(1)
    raise RuntimeError(f"{engine} startup timed out; inspect {log}")


def stop(process):
    if process.poll() is None:
        if process.stdin:
            try:
                process.stdin.write(b"!quit\n")
                process.stdin.flush()
                process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                pass
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
    if process.stdin:
        try:
            process.stdin.close()
        except OSError:
            pass  # The child can exit before consuming the final quit command.


@contextlib.contextmanager
def service(engine, distribution, run, source, args, java_home, jdbc):
    port = args.trino_port if engine == "trino" else args.drill_port
    free_port(port)
    if engine == "drill":
        for rpc_port in (31010, 31011, 31012):
            free_port(rpc_port)
    endpoint = f"http://127.0.0.1:{port}"
    conf = run / "config"
    conf.mkdir()
    env = dict(os.environ, JAVA_HOME=java_home)
    if engine == "trino":
        private_write(conf / "jvm.config", (HERE / "config/trino-jvm.config").read_text())
        private_write(conf / "config.properties", properties({
            "coordinator": "true", "node-scheduler.include-coordinator": "true",
            "http-server.http.port": port, "discovery.uri": endpoint,
            "query.max-run-time": f"{args.timeout_ms}ms", **RESOURCES["trino"]}))
        private_write(conf / "node.properties", properties({
            "node.environment": "job_benchmark", "node.id": "job-trino-484",
            "node.data-dir": run / "data", "node.internal-address": "127.0.0.1",
            "node.bind-ip": "127.0.0.1"}))
        private_write(conf / "catalog/job.properties", properties({
            "connector.name": "postgresql", "connection-url": source["url"],
            "connection-user": source["user"], "connection-password": source["password"]}))
        command = [str(distribution / "bin/launcher"), "--etc-dir", str(conf), "run"]
    else:
        private_write(conf / "drill-override.conf", json.dumps({"drill": {"exec": {
            "cluster-id": "job-benchmark", "rpc": {"bind_addr": "127.0.0.1"},
            "http": {"enabled": True, "bind_addr": "127.0.0.1", "port": port},
            "sys": {"store": {"provider": {"local": {"path": str(run / "data")}}}},
            "spill": {"directories": [str(run / "spill")]},
            "udf": {"directory": {"root": str(run / "udf")}}
        }}}, indent=2))
        # The distribution launcher loads jars from a site configuration directory.
        (conf / "jars").mkdir()
        shutil.copy2(jdbc, conf / "jars" / VERSIONS["jdbc"]["archive"])
        env.update(RESOURCES["drill"], DRILL_CONF_DIR=str(conf), DRILL_LOG_DIR=str(run / "logs"))
        command = [str(distribution / "bin/drill-embedded"), "--silent=true"]
    log = run / "service.log"
    with log.open("wb") as output:
        process = subprocess.Popen(command, env=env, cwd=run, stdout=output,
                                   stderr=subprocess.STDOUT,
                                   stdin=subprocess.PIPE if engine == "drill" else subprocess.DEVNULL,
                                   start_new_session=True)
        try:
            ready(process, engine, endpoint, log)
            if engine == "drill":
                response = http(endpoint, "/storage/job.json", {"name": "job", "config": {
                    "type": "jdbc", "enabled": True, "driver": "org.postgresql.Driver",
                    "url": source["url"], "username": source["user"], "password": source["password"],
                    "caseInsensitiveTableNames": False, "sourceParameters": {"readOnly": True}}})
                # 1.22 returns "Success"; older REST documentation uses "success".
                if str(response.get("result")).lower() != "success":
                    raise RuntimeError("Drill JDBC storage registration failed")
            yield endpoint
        finally:
            stop(process)
            # JDBC credentials are regenerated on the next run; don't retain them.
            (conf / "catalog/job.properties").unlink(missing_ok=True)
            if engine == "drill":
                shutil.rmtree(run / "data", ignore_errors=True)


def edn_options(engine, endpoint, source, args, manifest):
    queries = "nil" if args.queries is None else "[" + " ".join(":" + q for q in args.queries) + "]"
    return ("{:engine :" + engine + " :endpoint " + json.dumps(endpoint)
            + " :corpus " + json.dumps(str(args.corpus.resolve()))
            + " :schema " + json.dumps(args.schema)
            + " :drill-schema " + json.dumps("job." + source["identity"]["database"] + "." + args.schema)
            + f" :queries {queries} :samples {args.samples} :warmups {args.warmups}"
            + f" :timeout-ms {args.timeout_ms} :environment-file " + json.dumps(str(manifest)) + "}")


def run_engine(engine, args, source):
    java_home = os.environ.get(engine.upper() + "_JAVA_HOME")
    if not java_home:
        raise ValueError(f"Set {engine.upper()}_JAVA_HOME to Java {VERSIONS[engine]['java']}")
    engine_java = java_version(java_home, VERSIONS[engine]["java"])
    client_java = java_version(os.environ.get("JAVA_HOME"), 21)
    distribution = install(engine, args.home, args.downloads)
    jdbc = fetch("jdbc", args.downloads) if engine == "drill" else None
    run = args.output / engine
    run.mkdir(mode=0o700)
    manifest = run / "environment.json"
    environment = {
        "format_version": 1, "engine": engine, "artifacts": VERSIONS,
        "engine_java": engine_java, "client_java": client_java,
        "platform": platform.platform(), "architecture": platform.machine(), "cpus": os.cpu_count(),
        "physical_memory_bytes": physical_memory(),
        "source": source["identity"], "resources": RESOURCES,
        "source_read_only": True, "source_statement_timeout_ms": args.timeout_ms,
        "revisions": {"federation": revision(ROOT), "datalevin": revision(ROOT.parent / "datalevin")},
        "runner_sha256": sha256(Path(__file__)),
        "corpus_manifest_sha256": sha256(HERE / "corpus.sha256"),
        "config_sha256": {p.name: sha256(p) for p in sorted((HERE / "config").iterdir())},
        "dataset_provenance": dataset_provenance(args.dataset_provenance),
    }
    write_json(manifest, environment)
    env = dict(os.environ, POSTGRES_JOB_URL=source["url"], POSTGRES_JOB_USER=source["user"],
               POSTGRES_JOB_PASSWORD=source["password"])
    print(f"Starting {engine}; logs and checkpoints: {run}", flush=True)
    with service(engine, distribution, run, source, args, java_home, jdbc) as endpoint:
        options = edn_options(engine, endpoint, source, args, manifest)
        private_write(run / "options.edn", options + "\n")
        with (run / "client.log").open("wb") as log:
            process = subprocess.Popen(["clojure", "-M:job-baselines", options, str(run / "run.edn")],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            try:
                if process.wait() != 0:
                    raise RuntimeError(f"{engine} measurement failed; inspect {run / 'client.log'}")
            finally:
                stop(process)
    print(f"Completed {engine}; service stopped", flush=True)


def query_ids(value):
    ids = value.split(",")
    if len(set(ids)) != len(ids) or not all(re.fullmatch(r"\d+[a-z]", q) for q in ids):
        raise argparse.ArgumentTypeError("Use comma-separated query IDs, e.g. 6a,11a")
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, default=HERE / ".work")
    parser.add_argument("--downloads", type=Path, help="Cache containing pinned archives; checksums always verified")
    commands = parser.add_subparsers(dest="command", required=True)
    fetch_parser = commands.add_parser("fetch", help="Fetch and install pinned distributions")
    fetch_parser.add_argument("engine", choices=["trino", "drill", "all"], default="all", nargs="?")
    run = commands.add_parser("run", help="Start, measure, and stop each engine sequentially")
    run.add_argument("engine", choices=["trino", "drill", "all"])
    run.add_argument("--output", required=True, type=Path, help="New output directory; never overwrite a run")
    run.add_argument("--corpus", type=Path, default=ROOT.parent / "datalevin/benchmarks/JOB-bench/queries")
    run.add_argument("--schema", default="public")
    run.add_argument("--queries", type=query_ids, help="Default: all 113 queries")
    run.add_argument("--samples", type=int, choices=range(1, 11), default=1)
    run.add_argument("--warmups", type=int, choices=range(4), default=0)
    run.add_argument("--timeout-ms", type=int, default=300000)
    run.add_argument("--trino-port", type=int, default=18080)
    run.add_argument("--drill-port", type=int, default=18047)
    run.add_argument("--dataset-provenance", required=True,
                     help="Public dataset/import identifier or manifest path; no credentials")
    args = parser.parse_args()
    args.home = args.home.resolve()
    args.downloads = (args.downloads or args.home / "downloads").resolve()
    engines = ["trino", "drill"] if args.engine == "all" else [args.engine]
    os.umask(0o077)
    if args.command == "fetch":
        for engine in engines:
            install(engine, args.home, args.downloads)
            if engine == "drill":
                fetch("jdbc", args.downloads)
        return
    if not 1000 <= args.timeout_ms <= 300000:
        parser.error("--timeout-ms must be 1000..300000")
    if not args.corpus.is_dir():
        parser.error("--corpus must contain the original 113 SQL files")
    verify_corpus(args.corpus, args.queries)
    for port in [args.trino_port, args.drill_port]:
        if not 1024 <= port <= 65535:
            parser.error("HTTP ports must be 1024..65535")
    source = source_config(os.environ.get("POSTGRES_JOB_URL"), os.environ.get("POSTGRES_JOB_USER"),
                           os.environ.get("POSTGRES_JOB_PASSWORD"), args.schema, args.timeout_ms)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, mode=0o700)  # fail rather than mixing experiments
    for engine in engines:
        run_engine(engine, args, source)
    subprocess.run(["clojure", "-M:job-baselines-report", str(args.output / "summary"),
                    *[str(args.output / e / "run.edn") for e in engines]], cwd=ROOT, check=True)
    print(f"Report: {args.output / 'summary/README.md'}")


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
