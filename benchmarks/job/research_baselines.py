#!/usr/bin/env python3
"""Run DuckDB and Accio on the same static PostgreSQL JOB source as Trino/Drill."""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import urlopen
from urllib.error import HTTPError
import uuid

import baselines as base

ACCIO_COMMIT = 'c997e2b26bc04a4a6e82d74c73e4d90a38160acb'


def free_bridge_port(port):
    # Match HTTPServer's reuse policy: a previous owned listener can leave
    # TIME_WAIT sockets even after it and all of its workers have stopped.
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            raise ValueError('Port ' + str(port) + ' is in use') from error


def connection_config(source, schema, app):
    parsed = urlsplit(source['url'][5:])
    params = dict(parse_qsl(parsed.query))
    params.pop('currentSchema', None)
    options = params.pop('options') + ' -c search_path=' + schema
    params.pop('ApplicationName', None)
    aliases = {'connectTimeout': 'connect_timeout'}
    params = {aliases.get(k, k): v for k, v in params.items()}
    allowed = {'sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'connect_timeout'}
    if set(params) - allowed:
        raise ValueError('Unsupported libpq JDBC parameters: ' + ','.join(sorted(set(params) - allowed)))
    params.update(host=parsed.hostname, port=parsed.port or 5432,
                  dbname=source['identity']['database'], user=source['user'], password=source['password'],
                  options=options, application_name=app, connect_timeout=params.get('connect_timeout', '10'))
    def quoted(value):
        return "'" + str(value).replace('\\', '\\\\').replace("'", "\\'") + "'"
    libpq = ' '.join(k + '=' + quoted(v) for k, v in params.items())
    env = {'PGHOST': str(params['host']), 'PGPORT': str(params['port']),
           'PGDATABASE': params['dbname'], 'PGUSER': params['user'], 'PGPASSWORD': params['password'],
           'PGOPTIONS': options, 'PGAPPNAME': app, 'PGCONNECT_TIMEOUT': str(params['connect_timeout'])}
    for name in ('sslmode', 'sslcert', 'sslkey', 'sslrootcert'):
        if name in params:
            env['PG' + name.upper()] = params[name]
    jdbc_params = dict(parse_qsl(parsed.query))
    jdbc_params.update(ApplicationName=app, options=options)
    jdbc = 'jdbc:' + urlunsplit(parsed._replace(query=urlencode(jdbc_params)))
    return libpq, env, jdbc


def run_engine(engine, args, source):
    run = args.output / engine
    run.mkdir(mode=0o700)
    app = 'job-' + engine + '-' + uuid.uuid4().hex[:12]
    libpq, pg_env, jdbc_url = connection_config(source, args.schema, app)
    conf = run / 'private'
    conf.mkdir(mode=0o700)
    jar = args.accio_source / 'rewriter/target/accio-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar'
    resources = {'duckdb_memory_limit': '8GiB', 'duckdb_spill_limit': '16GiB',
                 'threads': args.threads, 'accio_java_heap': '8GiB', 'accio_java_direct': '8GiB',
                 'accio_partition_max_parallelism': 8}
    source_config = {
        'type': 'POSTGRES', 'driver': 'org.postgresql.Driver', 'url': jdbc_url,
        'username': source['user'], 'password': source['password'],
        'costParams': {'join': 2.0, 'agg': 2.0, 'sort': 2.0, 'trans': 1.0},
        'cardEstType': 'postgres', 'partitionType': 'postgres',
        'partition': {'max_parallelism': 8}, 'dialect': 'postgres', 'disableOps': []}
    base.write_json(conf / 'job.json', source_config)
    base.write_json(conf / 'local.json', {'type': 'MANUAL', 'dialect': 'postgres', 'schema': {}})
    config = {'engine': engine, 'schema': args.schema, 'timeout_ms': args.timeout_ms,
              'threads': args.threads, 'accio_jar': str(jar), 'accio_config': str(conf),
              'extension_directory': str(args.extensions), 'temp_directory': str(run / 'spill'),
              'pg_env': pg_env, 'libpq': libpq, 'application_name': app}
    config.update(text_min_order=args.text_min_order, source_identity=source['identity'])
    base.write_json(conf / 'worker.json', config)
    environment = {
        'format_version': 1, 'engine': engine, 'source': source['identity'], 'resources': resources,
        'platform': platform.platform(), 'architecture': platform.machine(), 'cpus': os.cpu_count(),
        'physical_memory_bytes': base.physical_memory(), 'client_java': base.java_version(os.environ.get('JAVA_HOME'), 21),
        'runner_sha256': base.sha256(Path(__file__)),
        'config_sha256': {'worker': base.sha256(base.HERE / 'research_worker.py'),
                          'source_order': base.sha256(base.HERE / 'source_order.py'),
                          'requirements': base.sha256(base.HERE / 'research-requirements.txt')},
        'semantics': {'text_min_order': args.text_min_order, 'oracle': 'original PostgreSQL SQL'},
        'corpus_manifest_sha256': base.sha256(base.HERE / 'corpus.sha256'),
        'dataset_provenance': base.dataset_provenance(args.dataset_provenance),
        'revisions': {'federation': base.revision(base.ROOT), 'datalevin': base.revision(base.ROOT.parent / 'datalevin')},
        'accio_commit': ACCIO_COMMIT, 'accio_jar_sha256': base.sha256(jar) if jar.exists() else None,
        'accio_source_settings': {k: v for k, v in source_config.items() if k not in ('url', 'username', 'password')},
        'python_packages': subprocess.check_output([str(args.python), '-m', 'pip', 'freeze'], text=True).splitlines(),
        'extension_sha256': {str(p.relative_to(args.extensions)): base.sha256(p)
                             for p in args.extensions.rglob('*.duckdb_extension')},
        'transport': 'local Python HTTP bridge; IPC/HTTP overhead included in client wall time',
        'accio_execution': 'unmodified Java benefit optimizer; stock DuckDB postgres_query/UNION ALL bridge; '
                           'upstream cardinality hint retained in diagnostics but unsupported by stock postgres_query',
    }
    manifest = run / 'environment.json'
    free_bridge_port(args.port)
    endpoint = 'http://127.0.0.1:' + str(args.port)
    print('Starting ' + engine + '; checkpoints and logs: ' + str(run), flush=True)
    with (run / 'service.log').open('wb') as log:
        process = subprocess.Popen([str(args.python), str(base.HERE / 'research_worker.py'),
                                    '--config', str(conf / 'worker.json'), '--port', str(args.port)],
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 150
            while True:
                if process.poll() is not None:
                    raise RuntimeError('Engine bridge exited; inspect service.log')
                try:
                    with urlopen(endpoint + '/prepare', timeout=125) as response:
                        ready = json.load(response)
                    break
                except ConnectionError:
                    if time.monotonic() > deadline:
                        raise RuntimeError('Engine bridge startup deadline')
                    time.sleep(0.2)
                except OSError as error:
                    if isinstance(error, HTTPError):
                        detail = error.read().decode()
                        base.private_write(run / 'startup-error.json', detail)
                        raise RuntimeError('Engine startup failed; inspect ' + str(run / 'startup-error.json')) from error
                    if getattr(error, 'code', None) or time.monotonic() > deadline:
                        raise
                    time.sleep(0.2)
            environment['runtime'] = ready
            base.write_json(manifest, environment)
            options = base.edn_options(engine, endpoint, source, args, manifest)
            base.private_write(run / 'options.edn', options + '\n')
            env = dict(os.environ, POSTGRES_JOB_URL=source['url'], POSTGRES_JOB_USER=source['user'],
                       POSTGRES_JOB_PASSWORD=source['password'])
            with (run / 'client.log').open('wb') as client_log:
                client = subprocess.Popen(['clojure', '-M:job-baselines', options, str(run / 'run.edn')],
                                          cwd=base.ROOT, env=env, stdout=client_log, stderr=subprocess.STDOUT,
                                          start_new_session=True)
                try:
                    if client.wait() != 0:
                        raise RuntimeError('Measurement failed; inspect ' + str(run / 'client.log'))
                finally:
                    base.stop(client)
        finally:
            base.stop(process)
            for file in conf.glob('*.json'):
                file.unlink()
    print('Completed ' + engine + '; worker stopped', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('engine', choices=['duckdb', 'accio', 'all'])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--dataset-provenance', required=True)
    parser.add_argument('--corpus', type=Path, default=base.ROOT.parent / 'datalevin/benchmarks/JOB-bench/queries')
    parser.add_argument('--python', type=Path, default=base.HERE / '.work/python-engines/bin/python')
    parser.add_argument('--extensions', type=Path, default=base.HERE / '.work/duckdb-extensions')
    parser.add_argument('--accio-source', type=Path, default=base.HERE / '.work/accio-src')
    parser.add_argument('--schema', default='public')
    parser.add_argument('--text-min-order', choices=['binary', 'source-libc'], default='source-libc',
                        help='Local text MIN ordering; source-libc requires the same-host libc locale')
    parser.add_argument('--queries', type=base.query_ids)
    parser.add_argument('--samples', type=int, choices=range(1, 11), default=1)
    parser.add_argument('--warmups', type=int, choices=range(4), default=0)
    parser.add_argument('--timeout-ms', type=int, default=300000)
    parser.add_argument('--threads', type=int, default=os.cpu_count())
    parser.add_argument('--port', type=int, default=18081)
    args = parser.parse_args()
    os.umask(0o077)
    for key in ('output', 'python', 'extensions', 'accio_source', 'corpus'):
        # Preserve the venv interpreter path; resolving its symlink loses the venv.
        setattr(args, key, getattr(args, key).absolute())
    if not 1000 <= args.timeout_ms <= 300000 or not 1 <= args.threads <= os.cpu_count():
        parser.error('Invalid timeout or thread count')
    base.verify_corpus(args.corpus, args.queries)
    if args.engine in ('accio', 'all'):
        if base.revision(args.accio_source) != {'commit': ACCIO_COMMIT, 'dirty': False}:
            parser.error('Accio source must be the unmodified pinned commit ' + ACCIO_COMMIT)
    source = base.source_config(os.environ.get('POSTGRES_JOB_URL'), os.environ.get('POSTGRES_JOB_USER'),
                                os.environ.get('POSTGRES_JOB_PASSWORD'), args.schema, args.timeout_ms)
    args.output.mkdir(parents=True, mode=0o700)
    engines = ['duckdb', 'accio'] if args.engine == 'all' else [args.engine]
    for engine in engines:
        run_engine(engine, args, source)
    subprocess.run(['clojure', '-M:job-baselines-report', str(args.output / 'summary'),
                    *[str(args.output / e / 'run.edn') for e in engines]], cwd=base.ROOT, check=True)
    print('Report: ' + str(args.output / 'summary/README.md'))


if __name__ == '__main__':
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print('Error: ' + str(error), file=sys.stderr)
        sys.exit(1)
