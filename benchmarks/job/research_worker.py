"""HTTP measurement bridge for DuckDB and the unmodified Accio Java optimizer.

An owned child process provides a hard planning/execution deadline. Engine
startup/recovery happens via /prepare, outside the harness's timed calls.
"""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
import os
from pathlib import Path
import re
import signal
import threading
import time

import source_order


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def quote_job_aliases(statement):
    """Quote reserved aliases in pinned JOB SQL, leaving literals/comments intact."""
    token = re.compile(r"(?is)'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\r\n]*|/\*.*?\*/|\b(?:at|character)\b")
    def replace(match):
        if (match[0].lower() in ('at', 'character')
                and (re.match(r'\s*\.', statement[match.end():])
                     or re.search(r'\bAS\s*$', statement[:match.start()], re.I))):
            return identifier(match[0].lower())
        return match[0]
    return token.sub(replace, statement)


def qualify_job(statement):
    """Only qualify the flat FROM list in the pinned JOB grammar."""
    match = re.search(r"(?is)\bFROM\s+(.*?)\bWHERE\b", statement)
    if not match:
        raise ValueError("Expected JOB's FROM/WHERE shape")
    refs = []
    for ref in match[1].split(','):
        pair = re.fullmatch(r"\s*([a-z_]+)\s+AS\s+([a-z_0-9]+)\s*", ref, re.I)
        if not pair:
            raise ValueError("Unexpected JOB table reference")
        refs.append('job.' + identifier(pair[1]) + ' AS ' + identifier(pair[2]))
    return statement[:match.start(1)] + ', '.join(refs) + ' ' + statement[match.end(1):]


def split_queries(statement):
    """Accio separates partition SQL with semicolons; preserve quoted literals."""
    token = re.compile(r"(?s)'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\r\n]*|/\*.*?\*/|;")
    start = 0
    result = []
    for match in token.finditer(statement):
        if match[0] == ';':
            result.append(statement[start:match.start()].strip())
            start = match.end()
    result.append(statement[start:].strip())
    return [q for q in result if q]


class Engine:
    def __init__(self, config):
        import duckdb
        self.config = config
        self.last = None
        self.conn = duckdb.connect(config={
            'extension_directory': config['extension_directory'],
            'memory_limit': '8GiB', 'threads': config['threads'],
            'temp_directory': config['temp_directory'], 'max_temp_directory_size': '16GiB'})
        self.conn.execute('LOAD postgres')
        # Credentials are provided through libpq environment variables, never SQL.
        os.environ.update(config['pg_env'])
        self.conn.execute("ATTACH '' AS job (TYPE postgres, READ_ONLY, SCHEMA "
                          + literal(config['schema']) + ")")
        self.conn.execute("SET search_path=" + literal('memory.main,job.' + config['schema']))
        self.conn.execute('SET pg_statement_timeout_millis=' + str(config['timeout_ms']))
        self.text_min_order = config.get('text_min_order', 'binary')
        self.order_metadata = {'mode': self.text_min_order}
        if self.text_min_order == 'source-libc':
            source = self.conn.execute("SELECT * FROM postgres_query('job', $$"
                "SELECT datlocprovider::text,datcollate,pg_encoding_to_char(encoding),version() "
                "FROM pg_database WHERE datname=current_database()$$)").fetchone()
            overrides = self.conn.execute("SELECT * FROM postgres_query('job', " + literal(
                "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=" + literal(config['schema'])
                + " AND a.attnum>0 AND NOT a.attisdropped AND a.attcollation<>0 AND a.attcollation<>100")
                + ")").fetchone()[0]
            if overrides:
                raise ValueError('source-libc JOB profile does not support column collation overrides')
            self.order_metadata.update(source_order.install(self.conn, source, config['source_identity']))
            self.order_metadata['source'] = source
        elif self.text_min_order != 'binary':
            raise ValueError('Unknown text MIN order')
        if config['engine'] == 'accio':
            import jpype
            jpype.startJVM('-Xmx8g', '-XX:MaxDirectMemorySize=8g',
                          classpath=[config['accio_jar']], convertStrings=True)
            self.rewriter = jpype.JClass('ai.dataprep.accio.FederatedQueryRewriter')()
            dbs = jpype.JClass('java.util.ArrayList')()
            dbs.add('job')
            self.session = self.rewriter.createAccioSession(dbs, None, config['accio_config'])
            # Schema discovery is setup, as with accepted adapter mappings.
            schema = self.session.rootSchema.getSubSchema('job')
            factory = jpype.JClass('org.apache.calcite.sql.type.SqlTypeFactoryImpl')(
                jpype.JClass('org.apache.calcite.rel.type.RelDataTypeSystem').DEFAULT)
            for table in schema.getTableNames():
                schema.getTable(table).getRowType(factory)
        self.metadata = {
            'text_min_order': self.order_metadata,
            'duckdb_version': duckdb.__version__,
            'duckdb_settings': dict(self.conn.execute(
                "SELECT name,value FROM duckdb_settings() WHERE name LIKE 'pg_%' "
                "OR name IN ('threads','memory_limit','max_temp_directory_size')").fetchall()),
            'source_read_only': self.conn.execute(
                "SELECT * FROM postgres_query('job', $$SELECT current_setting('default_transaction_read_only')$$)").fetchall(),
            'extensions': self.conn.execute(
                "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded").fetchall()}

    def rewrite(self, statement):
        submitted = quote_job_aliases(qualify_job(statement))
        plan = self.rewriter.rewrite(submitted, self.session, 'benefit')
        fragments = []
        for i in range(plan.getCount()):
            fragments.append({'db': str(plan.getDBName(i)), 'alias': str(plan.getAliasDBName(i)),
                              'sql': str(plan.getSql(i)).replace('$f', 'ACCIOVAR_').replace('EXPR$', 'ACCIOEXPR_'),
                              'cardinality': float(plan.getCardinality(i))})
        return submitted, fragments

    def register(self, fragments):
        for fragment in fragments:
            if fragment['db'] == 'LOCAL':
                continue
            if fragment['db'] != 'job':
                raise ValueError('Unexpected remote database in Accio plan')
            partitions = split_queries(fragment['sql'])
            # Preserve Accio's generated partition predicates. Stock DuckDB's
            # postgres_query has no old fork's cardinality/partition argument.
            # UNION ALL exposes its partitions as separate scan branches.
            sql = ' UNION ALL '.join('SELECT * FROM postgres_query(\'job\', ' + literal(q) + ')'
                                     for q in partitions)
            self.conn.execute('CREATE TEMP VIEW ' + identifier(fragment['alias']) + ' AS ' + sql)

    def execute(self, statement):
        started = time.perf_counter_ns()
        self.last = {'input_sql': statement}
        planning_ns = 0
        fragments = []
        local = quote_job_aliases(statement)
        if local != statement:
            self.last.update(submitted_sql=local, sql_normalization='quote-reserved-job-aliases')
        if self.config['engine'] == 'accio':
            submitted, fragments = self.rewrite(statement)
            planning_ns = time.perf_counter_ns() - started
            self.last.update(submitted_sql=submitted, fragments=fragments)
            local = next(f['sql'] for f in fragments if f['db'] == 'LOCAL')
        if self.text_min_order == 'source-libc':
            local = source_order.rewrite_minima(local)
        self.last['local_sql'] = local
        self.last['text_min_order'] = self.order_metadata
        self.conn.execute('BEGIN')
        try:
            self.register(fragments)
            rows = self.conn.execute(local).fetchall()
        finally:
            # Also removes the transaction's local temporary views.
            self.conn.execute('ROLLBACK')
        return {'rows': [[None if v is None else str(v) for v in row] for row in rows],
                'optimizer_ns': planning_ns, 'engine_elapsed_ns': time.perf_counter_ns() - started,
                'fragments': len(fragments), 'partition_queries': sum(
                    len(split_queries(f['sql'])) for f in fragments if f['db'] != 'LOCAL')}

    def explain(self, statement):
        if not self.last or self.last['input_sql'] != statement:
            return {'unavailable': 'No retained plan for this statement'}
        plan = dict(self.last)
        if 'local_sql' in plan:
            self.conn.execute('BEGIN')
            try:
                self.register(plan.get('fragments', []))
                plan['duckdb_plan'] = self.conn.execute('EXPLAIN ' + plan['local_sql']).fetchall()
            finally:
                self.conn.execute('ROLLBACK')
        return plan


def child(pipe, config):
    try:
        engine = Engine(config)
        pipe.send({'metadata': engine.metadata})
        while True:
            request = pipe.recv()
            try:
                response = (engine.execute(request['sql']) if request['op'] == 'query'
                            else engine.explain(request['sql']))
                pipe.send(response)
            except Exception as error:
                pipe.send({'error': {'class': type(error).__name__, 'message': str(error)}})
    except BaseException as error:
        try:
            pipe.send({'error': {'class': type(error).__name__, 'message': str(error)}})
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        pipe.close()


class Supervisor:
    def __init__(self, config):
        self.config = config
        self.process = self.pipe = None
        self.metadata = None
        self.restarts = 0
        self.lock = threading.Lock()

    def prepare(self):
        if self.process is None or not self.process.is_alive():
            if self.process is not None:
                self.stop()
            self.pipe, remote = multiprocessing.get_context('spawn').Pipe()
            self.process = multiprocessing.get_context('spawn').Process(target=child, args=(remote, self.config))
            self.process.start()
            remote.close()
            if not self.pipe.poll(120):
                self.stop()
                raise RuntimeError('Engine startup deadline exceeded')
            response = self.pipe.recv()
            if 'error' in response:
                self.stop()
                raise RuntimeError(response['error']['message'])
            self.metadata = response['metadata']
        return {'metadata': self.metadata, 'restarts': self.restarts}

    def cancel_source(self):
        import psycopg
        # This application name is unique to this owned engine run.
        with psycopg.connect(self.config['libpq'], autocommit=True,
                             application_name='job-benchmark-cleanup', connect_timeout=5) as conn:
            conn.execute('SELECT pg_cancel_backend(pid) FROM pg_stat_activity '
                         "WHERE application_name=%s AND state='active' AND pid<>pg_backend_pid()",
                         (self.config['application_name'],)).fetchall()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                active = conn.execute("SELECT count(*) FROM pg_stat_activity WHERE application_name=%s "
                                      "AND state='active'", (self.config['application_name'],)).fetchone()[0]
                if not active:
                    return
                time.sleep(0.1)
            raise RuntimeError('Owned PostgreSQL work did not stop after cancellation')

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            self.process.join(5)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(5)
            self.process.close()
            self.process = None
        if self.pipe is not None:
            self.pipe.close()
            self.pipe = None

    def request(self, request):
        if self.process is None:
            return {'unavailable': 'Worker stopped after failed query'}
        self.pipe.send(request)
        if self.pipe.poll(request['timeout_ms'] / 1000):
            try:
                return self.pipe.recv()
            except EOFError:
                reason = 'Worker exited unexpectedly'
        else:
            reason = 'Client planning/execution deadline exceeded'
        self.stop()
        self.cancel_source()
        self.restarts += 1
        return {'error': {'class': 'WorkerDeadlineOrExit', 'message': reason}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    supervisor = Supervisor(config)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.respond('/prepare', {})

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            self.respond(self.path, request)

        def respond(self, path, request):
            try:
                with supervisor.lock:
                    response = (supervisor.prepare() if path == '/prepare'
                                else supervisor.request(dict(request, op='query' if path == '/query' else 'plan')))
                data = json.dumps(response).encode()
                self.send_response(200)
            except Exception as error:
                data = json.dumps({'error': {'class': type(error).__name__, 'message': str(error)}}).encode()
                self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except BrokenPipeError:
                pass

    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()
        supervisor.cancel_source()
        server.server_close()


if __name__ == '__main__':
    main()
