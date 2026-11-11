"""Optional live, read-only deadline/recovery check; run outside measurements."""
import json
import os
import tempfile
import uuid

import baselines
import research_baselines
from research_worker import Supervisor


def main():
    source = baselines.source_config(os.environ.get('POSTGRES_JOB_URL'),
                                    os.environ.get('POSTGRES_JOB_USER'),
                                    os.environ.get('POSTGRES_JOB_PASSWORD'), 'public', 30000)
    app = 'job-recovery-check-' + uuid.uuid4().hex[:12]
    libpq, env, _ = research_baselines.connection_config(source, 'public', app)
    with tempfile.TemporaryDirectory(dir=baselines.HERE / '.work') as directory:
        worker = Supervisor({'engine': 'duckdb', 'schema': 'public', 'threads': 1,
                             'timeout_ms': 30000, 'pg_env': env, 'libpq': libpq,
                             'application_name': app, 'temp_directory': directory,
                             'extension_directory': str(baselines.HERE / '.work/duckdb-extensions')})
        try:
            first = worker.prepare()
            assert first['metadata']['source_read_only'] == [('on',)]
            expired = worker.request({'op': 'query', 'timeout_ms': 1000,
                                      'sql': "SELECT * FROM postgres_query('job', $$SELECT 1 AS n FROM pg_sleep(10)$$)"})
            assert expired['error']['class'] == 'WorkerDeadlineOrExit', expired
            recovered = worker.prepare()
            assert recovered['restarts'] == 1, recovered
            answer = worker.request({'op': 'query', 'timeout_ms': 1000, 'sql': 'SELECT 1'})
            assert answer['rows'] == [['1']], answer
            print(json.dumps({'read_only': True, 'deadline': expired,
                              'restarts': recovered['restarts'], 'recovered_query': answer}))
        finally:
            worker.stop()
            worker.cancel_source()


if __name__ == '__main__':
    main()
