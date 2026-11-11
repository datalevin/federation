import re
import unittest
from urllib.parse import parse_qs, urlsplit

import baselines
import research_baselines as research
import research_worker as worker


class ResearchTests(unittest.TestCase):
    def test_reserved_alias_quoting_changes_only_six_job_queries(self):
        corpus = baselines.ROOT.parent / 'datalevin/benchmarks/JOB-bench/queries'
        changed = []
        for path in corpus.glob('*.sql'):
            original = path.read_text()
            quoted = worker.quote_job_aliases(original)
            self.assertEqual(original, quoted.replace('"at"', 'at').replace('"character"', 'character'))
            if original != quoted:
                changed.append(path.stem)
        self.assertEqual(['10b', '10c', '15a', '15b', '15c', '15d'], sorted(changed))
        sql = "SELECT 'at.title AS character', \"character\", at.title AS character -- at.title\n/* AS at */ FROM aka_title AS at"
        self.assertEqual("SELECT 'at.title AS character', \"character\", \"at\".title AS \"character\" -- at.title\n/* AS at */ FROM aka_title AS \"at\"",
                         worker.quote_job_aliases(sql))

    def test_qualification_preserves_job_predicates_and_outputs(self):
        corpus = baselines.ROOT.parent / 'datalevin/benchmarks/JOB-bench/queries'
        files = list(corpus.glob('*.sql'))
        self.assertEqual(113, len(files))
        for path in files:
            sql = path.read_text()
            qualified = worker.qualify_job(sql)
            before, after = re.split(r'\bWHERE\b', sql, maxsplit=1)
            self.assertEqual(after, qualified.split('WHERE', 1)[1])
            self.assertEqual(before.split('FROM', 1)[0], qualified.split('FROM', 1)[0])
            refs = re.findall(r'job\."([a-z_]+)" AS "([a-z_0-9]+)"', qualified)
            self.assertEqual(re.findall(r'([a-z_]+)\s+AS\s+([a-z_0-9]+)',
                                        before.split('FROM', 1)[1]), refs)
        with self.assertRaises(ValueError):
            worker.qualify_job('SELECT x FROM title JOIN cast_info ON true WHERE true')

    def test_partition_split_keeps_semicolons_in_literals_and_comments(self):
        first = 'SELECT \'a;it\'\'s\', "a;column" /* ; */ -- ;\nFROM t'
        self.assertEqual([first, 'SELECT 2'], worker.split_queries(first + ';\nSELECT 2;'))

    def test_libpq_and_jdbc_share_read_only_identity_and_deadline(self):
        source = baselines.source_config(
            'jdbc:postgresql://localhost:5433/imdb?sslmode=require',
            'reader', "a'\\b", 'public', 1234)
        libpq, env, jdbc = research.connection_config(source, 'public', 'unique-worker')
        self.assertIn("password='a\\'\\\\b'", libpq)
        self.assertEqual('unique-worker', env['PGAPPNAME'])
        self.assertEqual('require', env['PGSSLMODE'])
        self.assertEqual("a'\\b", env['PGPASSWORD'])
        params = parse_qs(urlsplit(jdbc[5:]).query)
        self.assertEqual([env['PGOPTIONS']], params['options'])
        self.assertIn('default_transaction_read_only=on', env['PGOPTIONS'])
        self.assertIn('statement_timeout=1234', env['PGOPTIONS'])
        self.assertEqual(['unique-worker'], params['ApplicationName'])
        self.assertNotIn('password', params)


if __name__ == '__main__':
    unittest.main()
