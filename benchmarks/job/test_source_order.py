import functools
import json
import platform
import unittest

import baselines
import source_order

try:
    import duckdb
except ImportError:
    duckdb = None


class OrderTests(unittest.TestCase):
    def setUp(self):
        try:
            self.order = source_order.LibcOrder('en_US.UTF-8')
        except (ValueError, AttributeError):
            self.skipTest('Local libc en_US.UTF-8 locale unavailable')

    def test_keys_follow_comparator_including_currency_unicode_and_long_values(self):
        values = ['', 'A', 'a', '€ 1,000', '$1,000', 'Öga', 'Spår', 'é', 'é', 'x' * 12000]
        actual = sorted(values, key=lambda value: (self.order.key(value), value.encode()))
        def compare(a, b):
            primary = self.order.compare(a, b)
            return primary or ((a.encode() > b.encode()) - (a.encode() < b.encode()))
        self.assertEqual(sorted(values, key=functools.cmp_to_key(compare)), actual)
        with self.assertRaisesRegex(ValueError, 'NUL'):
            self.order.key('a\0b')

    @unittest.skipUnless(platform.system() == 'Darwin', 'B0 uses Darwin libc')
    def test_all_observed_binary_text_minimum_differences_choose_the_source_value(self):
        path = baselines.ROOT / 'doc/measurements/job-duckdb-accio-full113-2026-11-11/diagnostics.json'
        queries = json.loads(path.read_text())['results'][0]['collation-diagnostics']
        checked = 0
        for query in queries:
            for expected, binary in zip(query['original-answer'][0], query['engine-answer'][0]):
                if isinstance(expected, str) and expected != binary:
                    checked += 1
                    self.assertEqual(expected, min([expected, binary], key=lambda v: (self.order.key(v), v.encode())), query['query'])
        self.assertEqual(46, checked)

    def test_sql_rewrite_preserves_literals_comments_and_quoted_names(self):
        sql = 'SELECT min (a), MIN(b), \'MIN(x)\', "MIN" -- MIN(c)\n/* MIN(d) */ FROM t'
        self.assertEqual('SELECT job_source_min (a), job_source_min(b), \'MIN(x)\', "MIN" -- MIN(c)\n/* MIN(d) */ FROM t',
                         source_order.rewrite_minima(sql))

    def test_refuses_unmatched_source_locale_providers_or_remote_hosts(self):
        for source, identity in [(('i', 'en_US', 'UTF8', platform.system()), {'host': 'localhost'}),
                                 (('c', 'en_US.UTF-8', 'UTF8', platform.system()), {'host': 'remote'})]:
            with self.assertRaisesRegex(ValueError, 'same-host'):
                source_order.install(None, source, identity)

    @unittest.skipUnless(duckdb, 'DuckDB is installed in the benchmark venv')
    def test_typed_aggregate_handles_numeric_null_empty_and_independent_minima(self):
        conn = duckdb.connect()
        # Register the real key function; same definition installed by the live
        # integration, after its source guard/probe checks.
        conn.create_function('job_source_sort_key', self.order.key, ['VARCHAR'], 'BLOB')
        conn.execute(source_order.MIN_MACRO)
        row = conn.execute("SELECT job_source_min(s), job_source_min(n), typeof(job_source_min(n)) "
                           "FROM (VALUES ('(as A)',10),('(Set)',-2),(NULL,NULL)) t(s,n)").fetchone()
        self.assertEqual(('(as A)', -2, 'INTEGER'), row)
        self.assertEqual((None, None), conn.execute(
            'SELECT job_source_min(s),job_source_min(n) FROM (SELECT NULL::VARCHAR s, NULL::INTEGER n WHERE false)t').fetchone())
        self.assertEqual((None, -2), conn.execute(
            'SELECT job_source_min(s),job_source_min(n) FROM (VALUES (NULL::VARCHAR,10),(NULL,-2))t(s,n)').fetchone())
        self.assertNotIn('job_source_sort_key', conn.execute(
            'EXPLAIN SELECT job_source_min(n) FROM (VALUES (10),(-2))t(n)').fetchone()[1])
        conn.close()


if __name__ == '__main__':
    unittest.main()
