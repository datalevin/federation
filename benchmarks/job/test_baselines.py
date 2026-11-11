import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import parse_qs, urlsplit

import baselines as baseline


class ConfigurationTests(unittest.TestCase):
    def test_pinned_corpus_and_query_selection(self):
        corpus = baseline.ROOT.parent / "datalevin/benchmarks/JOB-bench/queries"
        baseline.verify_corpus(corpus, ["6a", "11a"])
        with self.assertRaisesRegex(ValueError, "Unknown JOB query"):
            baseline.verify_corpus(corpus, ["99z"])
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "pinned 113"):
                baseline.verify_corpus(Path(directory), None)

    def test_provenance_manifest_is_embedded(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "import.txt"
            manifest.write_text("Dataset checksums and import procedure\n")
            recorded = baseline.dataset_provenance(str(manifest))
            self.assertEqual(recorded["manifest"], manifest.read_text())
            self.assertEqual(recorded["manifest_sha256"], baseline.sha256(manifest))

    def test_source_credentials_and_read_only_settings(self):
        source = baseline.source_config(
            "jdbc:postgresql://localhost:5433/imdb?user=reader&password=a%26b&sslmode=require",
            None, None, "public", 1234)
        self.assertEqual(source["password"], "a&b")
        params = parse_qs(urlsplit(source["url"][5:]).query)
        self.assertNotIn("user", params)
        self.assertNotIn("password", params)
        self.assertEqual(params["options"], ["-c default_transaction_read_only=on -c statement_timeout=1234"])
        self.assertEqual(params["currentSchema"], ["public"])
        self.assertEqual(params["sslmode"], ["require"])
        self.assertEqual(source["identity"]["database"], "imdb")
        self.assertNotIn("a&b", json.dumps(source["identity"]))

    def test_ambiguous_or_unsafe_source_urls(self):
        for url in ["jdbc:postgresql://user:password@localhost/imdb",
                    "jdbc:postgresql://localhost/imdb?user=a&user=b",
                    "jdbc:postgresql://localhost/imdb?options=-cdefault_transaction_read_only=off",
                    "jdbc:postgresql://localhost/imdb#fragment"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                baseline.source_config(url, "reader", None, "public", 300000)

    def test_java_properties_cannot_inject_settings(self):
        value = baseline.properties({"connection-password": " #a=é\ncoordinator=false\\"})
        self.assertEqual(value, "connection-password=\\ \\#a\\=\\u00e9\\ncoordinator\\=false\\\\\n")
        self.assertEqual(len(value.splitlines()), 1)

    def test_corrupt_cached_artifact_is_rejected_before_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            downloads = Path(directory)
            (downloads / baseline.VERSIONS["trino"]["archive"]).write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
                baseline.install("trino", downloads / "home", downloads)
            self.assertFalse((downloads / "home/engines").exists())

    def test_existing_unowned_install_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            install = home / "engines" / baseline.VERSIONS["drill"]["directory"]
            install.mkdir(parents=True)
            keep = install / "keep"
            keep.write_text("preserve")
            with self.assertRaisesRegex(ValueError, "Unrecognized installation"):
                baseline.install("drill", home, home / "downloads")
            self.assertEqual(keep.read_text(), "preserve")


class ProcessTests(unittest.TestCase):
    def test_owned_process_stops_and_cleanup_is_repeatable(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            baseline.stop(process)
            self.assertIsNotNone(process.poll())
            baseline.stop(process)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
