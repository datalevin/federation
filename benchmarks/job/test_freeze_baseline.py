import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import freeze_baseline as frozen


class FreezeTests(unittest.TestCase):
    def test_preservation_survives_source_edits_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / 'doc/measurements/run.edn'
            evidence.parent.mkdir(parents=True)
            evidence.write_text('retained answer and timing')
            out = root / 'baseline'
            frozen.freeze(out, {'federation/doc/measurements/run.edn': evidence}, {'baseline_id': 'b0'})
            frozen.verify(out, root)
            with self.assertRaises(FileExistsError):
                frozen.freeze(out, {}, {})
            evidence.write_text('accidentally changed')
            frozen.verify(out)
            with self.assertRaisesRegex(ValueError, 'Working evidence changed'):
                frozen.verify(out, root)

    def test_checksums_detect_archive_and_manifest_corruption(self):
        for name in ['baseline.tar.gz', 'manifest.json']:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / 'source'; source.write_text('snapshot')
                out = root / 'baseline'
                frozen.freeze(out, {'source': source}, {})
                with (out / name).open('ab') as stream:
                    stream.write(b'changed')
                with self.assertRaisesRegex(ValueError, 'Checksum mismatch'):
                    frozen.verify(out)

    def test_member_hashes_are_verified_independently_of_archive_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'; source.write_text('snapshot')
            out = root / 'baseline'
            frozen.freeze(out, {'source': source}, {})
            manifest = json.loads((out / 'manifest.json').read_text())
            manifest['files'][0]['sha256'] = '0' * 64
            raw = json.dumps(manifest).encode()
            (out / 'manifest.json').write_bytes(raw)
            (out / 'SHA256SUMS').write_text(hashlib.sha256(raw).hexdigest() + '  manifest.json\n'
                                          + manifest['archive']['sha256'] + '  baseline.tar.gz\n')
            with self.assertRaisesRegex(ValueError, 'Member checksum mismatch'):
                frozen.verify(out)


if __name__ == '__main__':
    unittest.main()
