"""Freeze existing JOB evidence and source files; never connects to a database."""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parents[2]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_new(path, data):
    with path.open('xb') as stream:
        stream.write(data)


def freeze(output, files, metadata):
    """files maps safe archive names to explicit source paths."""
    output.mkdir(parents=True, exist_ok=False)
    manifest = dict(metadata, format_version=1,
                    frozen_at=datetime.now(timezone.utc).isoformat(), files=[])
    archive = output / 'baseline.tar.gz'
    with tarfile.open(archive, 'x:gz') as bundle:
        for name, source in sorted(files.items()):
            relative = PurePosixPath(name)
            if relative.is_absolute() or '..' in relative.parts or not relative.parts:
                raise ValueError('Unsafe archive name: ' + name)
            data = source.read_bytes()
            item = tarfile.TarInfo(name)
            item.size, item.mtime = len(data), 0
            item.mode = 0o755 if source.stat().st_mode & 0o111 else 0o644
            bundle.addfile(item, io.BytesIO(data))
            manifest['files'].append({'path': name, 'bytes': len(data), 'sha256': digest(data)})
    manifest['archive'] = {'path': archive.name, 'bytes': archive.stat().st_size,
                           'sha256': digest(archive.read_bytes())}
    raw = (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode()
    write_new(output / 'manifest.json', raw)
    write_new(output / 'SHA256SUMS',
              (digest(raw) + '  manifest.json\n' + manifest['archive']['sha256']
               + '  baseline.tar.gz\n').encode())
    return manifest


def verify(directory, evidence_root=None):
    # SHA256SUMS anchors the manifest and compressed artifact. A separately
    # retained copy of these two digests is the external integrity anchor.
    checked = set()
    for line in (directory / 'SHA256SUMS').read_text().splitlines():
        expected, name = line.split('  ', 1)
        if name not in ('manifest.json', 'baseline.tar.gz'):
            raise ValueError('Unexpected checksum entry')
        if digest((directory / name).read_bytes()) != expected:
            raise ValueError('Checksum mismatch: ' + name)
        checked.add(name)
    if checked != {'manifest.json', 'baseline.tar.gz'}:
        raise ValueError('Missing checksum entries')
    manifest = json.loads((directory / 'manifest.json').read_text())
    if manifest['archive']['path'] != 'baseline.tar.gz':
        raise ValueError('Unexpected archive path')
    entries = {entry['path']: entry for entry in manifest['files']}
    if len(entries) != len(manifest['files']):
        raise ValueError('Duplicate manifest entry')
    seen = set()
    with tarfile.open(directory / manifest['archive']['path'], 'r:gz') as bundle:
        for member in bundle:
            if not member.isfile() or member.name not in entries or member.name in seen:
                raise ValueError('Unexpected archive member: ' + member.name)
            entry = entries[member.name]
            data = bundle.extractfile(member).read()
            if len(data) != entry['bytes'] or digest(data) != entry['sha256']:
                raise ValueError('Member checksum mismatch: ' + member.name)
            seen.add(member.name)
            if evidence_root and member.name.startswith('federation/doc/measurements/'):
                live = evidence_root / member.name.removeprefix('federation/')
                if not live.is_file() or digest(live.read_bytes()) != entry['sha256']:
                    raise ValueError('Working evidence changed: ' + str(live))
    if seen != set(entries):
        raise ValueError('Missing archive members')
    return manifest


def select_files(spec, roots):
    result = {}
    for repo, scopes in spec['capture'].items():
        root = roots[repo]
        for scope in scopes:
            if Path(scope).is_absolute() or '..' in Path(scope).parts:
                raise ValueError('Capture scope must stay within its repository')
            path = root / scope
            if path.is_file():
                paths = [path]
            elif path.is_dir():
                # Honor ignore rules, excluding database/build caches even
                # when those exist underneath source directories.
                paths = [root / p for p in subprocess.check_output(
                    ['rg', '--files', '--', scope], cwd=root, text=True).splitlines()]
            else:
                raise ValueError('Missing capture scope: ' + str(path))
            for file in paths:
                result[repo + '/' + file.relative_to(root).as_posix()] = file
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    create = sub.add_parser('freeze')
    create.add_argument('--spec', type=Path, required=True)
    create.add_argument('--output', type=Path, required=True)
    check = sub.add_parser('verify')
    check.add_argument('directory', type=Path)
    check.add_argument('--working-evidence', action='store_true')
    args = parser.parse_args()
    if args.command == 'verify':
        manifest = verify(args.directory, ROOT if args.working_evidence else None)
    else:
        spec = json.loads(args.spec.read_text())
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]+', spec['baseline_id']):
            parser.error('Use a stable lowercase baseline identifier')
        roots = {'federation': ROOT, 'datalevin': ROOT.parent / 'datalevin'}
        metadata = dict(spec)
        metadata['revisions_at_freeze'] = {}
        for repo, root in roots.items():
            def git(*arguments):
                return subprocess.check_output(['git', '-C', str(root), *arguments], text=True).strip()
            metadata['revisions_at_freeze'][repo] = {
                'commit': git('rev-parse', 'HEAD'), 'status': git('status', '--porcelain').splitlines()}
        files = select_files(spec, roots)
        references = [spec['primary_progress_anchor'], spec['replicate_context']]
        for study in spec['studies']:
            references.extend([study['report'], *study['raw_runs']])
        if any(name not in files for name in references):
            raise ValueError('Required study evidence is missing from capture scopes')
        manifest = freeze(args.output, files, metadata)
        verify(args.output)
    print(json.dumps({'baseline_id': manifest['baseline_id'], 'files': len(manifest['files']),
                      'archive_sha256': manifest['archive']['sha256']}))


if __name__ == '__main__':
    main()
