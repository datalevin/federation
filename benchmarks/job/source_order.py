"""Local libc sort keys for the same-host PostgreSQL JOB compatibility profile."""
import ctypes
import ctypes.util
import locale
import platform
import re


MIN_MACRO = """CREATE MACRO job_source_min(x) AS
  CASE WHEN typeof(min(x))='VARCHAR' THEN
    cast_to_type(arg_min(CAST(x AS VARCHAR), struct_pack(
      collation_key := job_source_sort_key(CAST(x AS VARCHAR)),
      byte_tie := encode(CAST(x AS VARCHAR)))), min(x))
  ELSE min(x) END"""


class LibcOrder:
    def __init__(self, name):
        self.name = name
        self.library = ctypes.util.find_library('c')
        self.libc = ctypes.CDLL(self.library, use_errno=True)
        self.libc.newlocale.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_void_p]
        self.libc.newlocale.restype = ctypes.c_void_p
        self.libc.strxfrm_l.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_void_p]
        self.libc.strxfrm_l.restype = ctypes.c_size_t
        self.libc.strcoll_l.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p]
        self.libc.strcoll_l.restype = ctypes.c_int
        # Darwin's masks are indexed from category-1; glibc uses category.
        category = locale.LC_COLLATE - (1 if platform.system() == 'Darwin' else 0)
        self.handle = self.libc.newlocale(1 << category, name.encode(), None)
        if not self.handle:
            raise ValueError('Source libc locale is unavailable locally: ' + name)

    def key(self, value):
        raw = value.encode('utf-8')
        if b'\0' in raw:
            raise ValueError('PostgreSQL text cannot contain NUL')
        buffer = ctypes.create_string_buffer(4 * len(raw) + 64)
        length = self.libc.strxfrm_l(buffer, raw, len(buffer), self.handle)
        if length >= len(buffer):
            buffer = ctypes.create_string_buffer(length + 1)
            actual = self.libc.strxfrm_l(buffer, raw, len(buffer), self.handle)
            if actual != length:
                raise RuntimeError('libc sort-key size changed')
        return buffer.raw[:length]

    def compare(self, left, right):
        return self.libc.strcoll_l(left.encode('utf-8'), right.encode('utf-8'), self.handle)


def rewrite_minima(statement):
    # Only function calls outside SQL strings/comments/quoted identifiers.
    token = re.compile(r"(?is)'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\r\n]*|/\*.*?\*/|\bMIN(?=\s*\()")
    return token.sub(lambda m: 'job_source_min' if m[0].upper() == 'MIN' else m[0], statement)


def install(connection, source, identity):
    """Guard this profile to a local UTF-8 libc source; do not guess ICU mappings."""
    provider, name, encoding, version = source
    family = platform.system()
    if (provider != 'c' or encoding != 'UTF8'
            or identity['host'] not in ('127.0.0.1', 'localhost', '::1')
            or family not in ('Darwin', 'Linux') or family.lower() not in version.lower()):
        raise ValueError('source-libc requires a same-host UTF-8 libc PostgreSQL source on the same OS family')
    order = LibcOrder(name)
    # Check strxfrm keys against the actual comparator, including punctuation,
    # currency, accents, normalization and long strings. No global setlocale.
    probes = ['', '(as A)', '(Set)', '$1,000', '€ 1,000', 'A', 'a', 'é', 'é', 'Öga', 'Spår', 'x' * 8192]
    for a, b in zip(probes, probes[1:]):
        by_key = (order.key(a) > order.key(b)) - (order.key(a) < order.key(b))
        actual = order.compare(a, b)
        if by_key != ((actual > 0) - (actual < 0)):
            raise RuntimeError('libc sort keys disagree with the local comparator')
    values = ','.join("('" + value.replace("'", "''") + "')" for value in probes)
    query = 'SELECT value FROM (VALUES ' + values + ') AS t(value) ORDER BY value'
    source_values = [row[0] for row in connection.execute(
        "SELECT * FROM postgres_query('job', '" + query.replace("'", "''") + "')").fetchall()]
    if source_values != sorted(probes, key=lambda value: (order.key(value), value.encode('utf-8'))):
        raise ValueError('Source and local libc ordering disagree on compatibility probes')
    connection.create_function('job_source_sort_key', order.key, ['VARCHAR'], 'BLOB')
    # Numeric branches remain native MIN; deterministic text ties use UTF-8
    # bytes, as PostgreSQL does after equal libc collation comparisons.
    connection.execute(MIN_MACRO)
    return {'locale': name, 'provider': 'libc', 'library': order.library,
            'local_platform': platform.platform(), 'scope': 'local text MIN only',
            'source_probe_count': len(probes),
            'tie_break': 'UTF-8 bytes', 'execution': 'Python UDF calling strxfrm_l; cost included'}
