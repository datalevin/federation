# datalevin-federation

Planned source adapters and semantic mappings for Datalevin direct analytics.
This project owns every production query-source adapter, including local
relations, PostgreSQL, and optional dataset/Parquet/DataFusion integrations.

- [Companion design and delivery plan](federation.md)
- [Release roadmap and milestone checklist](../datalevin/roadmap.md)
- [Datalevin analytics and virtual relations](../datalevin/analytics.md)
- [Shared schema definitions contract](../datalevin/doc/schema-definitions.md)
- [Public query-source SPI](../datalevin/doc/query-source-spi.md)
- [Optional extensions appendix](../datalevin/doc/query-source-extensions.md)

F0 includes an accepted-metadata [mapping compiler](src/datalevin/federation/mapping.clj),
[canonical fingerprint encoding](src/datalevin/federation/fingerprint.clj), and
[contract fixtures](test/datalevin/federation/contract_test.clj). No provider I/O,
production adapter, schema installation, or EntityRef runtime codec is implemented.
Build, dependencies, tests, and releases remain separate from Datalevin proper.

With the matching Datalevin checkout at `../datalevin`, run:

```sh
clojure -M:test
```

`compile-relation` accepts the main-schema definition map, a relation ID, and a
context containing a compute-context ID and recorded semantic dependencies.
It returns the accepted relation body with digests and its validated SPI source
descriptor. The fixtures include the plan's normalized mapping, composite keys,
and resolved reference targets. Concise mapping normalization and physical-type
conversion remain adapter work; the compiler consumes accepted logical types.

Canonical v1 uses `tag + decimal UTF-8 payload byte length + ':' + payload`.
Tags are `n` nil, `b` boolean (`0`/`1`), `l` long, `i` bigint, `d` decimal
(`scale:unscaled`), `f` double and `r` float (Java hexadecimal string), `t` string,
`k` keyword, `y` symbol, `u` UUID (lowercase text), `z` instant (epoch milliseconds),
`a` bytes, `v` vector, `q` list, `s` set, and `m` map. Keyword/symbol payloads
concatenate encoded namespace (nil when absent) and name. Containers concatenate
encoded children; maps concatenate key/value pairs sorted by unsigned encoded key,
and sets sort unsigned encoded values. Scalar distinctions and sequence order
are retained. The byte/digest fixtures were independently checked with Python's
standard SHA-256 implementation.
