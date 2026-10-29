# Experimental EntityRef fixtures

`entity-ref-inputs.edn` defines nine nominal identity examples;
`entity-ref-golden.tsv` pins their encoded bytes. The JVM identity suite checks
construction, decoding, keys, complete types, and re-encoding. The independent
dependency-free Rust verifier constructs the same semantic examples, checks
Rust encoding against the golden bytes, and decodes the golden bytes back to
those examples. It is a fixture oracle for this subset, not a production Rust
codec. Passing these fixtures does not declare the wire format stable.

From the federation repository:

```sh
clojure -M:test:core
rustc --edition 2021 core/test/fixtures/verify_entity_ref.rs -o /tmp/datalevin-verify-entity-ref
/tmp/datalevin-verify-entity-ref core/test/fixtures/entity-ref-golden.tsv
```

The wire header is ASCII `ERF` followed by revision byte `01`. Two unsigned
32-bit big-endian lengths frame the complete type and key tuple, respectively.
Every node starts with a one-byte tag. Integer fields and lengths are
big-endian. Strings use strict UTF-8 with a 32-bit byte length. UUIDs contain
the two native 64-bit halves. Collection counts precede their nodes.

| Tag | Value |
| --- | --- |
| 0, 1, 2 | nil, false, true |
| 3 | signed 64-bit Long |
| 4, 5 | BigInt, BigInteger: framed decimal text |
| 6, 7 | Double, Float: canonical IEEE bits |
| 8 | BigDecimal: signed 32-bit scale and framed unscaled integer text |
| 9 | framed string |
| 10, 11 | keyword, symbol: framed namespace (empty if absent), then name |
| 12, 13, 14 | UUID, Date (signed epoch milliseconds), framed bytes |
| 15 | sequential value, encoded as a vector |
| 17, 18 | map entries, set elements |
| 19, 20 | UTF-16 character, ratio (framed numerator and denominator text) |
| 21 | framed nested EntityRef |
| 22, 23, 24 | signed Integer, Short, Byte |
| 25, 26 | Instant, Duration: signed seconds and signed 32-bit nanos |
| 27 | Period: signed 32-bit years, months, days |
| 28 | LocalDate: signed 64-bit epoch day |
| 29 | LocalTime: signed 64-bit nanosecond of day |
| 30 | LocalDateTime: signed 64-bit epoch day and nanosecond of day |
| 31 | OffsetTime: signed 64-bit nanosecond of day and signed 32-bit offset seconds |

Map entries and set elements sort by encoded length, then unsigned bytes;
equal encoded map keys tie-break by encoded value. Descriptor normalization
preserves Clojure map equality, including numeric and sequential equivalence,
and removes metadata. Key normalization follows existing QueryKey equality,
including bytes by content and decimal scale. No physical column, relation
alias, connection handle, or data-version token is added to nominal identity.
An export token is an ordinary, explicitly declared key component.

Hash-sensitive Float values in descriptor map keys/set members or equivalent
hashed positions inside data keys require explicit provider normalization and
are rejected. Clojure hash-trie routing can otherwise make their equality
depend on surrounding entries. Ordinary typed Float key components remain
supported. Arbitrary custom readers are unsupported; nested custom keys must
be EntityRef values using this bounded contract.

The default limits are 1 MiB per reference, 65,536 nodes, and depth 64. Callers
may lower these limits. Encoding retains and checks the measured node/depth
usage; decoding rejects excessive lengths/counts before allocating, malformed
UTF-8, duplicate map/set members, noncanonical representations, and trailing
bytes. Mutable key leaves are never retained. Returned key tuples and wire
bytes are detached copies. Diagnostics contain no reference values or causes.
