# Immutable EDN Lines exports

`org.clojars.huahaiy/datalevin-federation-file` reads an explicitly accepted
export through Datalevin's v2 query-source SPI. It supports scalar and reference
attributes, `:one` and `:many`, field/path extraction, embedded native joins,
and export-scoped identity. Accesses are scans; the existing query engine
performs joins, rules, and residual checks within its supported query subset.

Each nonempty physical UTF-8 line contains exactly one EDN map. A final newline
is optional; CRLF is accepted. Blank lines, additional forms, invalid UTF-8,
and unknown reader tags reject. Standard EDN readers are used, with no
application reader extensions or evaluation. Record byte and nesting limits
are explicit parts of the accepted format. Values retain their EDN types;
conversion version 1 performs no numeric or string coercion.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.file :as export])

;; /tmp/people.ednl:
;; {:name "Ada" :tags ["red" "red" nil "blue"]}
;; {:name "Grace" :tags ["red"]}

;; Allocate this UUID once for the external identity domain and persist it.
(def source-id #uuid "12345678-1111-4111-8111-123456789abc")

;; Explicit acceptance, including a new UUID for this captured export.
(def manifest (export/manifest "/tmp/people.ednl" (random-uuid)))
(def accepted
  (export/accept-mapping
    {:relation-id :exports/people :source-instance-id source-id
     :domain :person :identity-epoch 0 :identity-attribute :export/row
     :scope {:id "people:all" :kind :full :parameters {}}
     :attributes
     {:person/name {:extraction {:shape :value :expression [:field :name]}
                    :type :string :cardinality :one :null :absent
                    :conversion {:version 1}}
      :person/tag {:extraction {:shape :elements :expression [:field :tags]}
                   :type :string :cardinality :many :null :absent
                   :conversion {:version 1}}}}
    manifest))

(def people
  (export/source accepted
    {:path "/tmp/people.ednl" :compute-context-id :my-app/people-export}))

(d/q '[:find ?name ?tag :in $people :where
       [$people ?e :person/name ?name]
       [$people ?e :person/tag ?tag]]
     people)
;; => #{["Ada" "red"] ["Ada" "blue"] ["Grace" "red"]}
```

Source construction and `-source-info` perform no I/O. `manifest` is an explicit
inspection operation: it computes SHA-256, byte/record counts, and validates
framing. Defaults are 16 KiB per record, depth 32, and a 1 GiB inspection cap.
Options `:max-record-bytes` (at most 1 MiB), `:max-depth` (at most 128),
`:max-bytes`, and `:check-active!` bound/cooperate with caller-owned inspection.

`[:field key]` reads a string or keyword key. `[:path [key ...]]` traverses
nested maps, treating a missing/null parent as absence and rejecting a non-map
parent. Extraction does not flatten nested objects. `:elements` maps a declared
collection to unordered EAV values; duplicates and null elements add no facts.
Reference attributes use F0's complete nominal target/type contract. The scope
describes the accepted file's view; the adapter applies no implicit tenant filter.

Every entity key is `[export-token physical-record-ordinal]`, with ordinals
starting at 1. The token combines the explicit export UUID and content digest.
Identical records at different ordinals remain distinct. Reopening or moving the
same accepted export preserves identity; filtering and batching do not renumber
records. A separately captured export uses a new UUID even when its bytes are
identical. Domain/type descriptors stay stable across captures; row identities
do not imply durable entity continuity. This adapter does not infer natural keys.

Persist `accepted` using
[`install/install!`](../README.md#persisted-definitions): the source provider is
`:file/export`, and the relation stores the accepted metadata. On reopen, take
`:compiled relation-id :accepted` from `load-definitions` and pass it to
`export/source` with the runtime profile. The manifest is recorded under
`:dependencies :export`; the parser requirements and complete logical contracts
participate in fingerprints. A new export is an explicit accepted refresh.
Paths, streams, and runtime profiles are supplied separately from schema metadata.

Each query acquires one private copy before publishing its snapshot. Copying
checks the exact byte count and digest, then validates all records, projected
types, and the declared record count. A corrupt tail or known incomplete export
fails even if `LIMIT` or a selective query would have skipped it. Later changes
to the original file do not affect the pinned view. The next acquisition rejects
drift against the recorded manifest. Aliases share acquisition; independent
pattern scans replay the same private copy.

The full copy is charged to `:spill-bytes` before any backend read or file write.
Metadata and bounded parser/projection workspace are charged to
`:retained-bytes`. Projection validates one attribute of one record at a time;
rows are never accumulated for the entire export. Cursor output reserves a
bounded batch window within the requested caps before copying rows, and reports actual
conservative value charges as `:byte-count`. Complete nominal type descriptors
also contribute to that charge. A large batch cap does not force an equally
large reservation; one wide row must still fit the caller's caps. Acquiring the private copy adds
startup I/O and validation; it is not a selective indexed file lookup.

Profiles require `:compute-context-id` and either `:path` or `:open-input`.
Context IDs must distinguish incompatible profiles/views; aliases can reuse
one source object. An opener receives the SPI acquisition request, must bound
its setup with the supplied control, and returns an `InputStream` promptly.
The adapter owns and closes that returned stream. A close must unblock reads.
If setup fails before returning, the opener cleans up its own partial resources.
`:reopen :once` permits one acquisition attempt; the query can still replay the
private copy across multiple scans. A second query needs another source/input.
`:spool-directory` optionally selects an existing local scratch directory.

Backend acquisition uses the shared request/concurrency ledger. Cancellation
closes active input/output handles. Success, failure, early stop, and repeated
close release streams, reservations, and temporary copies. Plan handles own no
file reader; cursors own their private-copy readers. The acquired guarantee is
`:snapshot`, including when the caller permits weaker consistency.

SPI cursor metrics count records and actual buffered bytes read from the private
copy; their external-request count is zero. `export/statistics` reports cumulative
acquisition attempts, original bytes read, validated records, elapsed nanoseconds,
failed captures, and live copies separately. These counters contain no paths or
row values. Advisory estimates remain unknown. Neither acquisition work nor
private-copy read work is presented as a calibrated cost model.

Run the adapter's real-file, fault, shared scan, persistence, and native-join checks:

```sh
clojure -M:test:file
```

Validation (2026-10-21): **16 tests / 154 assertions** pass both in the workspace
and with standalone `lein test`. The complete companion suite passes **105 /
23,120**; lint is clean and `lein jar` builds the independent file artifact.

The file artifact depends on companion `core` and the supplied Datalevin runtime;
it adds no provider driver. For a Leiningen checkout build:

```sh
mkdir -p file/checkouts
ln -sfn ../../../datalevin file/checkouts/datalevin
ln -sfn ../../core file/checkouts/federation-core
cd file
lein jar
```
