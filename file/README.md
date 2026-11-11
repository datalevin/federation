# EDN Lines file exports

`org.clojars.huahaiy/datalevin-federation-file` exposes an EDN Lines export as a
snapshot you can query and join with native Datalevin data or other sources.
It supports scalar, collection, and reference attributes. Access uses file
scans; each query copies and validates the entire export before reading facts.

Follow the repository's [setup instructions](../README.md#getting-started) to
try the example.

## Accept and query an export

Create a file, inspect its contents with `manifest`, then explicitly map its
fields to attributes. Keep the source UUID stable for your application; use a
new export UUID for each capture.

```clojure
(require '[datalevin.core :as d]
         '[datalevin.federation.file :as export])

;; Create the example export.
(spit "/tmp/people.ednl"
      (str (pr-str {:name "Ada" :tags ["red" "red" nil "blue"]}) "\n"
           (pr-str {:name "Grace" :tags ["red"]}) "\n"))

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

`manifest` computes a digest, counts records and bytes, and validates the file
format. Creating a source from an accepted mapping performs no I/O. The profile
supplies the file's location when a query runs.

## File format and mappings

Each UTF-8 line must contain exactly one EDN map. A final newline is optional
and CRLF is accepted. Blank lines, extra forms, invalid UTF-8, and unknown reader
tags are rejected. Standard EDN readers are used without application extensions
or evaluation. Values retain their EDN types; conversion version `1` performs
no numeric or string coercion.

`[:field key]` reads a string or keyword key. `[:path [key ...]]` traverses nested
maps: a missing or null parent contributes no fact, while a non-map parent is an
error. `:shape :elements` maps a collection to many values; duplicates and null
elements contribute no additional facts. Reference attributes declare the target
entity's domain and key types; the [release-triage example](../examples/README.md)
shows references shared with GitHub.

The mapping's `:scope` describes the accepted export. It does not filter rows or
apply a tenant boundary; create the intended export before accepting it.

| Manifest option | Default | Purpose |
| --- | --- | --- |
| `:max-record-bytes` | `16384` | Maximum bytes per record; configurable up to 1 MiB. |
| `:max-depth` | `32` | Maximum nesting depth; configurable up to 128. |
| `:max-bytes` | `1073741824` | Maximum bytes inspected. |
| `:check-active!` | unset | Callback for caller-controlled cancellation during inspection. |

Pass options as the third argument to `manifest`.

## Identity and refreshes

An entity key is `[export-token physical-record-ordinal]`, with ordinals starting
at `1`. The token combines the export UUID and content digest. Identical records
on separate lines are distinct entities. Moving or reopening the same accepted
export preserves identity; filtering does not renumber records.

A new capture uses a new UUID even if its bytes are identical. Row identities do
not carry across captures, and the adapter does not infer natural keys. Include
business identifiers as attributes when you need to join successive exports.

Save `accepted` using the [definition installer](../README.md#persisted-definitions)
with source provider `:file/export`. After `install/load-definitions`, pass the
saved relation's `:accepted` mapping to `export/source` with its runtime profile.
Refresh by accepting a new manifest and installing the replacement mapping with
the current definition version. Keep paths and streams outside saved definitions.

## Runtime configuration

Profiles require `:compute-context-id` and exactly one of `:path` or `:open-input`.
Use different context IDs for incompatible profiles; aliases can reuse the same
source object.

| Option | Purpose |
| --- | --- |
| `:path` | Path to the accepted export. |
| `:open-input` | Function receiving the acquisition request and returning an `InputStream`. |
| `:reopen` | `:many` by default; `:once` permits only one acquisition attempt. |
| `:spool-directory` | Existing directory for temporary snapshot copies; defaults to the system temporary directory. |

A custom opener must use the request's control to bound setup and return promptly.
The adapter owns and closes the returned stream; closing it must unblock reads.
The opener must clean up partial resources if it fails before returning. With
`:reopen :once`, a second query needs a new source/input, though scans within the
first query can replay its private copy.

## Snapshots and resource limits

Each query verifies the complete byte count, digest, record count, and mapped
values before publishing its private snapshot. A corrupt tail fails even if a
result limit would have skipped it. Later changes to the original file do not
affect the active snapshot; a subsequent query rejects changed contents until
you accept a new manifest. Aliases of the same source share one capture.

The full temporary copy counts against `:query-source :limits :spill-bytes`.
Parser workspace, metadata, and output batches count against `:retained-bytes`.
Selective filters and small result limits do not reduce acquisition work.
Timeouts, cancellation, and query completion close streams and remove private
copies.

`d/explain` reports scan work. `export/statistics` separately reports cumulative
acquisition attempts, bytes read, validated records, failures, and live copies,
without exposing paths or row values.
