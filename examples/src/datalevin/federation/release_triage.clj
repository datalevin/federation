(ns datalevin.federation.release-triage
  "Executable release triage example: native priorities, immutable release
  assignments and GitHub work items compose through ordinary source EAV facts."
  (:require [datalevin.core :as d]
            [datalevin.federation.file :as export]
            [datalevin.federation.github :as github]
            [datalevin.federation.install :as install]
            [datalevin.federation.mapping :as mapping]))

(def native-schema
  {:triage/repository {:db/valueType :db.type/long}
   :triage/number {:db/valueType :db.type/long}
   :triage/priority {:db/valueType :db.type/long}})

(defn- attribute [type field cardinality]
  {:type type :extraction {:shape (if (= :many cardinality) :elements :value) :expression [:field field]}
   :cardinality cardinality :null :absent :conversion {:version 1}})

(defn accept
  "Accept an explicit GitHub declaration and inspected release export. Export
  records carry :issue [repository-id number], :owners [user-id ...], :window,
  and :tags. Shared reference types come from the accepted GitHub mapping.
  export-source-id is a persisted export-service identity, not a capture UUID."
  [github-declaration export-source-id manifest]
  (when-not (= :github/work-items (:relation-id github-declaration))
    (throw (ex-info "The workflow expects relation :github/work-items" {:error :federation/workflow})))
  (let [api (github/accept-mapping
              (assoc github-declaration :attributes #{:github.issue/number :github.issue/title
                                                       :github.issue/label :github.issue/assignee}))
        issue-type (get-in api [:identity :type])
        owner-type (get-in api [:attributes :github.issue/assignee :reference :type])
        reference (fn [type field cardinality]
                    (-> (attribute nil field cardinality) (dissoc :type)
                        (assoc :reference {:domain (get-in type [:parameters :identity-domain-id]) :type type})))
        release (export/accept-mapping
                  {:relation-id :release/assignments :source-instance-id export-source-id
                   :domain :release/assignment :identity-epoch 0 :identity-attribute :release/row
                   :scope {:id "release:accepted-export" :kind :full :parameters {}}
                   :attributes {:release/issue (reference issue-type :issue :one)
                                :release/owner (reference owner-type :owners :many)
                                :release/window (attribute :string :window :one)
                                :release/tag (attribute :string :tags :many)}} manifest)]
    {:github api :release release}))

(defn definitions
  "Portable source/relation envelopes. Runtime paths and credentials stay out."
  [{:keys [github release]}]
  {:github {:datalevin/definition-kind :source
            :datalevin.federation/definition
            {:format-version 1 :source-instance-id (:source-instance-id github)
             :provider :github/issues :profile :profiles/github}}
   (:relation-id github) {:datalevin/definition-kind :relation
                          :datalevin.federation/definition
                          {:format-version 1 :source :github :accepted github}}
   :release {:datalevin/definition-kind :source
             :datalevin.federation/definition
             {:format-version 1 :source-instance-id (:source-instance-id release)
              :provider :file/export :profile :profiles/release}}
   (:relation-id release) {:datalevin/definition-kind :relation
                          :datalevin.federation/definition
                          {:format-version 1 :source :release :depends-on #{(:relation-id github)} :accepted release}}})

(defn install!
  "Atomically install both mappings with explicit expected versions. Native
  schema edits may be supplied through the normal installer options."
  [conn accepted options]
  (install/install! conn (definitions accepted) options))

(defn sources
  "Resolve a saved, verified catalog with explicit runtime profiles. No I/O.
  Returns the real production adapters, shared by repeated source aliases."
  [loaded github-profile release-profile]
  {:github (github/source (get-in loaded [:compiled :github/work-items :accepted]) github-profile)
   :release (export/source (get-in loaded [:compiled :release/assignments :accepted]) release-profile)})

(def query
  "Return one row per matching priority, title, label, owner and release window.
  Native repository filtering and the API's accepted repository must agree."
  '{:find [?number ?priority ?title ?label ?owner ?window]
    :in [$ $github $release ?repository ?minimum [[?window ?witness]]]
    :where [[?t :triage/repository ?repository] [?t :triage/number ?number]
            [?t :triage/priority ?priority] [(>= ?priority ?minimum)]
            [$github ?issue :github.issue/number ?number]
            [$github ?issue :github.issue/title ?title]
            [$github ?issue :github.issue/label ?label]
            [$github ?issue :github.issue/assignee ?owner]
            [$release ?plan :release/issue ?issue]
            [$release ?plan :release/owner ?owner]
            [$release ?plan :release/window ?window]]
    :timeout 30000 :query-source {:consistency :best-effort}})

(def summary-query
  "Retain distinct export rows and ordinary input witnesses at aggregation."
  (assoc query :find '[(count ?issue) .] :with '[?plan ?label ?owner ?witness]))

(def lookup-query
  "Smaller measurement query: selective scalar native keys, two API accesses,
  two export scans, and a caller-supplied release-window relation."
  (assoc query :find '[?number ?priority ?title ?window]
         :where '[[?t :triage/repository ?repository] [?t :triage/number ?number]
                  [?t :triage/priority ?priority] [(>= ?priority ?minimum)]
                  [$github ?issue :github.issue/number ?number]
                  [$github ?issue :github.issue/title ?title]
                  [$release ?plan :release/issue ?issue]
                  [$release ?plan :release/window ?window]]))

(def rules
  "Positive mapped rules compose with native clauses through the existing engine."
  '[[(release-owner ?issue ?owner ?window)
     [?plan :release/issue ?issue]
     [?plan :release/owner ?owner]
     [?plan :release/window ?window]]
    [(named-work-item [?issue] ?title ?label ?owner)
     [?issue :github.issue/title ?title]
     [?issue :github.issue/label ?label]
     [?issue :github.issue/assignee ?owner]]])

(def rule-query
  (-> query
      (assoc :in '[$ $github $release % ?repository ?minimum [[?window ?witness]]]
             :where '[[?t :triage/repository ?repository] [?t :triage/number ?number]
                      [?t :triage/priority ?priority] [(>= ?priority ?minimum)]
                      [$github ?issue :github.issue/number ?number]
                      ($release release-owner ?issue ?owner ?window)
                      ($github named-work-item ?issue ?title ?label ?owner)])))

(defn eager-release
  "Independent caller extraction for an eager oracle; projection uses the same
  accepted nominal identities and exact values as every provider must publish."
  [accepted rows]
  (let [project (mapping/fact-projector (mapping/compile-logical-mapping accepted))
        token (export/export-token (get-in accepted [:dependencies :export]))]
    (into [] (mapcat (fn [ordinal row]
                       (project [token ordinal] {:release/issue (:issue row) :release/owner (:owners row)
                                                 :release/window (:window row) :release/tag (:tags row)}))
                    (iterate inc 1) rows))))

(defn eager-github
  "Project a controlled complete API observation into the same nominal domains.
  Real mutable GitHub results are never claimed to share an eager snapshot."
  [accepted observations]
  (let [project (mapping/fact-projector (mapping/compile-logical-mapping accepted))
        repository-id (get-in accepted [:dependencies :repository :id])]
    (into [] (mapcat (fn [item]
                       (project [repository-id (get item "number")]
                                {:github.issue/number (get item "number") :github.issue/title (get item "title")
                                 :github.issue/label (mapv #(get % "name") (get item "labels"))
                                 :github.issue/assignee (mapv #(get % "id") (get item "assignees"))}))) observations)))

(defn run
  "Execute ordinary d/q with explicit runtime sources and caller inputs."
  [conn {:keys [github release]} repository minimum windows]
  (d/q query @conn github release repository minimum windows))
