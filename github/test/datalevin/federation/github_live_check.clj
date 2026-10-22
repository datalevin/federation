(ns datalevin.federation.github-live-check
  "Opt-in public-network native triage join. Never part of the default tests."
  (:require [datalevin.core :as d]
            [datalevin.federation.github :as github]))

(defn -main
  "Read one real work item, then run the same native join with actual metrics.
  Optional owner, repository, numeric repository ID, work-item number. No writes
  to GitHub; GITHUB_TOKEN is optional and stays in the runtime profile."
  [& [owner repository repository-id number]]
  (let [owner (or owner "octocat") repository (or repository "Hello-World")
        repository-id (Long/parseLong (or repository-id "1296269"))
        number (Long/parseLong (or number "1"))
        accepted (github/accept-mapping
                   {:relation-id :github/work-items
                    :source-instance-id #uuid "b7bca594-d387-4a21-8cf4-60379ecb5098"
                    :repository {:id repository-id :owner owner :name repository}
                    :attributes #{:github.issue/number :github.issue/title :github.issue/state}})
        provider (github/source accepted
                   {:compute-context-id :github/live :token (System/getenv "GITHUB_TOKEN")})
        conn (d/create-conn nil {:triage/number {:db/valueType :db.type/long}
                                :triage/repository {:db/valueType :db.type/long}}
                            {:kv-opts {:inmemory? true} :background-sampling? false})
        query {:find '[?number ?title ?state] :in '[$ $g ?repository]
               :where '[[?t :triage/repository ?repository] [?t :triage/number ?number]
                        [$g ?e :github.issue/number ?number] [$g ?e :github.issue/title ?title]
                        [$g ?e :github.issue/state ?state]]
               :timeout 30000
               :query-source {:consistency :best-effort :limits {:external-requests 16}}}]
    (try
      (d/transact! conn [{:triage/number number :triage/repository repository-id}])
      (let [started (System/nanoTime)
            rows (d/q query @conn provider repository-id)
            elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)
            explain (d/explain {:run? true} query @conn provider repository-id)]
        (when-not (and (= 1 (count rows)) (= number (ffirst rows))
                       (string? (second (first rows))) (zero? (:live-sessions (github/statistics provider))))
          (throw (ex-info "Live GitHub join failed" {})))
        (prn {:rows rows :first-query-elapsed-ms elapsed-ms
              :acquisition (github/statistics provider)
              :sources (mapv #(select-keys % [:consistency :data-version]) (:sources explain))
              :cursors (mapv :metrics (:cursors explain))}))
      (finally (d/close conn) (shutdown-agents)))))
