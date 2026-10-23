(ns datalevin.federation.release-triage-live
  "Opt-in real GitHub + immutable export + persisted native triage workflow."
  (:require [clojure.java.io :as io]
            [datalevin.core :as d]
            [datalevin.federation.file :as export]
            [datalevin.federation.github :as github]
            [datalevin.federation.install :as install]
            [datalevin.federation.release-triage :as triage]
            [datalevin.util :as u]))

(defn -main
  "Read-only GitHub GETs; local schema/data/export live in a temporary directory.
  Optional owner, repository, numeric repository ID and work-item number.
  GITHUB_TOKEN is runtime-only. Mutable API reads carry best-effort guarantees;
  this live check does not claim an API snapshot/eager equivalence."
  [& [owner repository repository-id number]]
  (let [owner (or owner "octocat") repository (or repository "Hello-World")
        repository-id (Long/parseLong (or repository-id "1296269")) number (Long/parseLong (or number "1"))
        root (u/tmp-dir (str "federation-live-workflow-" (random-uuid)))
        _ (.mkdirs (io/file root "spools"))
        path (str root "/release.ednl")
        _ (spit path (str (pr-str {:issue [repository-id number] :owners [] :window "now" :tags ["live"]}) "\n"))
        accepted (triage/accept
                   {:relation-id :github/work-items
                    :source-instance-id #uuid "b7bca594-d387-4a21-8cf4-60379ecb5098"
                    :repository {:id repository-id :owner owner :name repository}}
                   #uuid "3d7919eb-d9cc-4c74-923c-37e88757c35b" (export/manifest path (random-uuid)))
        conn (d/create-conn (str root "/native") triage/native-schema {:wal? false :background-sampling? false})
        reopened (volatile! nil)]
    (try
      (triage/install! conn accepted {:expected-versions {:github nil :github/work-items nil
                                                         :release nil :release/assignments nil}})
      (d/transact! conn [{:triage/repository repository-id :triage/number number :triage/priority 10}])
      (d/close conn)
      (vreset! reopened (d/create-conn (str root "/native")))
      (let [loaded (install/load-definitions @reopened)
            sources (triage/sources loaded
                      {:compute-context-id :workflow/live-github :token (System/getenv "GITHUB_TOKEN")}
                      {:compute-context-id :workflow/live-release :path path :spool-directory (str root "/spools")})
            started (System/nanoTime)
            rows (d/q triage/lookup-query @@reopened (:github sources) (:release sources)
                      repository-id 9 [["now" :live]])
            elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)
            explain (d/explain {:run? true} triage/lookup-query @@reopened (:github sources) (:release sources)
                               repository-id 9 [["now" :live]])]
        (when-not (and (= 1 (count rows)) (= number (ffirst rows))
                       (string? (nth (first rows) 2)) (= "now" (last (first rows)))
                       (= 1 (:result-rows explain))
                       (zero? (:live-sessions (github/statistics (:github sources))))
                       (zero? (:live-spools (export/statistics (:release sources))))
                       (empty? (seq (.listFiles (io/file root "spools")))))
          (throw (ex-info "Live composed workflow did not complete cleanly" {})))
        (prn {:rows rows :first-query-elapsed-ms elapsed-ms
              :sources (:sources explain) :cursors (mapv #(select-keys % [:source-symbol :metrics]) (:cursors explain))
              :acquisition {:github (github/statistics (:github sources)) :export (export/statistics (:release sources))}
              :persisted-reopen? true}))
      (finally (when @reopened (d/close @reopened)) (d/close conn) (u/delete-files root) (shutdown-agents)))))
