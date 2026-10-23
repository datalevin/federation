(ns datalevin.federation.test-runner
  (:require [clojure.test :as test]))

(defn -main [& namespaces]
  (let [namespaces (if (seq namespaces)
                     (mapv symbol namespaces)
                     '[datalevin.federation.contract-test
                       datalevin.federation.identity-test
                       datalevin.federation.identity-storage-test
                       datalevin.federation.mapping-v2-test
                       datalevin.federation.install-test
                       datalevin.federation.persistence-integration-test
                       datalevin.federation.file-test
                       datalevin.federation.github-test
                       datalevin.federation.workflow-test
                       datalevin.federation.local-test
                       datalevin.federation.local-batch-test
                       datalevin.federation.local-conformance-test
                       datalevin.federation.local-transport-test])
        _ (run! require namespaces)
        {:keys [fail error]} (apply test/run-tests namespaces)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
