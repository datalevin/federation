(ns datalevin.federation.test-runner
  (:require [clojure.test :as test]
            [datalevin.federation.contract-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'datalevin.federation.contract-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
