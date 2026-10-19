(ns datalevin.federation.local-transport-test
  (:require [clojure.test :refer [deftest]]
            [datalevin.federation.local :as local]
            [datalevin.query.source.transport-conformance :as transports]))

(deftest sqlite-export-and-paginated-http-matrix
  (transports/matrix! local/source))
