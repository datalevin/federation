(ns datalevin.federation.local-conformance-test
  (:require [clojure.test :refer [deftest]]
            [datalevin.federation.local :as local]
            [datalevin.query.source.conformance :as conformance]))

(deftest tuple-execution-matrix
  (conformance/tuple-semantics! local/source))

(deftest eav-execution-matrix
  (conformance/eav-semantics! local/source))

(deftest native-execution-matrix
  (conformance/native-semantics! local/source))

(deftest lifecycle-execution-matrix
  (conformance/lifecycle! local/source local/replace-rows!))

(deftest key-execution-matrix
  (conformance/key-semantics! local/source))
