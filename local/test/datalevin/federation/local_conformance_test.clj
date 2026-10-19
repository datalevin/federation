(ns datalevin.federation.local-conformance-test
  (:require [clojure.test :refer [deftest]]
            [datalevin.federation.local :as local]
            [datalevin.query.source :as spi]
            [datalevin.query.source.conformance :as conformance]
            [datalevin.query.source.mapped-conformance :as mapped]))

(deftest tuple-execution-matrix
  (conformance/tuple-semantics! (conformance/source-constructor #(local/source %1 %2 {:mode :scan}))))

(deftest eav-execution-matrix
  (conformance/eav-semantics! (conformance/source-constructor #(local/source %1 %2 {:mode :scan}))))

(deftest native-execution-matrix
  (conformance/native-semantics! (conformance/source-constructor #(local/source %1 %2 {:mode :scan}))))

(deftest lifecycle-execution-matrix
  (conformance/lifecycle! (conformance/source-constructor #(local/source %1 %2 {:mode :scan}))
                          #(conformance/replace-rows local/replace-rows! %1 %2)))

(deftest key-execution-matrix
  (conformance/key-semantics! (conformance/source-constructor #(local/source %1 %2 {:mode :scan}))))

(defrecord ConfiguredSession [delegate options]
  spi/IQuerySourceSession
  (-session-info [_]
    (cond-> (spi/-session-info delegate)
      (:consumption options) (assoc :consumption (:consumption options))))
  (-open-cursor [_ request] (spi/-open-cursor delegate request))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_] (spi/-close-session! delegate)))

(defn- configured-source [info facts options]
  ;; Adversarial replay/permission declarations belong to this test wrapper.
  ;; Production local snapshots always promise identical replay and many opens.
  (let [provider (local/source info facts (select-keys options [:mode]))]
    (reify spi/IQuerySource
      (-source-info [_] (spi/-source-info provider))
      (-open-session [_ request] (->ConfiguredSession (spi/-open-session provider request) options))
      (-plan-source [_ session request]
        (update (spi/-plan-source provider (:delegate session) request) :alternatives
          (fn [alternatives]
            (mapv #(cond-> %
                     (:replay options) (assoc-in [:properties :replay] (:replay options))
                     (:constraints options) (assoc :constraints (:constraints options))) alternatives)))))))

(deftest mapped-values
  (mapped/values! configured-source))

(deftest mapped-dependencies
  (mapped/dependencies! configured-source))

(deftest mapped-regions
  (mapped/regions! configured-source))

(deftest mapped-explanations
  (mapped/explanations! configured-source))

(deftest mapped-native
  (mapped/native! configured-source))

(deftest mapped-caller-seeds
  (mapped/caller-seeds! configured-source))

(deftest mapped-ownership
  (mapped/ownership! configured-source))

(deftest mapped-failures
  (mapped/failures! configured-source))

(deftest mapped-pinned-versions
  (mapped/lifecycle! local/source local/replace-rows!))
