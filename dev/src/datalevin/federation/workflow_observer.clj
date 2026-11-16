(ns datalevin.federation.workflow-observer
  "Measurement/test instrumentation at public adapter boundaries. Ledger peaks
  are observed lower bounds, not an exact internal high-water mark."
  (:require [datalevin.query.source :as spi]
            [datalevin.query.source.control :as control]))

(defn recorder []
  {:started (System/nanoTime) :controls (atom #{})
   :state (atom {:first-batch-ns nil :observed-ledger-peak {} :cursors [] :planning-callback-ns 0})})

(defn sample! [recorder ctl]
  (swap! (:controls recorder) conj ctl)
  (let [usage (control/usage ctl)]
    (swap! (:state recorder) update :observed-ledger-peak #(merge-with max % usage))))

(defrecord ObservedSession [delegate label recorder ctl]
  spi/IQuerySourceSession
  (-session-info [_] (spi/-session-info delegate))
  (-open-cursor [_ open]
    (let [cursor (spi/-open-cursor delegate open) closed? (atom false)]
      (sample! recorder ctl)
      (reify spi/IQuerySourceCursor
        (-next-batch! [_ fetch]
          (let [batch (spi/-next-batch! cursor fetch)]
            (swap! (:state recorder) update :first-batch-ns #(or % (- (System/nanoTime) (long (:started recorder)))))
            (sample! recorder ctl)
            batch))
        (-cursor-metrics [_] (spi/-cursor-metrics cursor))
        (-close-cursor! [_]
          (when (compare-and-set! closed? false true)
            (try (spi/-close-cursor! cursor)
                 (finally
                   (sample! recorder ctl)
                   (swap! (:state recorder) update :cursors conj
                     {:source label :input-rows (count (:bindings open)) :metrics (spi/-cursor-metrics cursor)}))))
          nil))))
  (-cancel-session! [_ reason] (spi/-cancel-session! delegate reason))
  (-close-session! [_]
    (try (spi/-close-session! delegate) (finally (sample! recorder ctl)))))

(defn observe [source label recorder]
  (reify spi/IQuerySource
    (-source-info [_] (spi/-source-info source))
    (-open-session [_ request]
      (let [ctl (:control request)]
        (sample! recorder ctl)
        (let [session (spi/-open-session source request)]
          (sample! recorder ctl)
          (->ObservedSession session label recorder ctl))))
    (-plan-source [_ session request]
      (let [started (System/nanoTime)]
        (try (spi/-plan-source source (:delegate session) request)
             (finally
               (swap! (:state recorder) update :planning-callback-ns + (- (System/nanoTime) started))
               (sample! recorder (:control request))))))))

(defn sources [sources recorder]
  (into {} (map (fn [[label source]] [label (observe source label recorder)])) sources))

(defn clean? [recorder]
  (every? (fn [ctl] (every? zero? (vals (dissoc (control/usage ctl) :external-requests)))) @(:controls recorder)))
