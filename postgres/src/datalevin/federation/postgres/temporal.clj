(ns datalevin.federation.postgres.temporal
  "Exact calendar values; intervals retain PostgreSQL's three independent fields."
  (:require [datalevin.federation.postgres.jdbc :as jdbc])
  (:import [java.math BigDecimal]
           [java.sql ResultSet]
           [java.time LocalDate LocalDateTime LocalTime OffsetTime]))

(defn interval
  "Decode PostgreSQL iso_8601 interval text without floating-point arithmetic."
  [s]
  (try
    (let [[_ y m d h minute seconds]
          (re-matches #"P(?:([+-]?\d+)Y)?(?:([+-]?\d+)M)?(?:([+-]?\d+)D)?(?:T(?:([+-]?\d+)H)?(?:([+-]?\d+)M)?(?:([+-]?\d+(?:\.\d+)?)S)?)?" s)
          n #(if % (bigint %) 0N)]
      (jdbc/ensure! (some some? [y m d h minute seconds]) :values)
      {:months (long (+ (*' 12 (n y)) (n m)))
       :days (long (n d))
       :microseconds (long (+ (*' 3600000000 (n h)) (*' 60000000 (n minute))
                              (if seconds (.longValueExact (.multiply (BigDecimal. ^String seconds)
                                                                     (BigDecimal. "1000000"))) 0)))})
    (catch Exception _ (jdbc/fail! :query-source/invalid :values))))

(defn decode [type ^ResultSet rs i]
  (when-let [text (.getString rs (int i))]
    ;; SQL 24:00 and infinity have no exact java.time counterpart.
    (when (or (#{"infinity" "-infinity"} text)
              (and (#{"time" "timetz"} type) (.startsWith ^String text "24:")))
      (jdbc/fail! :query-source/invalid :values))
    (case type
      "date" (.getObject rs (int i) LocalDate)
      "timestamp" (.getObject rs (int i) LocalDateTime)
      "time" (.getObject rs (int i) LocalTime)
      "timetz" (.getObject rs (int i) OffsetTime)
      "interval" (interval text))))
