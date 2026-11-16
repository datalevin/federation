(ns datalevin.federation.workflow-fixture
  "Controlled real HTTP/file/native topology shared by integration checks and
  measurements. Not an adapter or an alternative query evaluator."
  (:require [clojure.java.io :as io]
            [datalevin.core :as d]
            [datalevin.federation.file :as export]
            [datalevin.federation.install :as install]
            [datalevin.federation.release-triage :as triage]
            [datalevin.util :as u]
            [jsonista.core :as json])
  (:import [com.sun.net.httpserver HttpServer HttpHandler HttpExchange]
           [java.net InetSocketAddress]
           [java.nio.charset StandardCharsets]
           [java.util.concurrent Executors ExecutorService TimeUnit]))

(def github-id #uuid "94b33a77-7f73-4416-bff8-cd921d63be9b")
(def export-id #uuid "763951f5-80ea-41f4-8274-10c783e80505")
(def capture-id #uuid "07ecfe5e-c673-44de-bd6f-3a10da5f3c02")
(def declaration {:relation-id :github/work-items :source-instance-id github-id
                  :repository {:id 42 :owner "acme" :name "project"}})

(defn item [number]
  {"id" (+ 1000 (long number)) "number" number "title" (str "Work item " number)
   "labels" (if (= number 3) [] [{"name" "bug"} {"name" "bug"} {"name" "release"}])
   "assignees" (if (= number 4) [] [{"id" 7} {"id" 7} {"id" 8}])})

(defn write-export!
  "Write deterministic UTF-8 records, then explicitly accept this capture."
  [path records capture]
  (with-open [writer (io/writer path :encoding "UTF-8")]
    (doseq [record records] (.write writer (str (pr-str record) "\n"))))
  (triage/accept declaration export-id (export/manifest path capture)))

(defn records [n]
  (mapv (fn [i]
          {:issue [42 (inc (long i))] :owners [7 7 nil 8] :window "now" :tags ["release" "release" nil]})
        (range n)))

(defn- server! [observations behavior latency-ms]
  (let [server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
        executor (Executors/newCachedThreadPool)
        requests (atom {:repository 0 :issues 0 :body-bytes 0 :numbers #{} :number-counts {}})]
    (.setExecutor server executor)
    (.createContext server "/"
      (reify HttpHandler
        (handle [_ exchange]
          (let [^HttpExchange exchange exchange
                path (.getPath (.getRequestURI exchange))
                repository? (= path "/repos/acme/project")
                number (some-> (re-matches #"/repos/acme/project/issues/(\d+)" path) second Long/parseLong)
                default (cond repository? {"id" 42} number (get @observations number))
                response (if-let [respond @behavior] (respond exchange default)
                           {:status (if default 200 404) :json default})]
            (swap! requests (fn [before]
                              (cond-> (update before (if repository? :repository :issues) inc)
                                number (update :numbers conj number)
                                number (update-in [:number-counts number] (fnil inc 0)))))
            (try
              (when (pos? (long latency-ms)) (Thread/sleep (long latency-ms)))
              (let [body (.getBytes ^String (or (:raw response) (json/write-value-as-string (:json response)))
                                   StandardCharsets/UTF_8)]
                ;; Queue headers and the bounded body together, avoiding a fixture
                ;; delay between tiny writes that would dominate injected latency.
                (.set (.getResponseHeaders exchange) "Connection" "close")
                (.sendResponseHeaders exchange (long (get response :status 200)) (alength body))
                (when-let [before (:before-body response)] (before))
                (.write (.getResponseBody exchange) body)
                (swap! requests update :body-bytes + (alength body)))
              (catch java.io.IOException _ nil)
              (catch InterruptedException _ (.interrupt (Thread/currentThread)))
              (finally (.close exchange)))))))
    (.start server)
    {:base (str "http://127.0.0.1:" (.getPort (.getAddress server))) :requests requests
     :close! #(do (.stop server 0) (.shutdownNow ^ExecutorService executor)
                  (.awaitTermination ^ExecutorService executor 2 TimeUnit/SECONDS))}))

(defn create!
  "Caller owns close!. Controlled observations remain immutable unless a fault
  test explicitly changes them. Only selected native rows have high priority."
  [{:keys [native-rows export-rows selected-keys latency-ms records]
    :or {native-rows 8 export-rows 8 selected-keys 2 latency-ms 0}}]
  (let [root (u/tmp-dir (str "federation-workflow-" (random-uuid)))
        _ (.mkdirs (io/file root "spools"))
        records (or records (datalevin.federation.workflow-fixture/records export-rows))
        observations (atom (into {} (map (fn [n] [n (item n)])) (range 1 (inc (max native-rows export-rows)))))
        behavior (atom nil)
        api (server! observations behavior latency-ms)
        path (str root "/release.ednl")
        accepted (write-export! path records capture-id)
        conn (d/create-conn (str root "/native") triage/native-schema
                            {:wal? false :background-sampling? false})]
    (try
      (triage/install! conn accepted {:expected-versions {:github nil :github/work-items nil
                                                         :release nil :release/assignments nil}})
      (d/transact! conn
        (mapv (fn [number] {:triage/repository 42 :triage/number number
                            :triage/priority (if (<= (long number) (long selected-keys)) 10 1)})
              (range 1 (inc native-rows))))
      (let [profiles {:github {:api-base (:base api) :compute-context-id :workflow/github}
                      :release {:path path :spool-directory (str root "/spools")
                                :compute-context-id :workflow/release}}]
        {:root root :conn conn :profiles profiles :accepted accepted :records records
         :observations observations :behavior behavior :api api
         :sources (triage/sources (install/load-definitions conn) (:github profiles) (:release profiles))
         :close! #(try (d/close conn) (finally ((:close! api)) (u/delete-files root)))} )
      (catch Throwable e
        (try (d/close conn) (finally ((:close! api)) (u/delete-files root)))
        (throw e)))))

(defn clean? [fixture]
  (empty? (seq (.listFiles (io/file (:root fixture) "spools")))))

(defn eager [fixture]
  {:github (triage/eager-github (get-in fixture [:accepted :github]) (vals @(:observations fixture)))
   :release (triage/eager-release (get-in fixture [:accepted :release]) (:records fixture))})
