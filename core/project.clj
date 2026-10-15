(defproject org.clojars.huahaiy/datalevin-federation-core "0.1.0-SNAPSHOT"
  :description "Shared semantic mapping and fingerprint contracts for Datalevin federation"
  :url "https://github.com/datalevin/federation"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  ;; The application supplies a Datalevin build with the query-source SPI.
  ;; Published 1.2.5 supplies the build dependencies, but does not yet have it.
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [datalevin "1.2.5" :scope "provided"]]
  :profiles {:dev
             {:resource-paths ["../../datalevin/src"
                               "../../datalevin/resources"
                               "../../datalevin/target/classes"]}}
  :filespecs [{:type :path :path "../LICENSE"}]
  :jvm-opts ["--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
             "--enable-native-access=ALL-UNNAMED"
             "-Dclojure.compiler.direct-linking=true"]
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases false}]])
