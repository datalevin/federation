(defproject org.clojars.huahaiy/datalevin-federation-local "1.2.11"
  :description "Declared-schema local relation adapter for Datalevin federation"
  :url "https://github.com/datalevin/federation"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  ;; The application supplies the matching Datalevin release.
  ;; Development links the sibling checkout through checkouts/datalevin; see README.
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [datalevin "1.2.11" :scope "provided"]]
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
