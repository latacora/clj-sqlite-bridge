(ns com.latacora.sqlite.progress-handler-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.bridge :as bridge])
  (:import
   (java.sql Connection DriverManager SQLException)))

(defn ^:private with-conn
  [f]
  (Class/forName "org.sqlite.JDBC")
  (with-open [^Connection conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (f conn)))

(defn ^:private exec!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn ^:private query-single
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (when (.next rs)
      (.getObject rs 1))))

(defn ^:private create-workload!
  "Creates a table with enough data to trigger the progress handler."
  [conn]
  (exec! conn "CREATE TABLE t (v INTEGER)")
  (exec! conn "INSERT INTO t VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
  ;; Cross-join to make a large enough result set
  (exec! conn "CREATE TABLE big AS SELECT a.v AS x, b.v AS y FROM t a, t b"))

(deftest progress-handler-is-called
  (testing "progress handler is invoked during query execution"
    (with-conn
     (fn [conn]
       (create-workload! conn)
       (let [call-count (atom 0)]
         (bridge/with-progress-handler
          {:conn conn :n 1 :handler (fn [] (swap! call-count inc) false)}
          (query-single conn "SELECT SUM(x * y) FROM big"))
         (is (pos? @call-count)))))))

(deftest progress-handler-interrupts-query
  (testing "returning truthy interrupts the query"
    (with-conn
     (fn [conn]
       (create-workload! conn)
       (bridge/with-progress-handler
        {:conn conn :n 1 :handler (constantly true)}
        (is (thrown? SQLException
                    (query-single conn "SELECT SUM(x * y) FROM big"))))))))

(deftest progress-handler-conditional-cancel
  (testing "handler can cancel after a threshold"
    (with-conn
     (fn [conn]
       (create-workload! conn)
       (let [call-count (atom 0)]
         (bridge/with-progress-handler
          {:conn conn :n 1
           :handler (fn []
                      (swap! call-count inc)
                      ;; Cancel after 5 callbacks
                      (> @call-count 5))}
          (is (thrown? SQLException
                      (query-single conn "SELECT SUM(x * y) FROM big")))
          (is (> @call-count 5))))))))

(deftest clear-progress-handler-stops-callbacks
  (testing "clearing the handler stops it from being called"
    (with-conn
     (fn [conn]
       (create-workload! conn)
       (let [called (atom false)]
         (bridge/set-progress-handler! conn 1
                                       (fn [] (reset! called true) false))
         (bridge/clear-progress-handler! conn)
         (query-single conn "SELECT SUM(x * y) FROM big")
         (is (false? @called)))))))

(deftest with-progress-handler-lifecycle
  (testing "handler is cleared after with-progress-handler exits"
    (with-conn
     (fn [conn]
       (create-workload! conn)
       ;; First query with handler that always interrupts
       (is (thrown? SQLException
                    (bridge/with-progress-handler
                     {:conn conn :n 1 :handler (constantly true)}
                     (query-single conn "SELECT SUM(x * y) FROM big"))))
       ;; After exiting, query should succeed
       (is (some? (query-single conn "SELECT SUM(x * y) FROM big")))))))
