(ns com.latacora.sqlite.serialization-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.serialization :as ser])
  (:import
   (java.sql Connection DriverManager)))

(Class/forName "org.sqlite.JDBC")

(defn ^:private with-conn
  [f]
  (with-open [^Connection conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (f conn)))

(defn ^:private exec!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn ^:private query-all
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (let [cols (.getColumnCount (.getMetaData rs))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows (mapv #(.getObject rs (inc %)) (range cols))))
          rows)))))

(deftest serialize-returns-bytes
  (testing "serialize returns a non-empty byte array"
    (with-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (v INTEGER)")
       (exec! conn "INSERT INTO t VALUES (42)")
       (let [data (ser/serialize conn)]
         (is (bytes? data))
         (is (pos? (alength data))))))))

(deftest serialize-deserialize-roundtrip
  (testing "data survives a serialize-then-deserialize cycle"
    (with-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
       (exec! conn1 "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
       (let [data (ser/serialize conn1)]
         (with-conn
          (fn [conn2]
            (ser/deserialize! conn2 data)
            (is (= [[1 "alice"] [2 "bob"]]
                   (query-all conn2 "SELECT * FROM t ORDER BY id"))))))))))

(deftest deserialize-replaces-existing-data
  (testing "deserialize replaces the current database contents"
    (with-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (v INTEGER)")
       (exec! conn1 "INSERT INTO t VALUES (1)")
       (let [data (ser/serialize conn1)]
         (with-conn
          (fn [conn2]
            (exec! conn2 "CREATE TABLE other (x TEXT)")
            (exec! conn2 "INSERT INTO other VALUES ('hello')")
            (ser/deserialize! conn2 data)
            ;; conn2 should now have table t, not other
            (is (= [[1]] (query-all conn2 "SELECT v FROM t"))))))))))

(deftest empty-database-roundtrip
  (testing "an empty database can be serialized and deserialized"
    (with-conn
     (fn [conn1]
       (let [data (ser/serialize conn1)]
         (is (bytes? data))
         (with-conn
          (fn [conn2]
            (ser/deserialize! conn2 data)
            ;; Should have no tables
            (is (= []
                   (query-all conn2
                              "SELECT name FROM sqlite_master WHERE type='table'"))))))))))
