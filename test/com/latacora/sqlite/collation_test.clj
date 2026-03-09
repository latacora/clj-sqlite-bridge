(ns com.latacora.sqlite.collation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.collation :as collation])
  (:import
   (java.sql DriverManager Connection SQLException)))

(defn ^:private with-conn
  [f]
  (Class/forName "org.sqlite.JDBC")
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

(deftest collation-order-by
  (testing "custom collation affects ORDER BY"
    (with-conn
     (fn [conn]
       ;; Reverse collation: sort in reverse lexicographic order
       (collation/with-collation
        {:conn conn
         :collation-name "reverse"
         :comparator (fn [a b] (compare b a))}
        (exec! conn "CREATE TABLE t (name TEXT)")
        (exec! conn "INSERT INTO t VALUES ('apple'), ('banana'), ('cherry')")
        (let [rows (query-all conn "SELECT name FROM t ORDER BY name COLLATE reverse")]
          (is (= [["cherry"] ["banana"] ["apple"]] rows))))))))

(deftest collation-equality
  (testing "custom collation affects equality comparisons"
    (with-conn
     (fn [conn]
       ;; Case-insensitive collation using Java toLowerCase
       (collation/with-collation
        {:conn conn
         :collation-name "nocase_java"
         :comparator (fn [a b] (.compareTo (.toLowerCase a) (.toLowerCase b)))}
        (exec! conn "CREATE TABLE t (name TEXT COLLATE nocase_java)")
        (exec! conn "INSERT INTO t VALUES ('Hello'), ('HELLO'), ('hello')")
        (let [rows (query-all conn "SELECT DISTINCT name FROM t")]
          (is (= 1 (count rows)))))))))

(deftest collation-group-by
  (testing "custom collation affects GROUP BY"
    (with-conn
     (fn [conn]
       (collation/with-collation
        {:conn conn
         :collation-name "nocase_java"
         :comparator (fn [a b] (.compareTo (.toLowerCase a) (.toLowerCase b)))}
        (exec! conn "CREATE TABLE t (name TEXT COLLATE nocase_java, v INTEGER)")
        (exec! conn "INSERT INTO t VALUES ('Foo', 1), ('foo', 2), ('FOO', 3)")
        (let [rows (query-all conn "SELECT name, SUM(v) FROM t GROUP BY name")]
          (is (= 1 (count rows)))
          (is (= 6 (long (second (first rows)))))))))))

(deftest collation-lifecycle
  (testing "collation is removed after with-collation"
    (with-conn
     (fn [conn]
       (collation/with-collation
        {:conn conn
         :collation-name "mycoll"
         :comparator compare}
        (exec! conn "CREATE TABLE t (name TEXT)")
        (exec! conn "INSERT INTO t VALUES ('a')")
        (is (some? (query-all conn "SELECT name FROM t ORDER BY name COLLATE mycoll"))))
       (is (thrown? SQLException
                    (query-all conn "SELECT name FROM t ORDER BY name COLLATE mycoll")))))))
