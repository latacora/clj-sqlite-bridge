(ns com.latacora.sqlite.limits-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.limits :as limits])
  (:import
   (java.sql Connection DriverManager)))

(Class/forName "org.sqlite.JDBC")

(defn ^:private with-conn
  [f]
  (with-open [^Connection conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (f conn)))

(deftest get-limit-returns-integer-values
  (testing "all known limits return integer values"
    (with-conn
     (fn [conn]
       (doseq [kw [:length :sql-length :column :expr-depth :compound-select
                    :vdbe-op :function-arg :attached :like-pattern-length
                    :variable-number :trigger-depth :worker-threads :page-count]]
         (let [v (limits/get-limit conn kw)]
           (is (integer? v) (str kw " should return an integer, got " v)))))))

  (testing "most limits have positive defaults"
    (with-conn
     (fn [conn]
       (doseq [kw [:length :sql-length :column :expr-depth :compound-select
                    :vdbe-op :function-arg :attached :like-pattern-length
                    :variable-number :trigger-depth]]
         (let [v (limits/get-limit conn kw)]
           (is (pos? v) (str kw " should have a positive default, got " v))))))))

(deftest set-limit-returns-previous-value
  (testing "set-limit! returns the previous value of the limit"
    (with-conn
     (fn [conn]
       (let [original (limits/get-limit conn :function-arg)
             prev (limits/set-limit! conn :function-arg 50)]
         (is (= original prev))
         (is (= 50 (limits/get-limit conn :function-arg))))))))

(deftest set-limit-roundtrip
  (testing "setting and getting a limit round-trips correctly"
    (with-conn
     (fn [conn]
       (limits/set-limit! conn :column 100)
       (is (= 100 (limits/get-limit conn :column)))))))

(deftest limits-are-per-connection
  (testing "limits set on one connection don't affect another"
    (with-conn
     (fn [conn1]
       (with-conn
        (fn [conn2]
          (let [original (limits/get-limit conn1 :function-arg)]
            (limits/set-limit! conn1 :function-arg 10)
            (is (= 10 (limits/get-limit conn1 :function-arg)))
            (is (= original (limits/get-limit conn2 :function-arg))))))))))

(deftest unknown-limit-throws
  (testing "unknown limit keyword throws IllegalArgumentException"
    (with-conn
     (fn [conn]
       (is (thrown? IllegalArgumentException
                    (limits/get-limit conn :nonexistent)))))))

(deftest set-limit-validates-value
  (testing "negative values are rejected"
    (with-conn
     (fn [conn]
       (is (thrown? IllegalArgumentException
                    (limits/set-limit! conn :column -1))))))

  (testing "values above Integer/MAX_VALUE are rejected"
    (with-conn
     (fn [conn]
       (is (thrown? IllegalArgumentException
                    (limits/set-limit! conn :column (inc Integer/MAX_VALUE)))))))

  (testing "non-integer values are rejected"
    (with-conn
     (fn [conn]
       (is (thrown? IllegalArgumentException
                    (limits/set-limit! conn :column 3.14)))))))
