(ns com.latacora.sqlite.bridge-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [com.latacora.sqlite.bridge :as bridge])
  (:import
   (java.sql DriverManager Connection PreparedStatement ResultSet SQLException)))

(defn ^:private with-conn
  [f]
  (Class/forName "org.sqlite.JDBC")
  (with-open [^Connection conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (f conn)))

(defn ^:private query-single
  [^Connection conn sql param-setter]
  (with-open [^PreparedStatement stmt (.prepareStatement conn sql)]
    (param-setter stmt)
    (with-open [^ResultSet rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(deftest with-func-adds-and-removes
  (with-conn
   (fn [conn]
     (bridge/with-func {:conn conn :func-name "echo" :func identity}
       (is (= "hi" (query-single conn "SELECT echo(?);" #(.setString % 1 "hi")))))
     (is (thrown? SQLException
                  (query-single conn "SELECT echo(?);" #(.setString % 1 "hi")))))))

(deftest udf-errors-surface
  (with-conn
   (fn [conn]
     (bridge/add-func! conn "boom" (fn [_] (throw (ex-info "boom" {}))))
     (is (thrown? SQLException
                  (query-single conn "SELECT boom(?);" #(.setLong % 1 1)))))))

(deftest boolean-return-coerces
  (with-conn
   (fn [conn]
     (bridge/with-func {:conn conn :func-name "is_even" :func (fn [n] (even? (long n)))}
       (is (= 1 (query-single conn "SELECT is_even(?);" #(.setLong % 1 4))))
       (is (= 0 (query-single conn "SELECT is_even(?);" #(.setLong % 1 5))))))))

(defspec echo-integers 100
  (prop/for-all [n gen/large-integer]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (= (long n)
            (query-single conn "SELECT echo(?);" #(.setLong % 1 (long n)))))))))

(defspec echo-doubles 100
  (prop/for-all [n (gen/double* {:NaN? false :infinite? false})]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (let [result (query-single conn "SELECT echo(?);" #(.setDouble % 1 n))]
           (<= (Math/abs (- (double result) n)) 1.0E-9)))))))

(defspec echo-strings 100
  (prop/for-all [s gen/string-alphanumeric]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (= s (query-single conn "SELECT echo(?);" #(.setString % 1 s))))))))

;; Helpers for listener tests

(defn ^:private exec!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn ^:private create-test-table!
  [conn]
  (exec! conn "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"))

;; Update listener tests

(deftest update-listener-fires-on-mutations
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [events (atom [])]
       (bridge/with-update-listener
        {:conn conn :listener-fn (fn [type db table row-id]
                                   (swap! events conj [type db table row-id]))}
        (exec! conn "INSERT INTO t (val) VALUES ('a')")
        (exec! conn "UPDATE t SET val = 'b' WHERE id = 1")
        (exec! conn "DELETE FROM t WHERE id = 1"))
       (is (= [[:insert "main" "t" 1]
               [:update "main" "t" 1]
               [:delete "main" "t" 1]]
              @events))))))

(deftest update-listener-removal-stops-notifications
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [events (atom [])
           f (fn [type db table row-id]
               (swap! events conj [type db table row-id]))]
       (bridge/add-update-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('a')")
       (bridge/remove-update-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('b')")
       (is (= [[:insert "main" "t" 1]] @events))))))

(deftest update-listener-deregister-fn
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [events (atom [])
           deregister (bridge/add-update-listener!
                       conn
                       (fn [type db table row-id]
                         (swap! events conj [type db table row-id])))]
       (exec! conn "INSERT INTO t (val) VALUES ('a')")
       (is (some? (:listener (meta deregister))))
       (deregister)
       (exec! conn "INSERT INTO t (val) VALUES ('b')")
       (is (= [[:insert "main" "t" 1]] @events))))))

;; Commit listener tests

(deftest commit-listener-fires-on-commit
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [commits (atom 0)]
       (.setAutoCommit conn false)
       (bridge/with-commit-listener
        {:conn conn :listener-fn (fn [] (swap! commits inc))}
        (exec! conn "INSERT INTO t (val) VALUES ('a')")
        (.commit conn)
        (exec! conn "INSERT INTO t (val) VALUES ('b')")
        (.commit conn))
       (is (= 2 @commits))))))

(deftest commit-listener-removal-via-remove-fn
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [commits (atom 0)
           f (fn [] (swap! commits inc))]
       (.setAutoCommit conn false)
       (bridge/add-commit-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('a')")
       (.commit conn)
       (bridge/remove-commit-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('b')")
       (.commit conn)
       (is (= 1 @commits))))))

;; Rollback listener tests

(deftest rollback-listener-fires-on-rollback
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [rollbacks (atom 0)]
       (.setAutoCommit conn false)
       (bridge/with-rollback-listener
        {:conn conn :listener-fn (fn [] (swap! rollbacks inc))}
        (exec! conn "INSERT INTO t (val) VALUES ('a')")
        (.rollback conn)
        (exec! conn "INSERT INTO t (val) VALUES ('b')")
        (.rollback conn))
       (is (= 2 @rollbacks))))))

(deftest rollback-listener-removal-via-remove-fn
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (let [rollbacks (atom 0)
           f (fn [] (swap! rollbacks inc))]
       (.setAutoCommit conn false)
       (bridge/add-rollback-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('a')")
       (.rollback conn)
       (bridge/remove-rollback-listener! conn f)
       (exec! conn "INSERT INTO t (val) VALUES ('b')")
       (.rollback conn)
       (is (= 1 @rollbacks))))))

;; Aggregate function tests

(defn ^:private query-all
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (let [cols (.getColumnCount (.getMetaData rs))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows (mapv #(.getObject rs (inc %)) (range cols))))
          rows)))))

(def ^:private clj-sum-spec
  {:init (constantly 0)
   :step (fn [acc x] (+ acc (long x)))
   :final identity})

(deftest aggregate-basic-sum
  (with-conn
   (fn [conn]
     (exec! conn "CREATE TABLE nums (v INTEGER)")
     (exec! conn "INSERT INTO nums (v) VALUES (10), (20), (30)")
     (bridge/with-aggregate
      {:conn conn :func-name "clj_sum" :agg-spec clj-sum-spec}
      (is (= 60 (query-single conn "SELECT clj_sum(v) FROM nums" identity)))))))

(deftest aggregate-empty-table
  (with-conn
   (fn [conn]
     (create-test-table! conn)
     (bridge/with-aggregate
      {:conn conn :func-name "clj_sum" :agg-spec clj-sum-spec}
      (is (= 0 (query-single conn "SELECT clj_sum(val) FROM t" identity)))))))

(deftest aggregate-group-by
  (testing "separate groups get independent state"
    (with-conn
     (fn [conn]
       (exec! conn "CREATE TABLE sales (region TEXT, amount INTEGER)")
       (exec! conn "INSERT INTO sales VALUES ('north', 10), ('south', 20), ('north', 30), ('south', 40)")
       (bridge/with-aggregate
        {:conn conn :func-name "clj_sum" :agg-spec clj-sum-spec}
        (let [rows (query-all conn "SELECT region, clj_sum(amount) FROM sales GROUP BY region ORDER BY region")]
          (is (= [["north" 40] ["south" 60]] rows))))))))

(deftest aggregate-string-concat
  (with-conn
   (fn [conn]
     (exec! conn "CREATE TABLE words (w TEXT)")
     (exec! conn "INSERT INTO words VALUES ('hello'), (' '), ('world')")
     (bridge/with-aggregate
      {:conn conn :func-name "clj_concat"
       :agg-spec {:init (constantly [])
                  :step (fn [acc s] (conj acc s))
                  :final (fn [acc] (apply str acc))}}
      (is (= "hello world"
             (query-single conn "SELECT clj_concat(w) FROM words" identity)))))))

(deftest aggregate-with-func-lifecycle
  (testing "aggregate is removed after with-aggregate"
    (with-conn
     (fn [conn]
       (create-test-table! conn)
       (bridge/with-aggregate
        {:conn conn :func-name "clj_sum" :agg-spec clj-sum-spec}
        (is (some? (query-single conn "SELECT clj_sum(val) FROM t" identity))))
       (is (thrown? SQLException
                    (query-single conn "SELECT clj_sum(val) FROM t" identity)))))))

(deftest aggregate-error-in-step
  (with-conn
   (fn [conn]
     (exec! conn "CREATE TABLE nums (v INTEGER)")
     (exec! conn "INSERT INTO nums VALUES (1), (2)")
     (bridge/with-aggregate
      {:conn conn :func-name "bad_agg"
       :agg-spec {:init (constantly nil)
                  :step (fn [_ _] (throw (ex-info "step boom" {})))
                  :final identity}}
      (is (thrown? SQLException
                   (query-single conn "SELECT bad_agg(v) FROM nums" identity)))))))

(defspec aggregate-sum-matches-builtin 50
  (prop/for-all [values (gen/not-empty (gen/vector gen/large-integer))]
    (with-conn
     (fn [conn]
       (exec! conn "CREATE TABLE nums (v INTEGER)")
       (doseq [v values]
         (exec! conn (str "INSERT INTO nums VALUES (" v ")")))
       (bridge/with-aggregate
        {:conn conn :func-name "clj_sum" :agg-spec clj-sum-spec}
        (let [builtin (query-single conn "SELECT SUM(v) FROM nums" identity)
              custom (query-single conn "SELECT clj_sum(v) FROM nums" identity)]
          (= (long builtin) (long custom))))))))
