(ns com.latacora.sqlite.bridge-stress-test
  "Concurrency stress tests for aggregate UDFs. Skipped by default;
  run with: bb test --focus :com.latacora.sqlite.bridge-stress-test"
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.bridge :as bridge])
  (:import
   (java.sql DriverManager Connection)
   (java.util.concurrent CountDownLatch Executors Future)))

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

(def ^:private clj-sum-spec
  {:init (constantly 0)
   :step (fn [acc x] (+ acc (long x)))
   :final identity})

(deftest ^:stress aggregate-concurrent-connections
  (testing "separate connections from multiple threads"
    (let [n-threads 8
          n-iterations 100
          pool (Executors/newFixedThreadPool n-threads)
          latch (CountDownLatch. n-threads)
          errors (atom [])
          futures (atom [])]
      (try
        (dotimes [_ n-threads]
          (swap! futures conj
                 (.submit pool
                          ^Callable
                          (fn []
                            (.countDown latch)
                            (.await latch)
                            (dotimes [_ n-iterations]
                              (try
                                (with-conn
                                 (fn [conn]
                                   (exec! conn "CREATE TABLE t (grp TEXT, v INTEGER)")
                                   (exec! conn "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4)")
                                   (bridge/add-aggregate! conn "clj_sum" clj-sum-spec)
                                   (let [rs (query-single conn "SELECT clj_sum(v) FROM t")]
                                     (when-not (= 10 (long rs))
                                       (swap! errors conj
                                              {:expected 10 :got rs :type :total})))
                                   (let [stmt (.createStatement conn)
                                         rs (.executeQuery stmt
                                                           "SELECT grp, clj_sum(v) FROM t GROUP BY grp ORDER BY grp")]
                                     (when (.next rs)
                                       (let [a-sum (.getLong rs 2)]
                                         (when-not (= 3 a-sum)
                                           (swap! errors conj
                                                  {:expected 3 :got a-sum :type :group-a}))))
                                     (when (.next rs)
                                       (let [b-sum (.getLong rs 2)]
                                         (when-not (= 7 b-sum)
                                           (swap! errors conj
                                                  {:expected 7 :got b-sum :type :group-b}))))
                                     (.close rs)
                                     (.close stmt))))
                                (catch Exception e
                                  (swap! errors conj {:exception e}))))))))
        (doseq [^Future f @futures]
          (.get f))
        (is (empty? @errors)
            (str "Concurrent aggregate errors: " (take 5 @errors)))
        (finally
          (.shutdown pool))))))

(deftest ^:stress aggregate-shared-connection
  (testing "multiple threads sharing one connection (SQLite serialized mode)"
    (with-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (grp TEXT, v INTEGER)")
       (exec! conn "INSERT INTO t VALUES ('a',1),('a',2),('b',3),('b',4)")
       (bridge/add-aggregate! conn "clj_sum" clj-sum-spec)
       (let [n-threads 8
             n-iterations 50
             pool (Executors/newFixedThreadPool n-threads)
             latch (CountDownLatch. n-threads)
             errors (atom [])
             futures (atom [])]
         (try
           (dotimes [_ n-threads]
             (swap! futures conj
                    (.submit pool
                             ^Callable
                             (fn []
                               (.countDown latch)
                               (.await latch)
                               (dotimes [_ n-iterations]
                                 (try
                                   (let [rs (query-single conn "SELECT clj_sum(v) FROM t")]
                                     (when-not (= 10 (long rs))
                                       (swap! errors conj
                                              {:expected 10 :got rs :type :total})))
                                   (locking conn
                                     (let [stmt (.createStatement conn)
                                           rs (.executeQuery stmt
                                                             "SELECT grp, clj_sum(v) FROM t GROUP BY grp ORDER BY grp")]
                                       (when (.next rs)
                                         (let [a-sum (.getLong rs 2)]
                                           (when-not (= 3 a-sum)
                                             (swap! errors conj
                                                    {:expected 3 :got a-sum :type :group-a}))))
                                       (when (.next rs)
                                         (let [b-sum (.getLong rs 2)]
                                           (when-not (= 7 b-sum)
                                             (swap! errors conj
                                                    {:expected 7 :got b-sum :type :group-b}))))
                                       (.close rs)
                                       (.close stmt)))
                                   (catch Exception e
                                     (swap! errors conj {:exception e}))))))))
           (doseq [^Future f @futures]
             (.get f))
           (is (empty? @errors)
               (str "Shared-connection aggregate errors: " (take 5 @errors)))
           (finally
             (.shutdown pool))))))))
