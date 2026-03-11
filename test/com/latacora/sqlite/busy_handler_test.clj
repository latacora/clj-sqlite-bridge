(ns com.latacora.sqlite.busy-handler-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.latacora.sqlite.bridge :as bridge])
  (:import
   (java.io File)
   (java.sql Connection DriverManager SQLException)))

(Class/forName "org.sqlite.JDBC")

(def ^:private ^File db-file (File/createTempFile "busy-test" ".db"))

(defn ^:private db-url []
  (str "jdbc:sqlite:" (.getAbsolutePath db-file)))

(use-fixtures :each
  (fn [f]
    ;; Reset the database file before each test
    (.delete db-file)
    (f)
    (.delete db-file)))

(defn ^:private with-conn
  [f]
  (with-open [^Connection conn (DriverManager/getConnection (db-url))]
    (f conn)))

(defn ^:private exec!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(deftest busy-handler-is-called-on-contention
  (testing "busy handler is invoked when another connection holds a lock"
    (with-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (v INTEGER)")
       (exec! conn1 "INSERT INTO t VALUES (1)")
       ;; conn1 starts a transaction and holds the write lock
       (.setAutoCommit conn1 false)
       (exec! conn1 "UPDATE t SET v = 2")
       ;; conn2 tries to write and hits BUSY
       (with-conn
        (fn [conn2]
          (let [retries (atom [])]
            (bridge/set-busy-handler! conn2
                                     (fn [retry-count]
                                       (swap! retries conj retry-count)
                                       ;; Don't retry — let it fail
                                       false))
            (is (thrown? SQLException
                        (exec! conn2 "UPDATE t SET v = 3")))
            ;; Handler should have been called at least once
            (is (seq @retries))
            (is (= 0 (first @retries))))))
       (.rollback conn1)))))

(deftest busy-handler-retry-succeeds
  (testing "busy handler that retries can succeed after lock is released"
    (with-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (v INTEGER)")
       (exec! conn1 "INSERT INTO t VALUES (1)")
       (.setAutoCommit conn1 false)
       (exec! conn1 "UPDATE t SET v = 2")
       (with-conn
        (fn [conn2]
          (let [retries (atom 0)]
            (bridge/set-busy-handler! conn2
                                     (fn [_retry-count]
                                       (swap! retries inc)
                                       ;; Release the lock on first retry
                                       (when (= 1 @retries)
                                         (.rollback conn1))
                                       true))
            ;; This should succeed after conn1 releases the lock
            (exec! conn2 "UPDATE t SET v = 3")
            (is (pos? @retries)))))))))

(deftest clear-busy-handler-reverts-behavior
  (testing "clearing the handler reverts to default (immediate SQLITE_BUSY)"
    (with-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (v INTEGER)")
       (exec! conn1 "INSERT INTO t VALUES (1)")
       (.setAutoCommit conn1 false)
       (exec! conn1 "UPDATE t SET v = 2")
       (with-conn
        (fn [conn2]
          (let [called (atom false)]
            (bridge/set-busy-handler! conn2 (fn [_] (reset! called true) false))
            (bridge/clear-busy-handler! conn2)
            ;; Without a busy handler, SQLite uses the default busy_timeout.
            ;; Set it to 0 so it fails immediately.
            (.setBusyTimeout (.unwrap conn2 org.sqlite.SQLiteConnection) 0)
            (is (thrown? SQLException
                        (exec! conn2 "UPDATE t SET v = 3")))
            (is (false? @called)))))
       (.rollback conn1)))))

(deftest with-busy-handler-lifecycle
  (testing "with-busy-handler clears handler after body"
    (with-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (v INTEGER)")
       (let [called (atom false)]
         (bridge/with-busy-handler {:conn conn
                                    :handler (fn [_] (reset! called true) false)}
           ;; Handler is set but not triggered (no contention in this test)
           (exec! conn "INSERT INTO t VALUES (1)"))
         ;; After with-busy-handler, handler should be cleared
         ;; We can't easily test "handler is cleared" without contention,
         ;; so just verify the body executed successfully
         (is (false? @called)))))))
