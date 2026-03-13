(ns com.latacora.sqlite.backup-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [com.latacora.sqlite.backup :as backup])
  (:import
   (java.io File)
   (java.sql Connection DriverManager)))

(Class/forName "org.sqlite.JDBC")

(def ^:private ^File backup-file (File/createTempFile "backup-test" ".db"))

(use-fixtures :each
  (fn [f]
    (.delete backup-file)
    (f)
    (.delete backup-file)))

(defn ^:private with-conn
  [url f]
  (with-open [^Connection conn (DriverManager/getConnection url)]
    (f conn)))

(defn ^:private with-mem-conn [f]
  (with-conn "jdbc:sqlite::memory:" f))

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

(deftest backup-and-restore-roundtrip
  (testing "data survives a backup-then-restore cycle"
    (with-mem-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
       (exec! conn1 "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
       (backup/backup! conn1 (.getAbsolutePath backup-file))

       ;; Restore into a fresh connection
       (with-mem-conn
        (fn [conn2]
          (backup/restore! conn2 (.getAbsolutePath backup-file))
          (is (= [[1 "alice"] [2 "bob"]]
                 (query-all conn2 "SELECT * FROM t ORDER BY id")))))))))

(deftest backup-progress-callback
  (testing "progress callback is invoked during backup"
    (with-mem-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (v INTEGER)")
       (exec! conn "INSERT INTO t VALUES (1)")
       (let [calls (atom [])]
         (backup/backup! conn (.getAbsolutePath backup-file)
                         :progress (fn [remaining total]
                                     (swap! calls conj [remaining total])))
         (is (seq @calls) "progress should have been called at least once")
         ;; Last call should have remaining=0
         (is (zero? (first (last @calls)))))))))

(deftest restore-progress-callback
  (testing "progress callback is invoked during restore"
    (with-mem-conn
     (fn [conn1]
       (exec! conn1 "CREATE TABLE t (v INTEGER)")
       (exec! conn1 "INSERT INTO t VALUES (1)")
       (backup/backup! conn1 (.getAbsolutePath backup-file))

       (with-mem-conn
        (fn [conn2]
          (let [calls (atom [])]
            (backup/restore! conn2 (.getAbsolutePath backup-file)
                             :progress (fn [remaining total]
                                         (swap! calls conj [remaining total])))
            (is (seq @calls)))))))))

(deftest backup-overwrites-existing-file
  (testing "backup overwrites an existing backup file"
    (with-mem-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (v INTEGER)")
       (exec! conn "INSERT INTO t VALUES (1)")
       (backup/backup! conn (.getAbsolutePath backup-file))
       ;; Modify data and backup again
       (exec! conn "DELETE FROM t")
       (exec! conn "INSERT INTO t VALUES (42)")
       (backup/backup! conn (.getAbsolutePath backup-file))

       (with-mem-conn
        (fn [conn2]
          (backup/restore! conn2 (.getAbsolutePath backup-file))
          (is (= [[42]] (query-all conn2 "SELECT v FROM t")))))))))

(deftest backup-with-stepped-pages
  (testing "backup works with explicit page count"
    (with-mem-conn
     (fn [conn]
       (exec! conn "CREATE TABLE t (v TEXT)")
       (doseq [_ (range 100)]
         (exec! conn (str "INSERT INTO t VALUES ('" (apply str (repeat 100 "x")) "')")))
       (let [calls (atom [])]
         (backup/backup! conn (.getAbsolutePath backup-file)
                         :pages 1
                         :progress (fn [remaining total]
                                     (swap! calls conj [remaining total])))
         ;; With pages=1, there should be multiple progress calls
         (is (> (count @calls) 1)))))))

(deftest backup-validates-numeric-options
  (testing "overflow values are rejected"
    (with-mem-conn
     (fn [conn]
       (is (thrown? IllegalArgumentException
                    (backup/backup! conn (.getAbsolutePath backup-file)
                                    :pages (inc Integer/MAX_VALUE))))
       (is (thrown? IllegalArgumentException
                    (backup/backup! conn (.getAbsolutePath backup-file)
                                    :sleep-ms (inc Integer/MAX_VALUE))))
       (is (thrown? IllegalArgumentException
                    (backup/restore! conn (.getAbsolutePath backup-file)
                                     :retries 3.14)))))))
