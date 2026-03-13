(ns com.latacora.sqlite.backup
  "Backup and restore SQLite databases using the Online Backup API."
  (:import
   (org.sqlite SQLiteConnection)
   (org.sqlite.core DB DB$ProgressObserver)
   (java.sql Connection)))

(defn ^:private ->db
  ^DB [^Connection conn]
  (.getDatabase (.unwrap conn SQLiteConnection)))

(defn ^:private ->observer
  ^DB$ProgressObserver [f]
  (when f
    (reify DB$ProgressObserver
      (progress [_ remaining total]
        (f remaining total)))))

(defn backup!
  "Backs up a SQLite database to a file.

  conn       — the JDBC connection to back up
  dest-file  — path to the destination file (created or overwritten)

  Options (as trailing key-value pairs):
    :db-name    — database name to back up (default \"main\")
    :progress   — (fn [remaining total]) called after each step
    :pages      — pages to copy per step (default -1, meaning all at once)
    :sleep-ms   — milliseconds to sleep between steps (default 0)
    :retries    — retry attempts if database is locked (default 1)"
  [^Connection conn dest-file & {:keys [db-name progress pages sleep-ms retries]
                                  :or {db-name "main"
                                       pages -1
                                       sleep-ms 0
                                       retries 1}}]
  (let [db (->db conn)
        observer (->observer progress)]
    (.backup db db-name (str dest-file) observer (int pages) (int sleep-ms) (int retries))))

(defn restore!
  "Restores a SQLite database from a file.

  conn       — the JDBC connection to restore into
  src-file   — path to the source backup file

  Options (as trailing key-value pairs):
    :db-name    — database name to restore into (default \"main\")
    :progress   — (fn [remaining total]) called after each step
    :pages      — pages to copy per step (default -1, meaning all at once)
    :sleep-ms   — milliseconds to sleep between steps (default 0)
    :retries    — retry attempts if database is locked (default 1)"
  [^Connection conn src-file & {:keys [db-name progress pages sleep-ms retries]
                                 :or {db-name "main"
                                      pages -1
                                      sleep-ms 0
                                      retries 1}}]
  (let [db (->db conn)
        observer (->observer progress)]
    (.restore db db-name (str src-file) observer (int pages) (int sleep-ms) (int retries))))
