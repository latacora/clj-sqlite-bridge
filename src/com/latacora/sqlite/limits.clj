(ns com.latacora.sqlite.limits
  "Get and set SQLite per-connection runtime limits."
  (:import
   (org.sqlite SQLiteConnection SQLiteLimits)
   (java.sql Connection)))

(def ^:private limit-keywords
  "Map of keyword to SQLiteLimits enum value."
  {:length              SQLiteLimits/SQLITE_LIMIT_LENGTH
   :sql-length          SQLiteLimits/SQLITE_LIMIT_SQL_LENGTH
   :column              SQLiteLimits/SQLITE_LIMIT_COLUMN
   :expr-depth          SQLiteLimits/SQLITE_LIMIT_EXPR_DEPTH
   :compound-select     SQLiteLimits/SQLITE_LIMIT_COMPOUND_SELECT
   :vdbe-op             SQLiteLimits/SQLITE_LIMIT_VDBE_OP
   :function-arg        SQLiteLimits/SQLITE_LIMIT_FUNCTION_ARG
   :attached            SQLiteLimits/SQLITE_LIMIT_ATTACHED
   :like-pattern-length SQLiteLimits/SQLITE_LIMIT_LIKE_PATTERN_LENGTH
   :variable-number     SQLiteLimits/SQLITE_LIMIT_VARIABLE_NUMBER
   :trigger-depth       SQLiteLimits/SQLITE_LIMIT_TRIGGER_DEPTH
   :worker-threads      SQLiteLimits/SQLITE_LIMIT_WORKER_THREADS
   :page-count          SQLiteLimits/SQLITE_LIMIT_PAGE_COUNT})

(defn ^:private ->sqlite-conn
  ^SQLiteConnection [^Connection conn]
  (.unwrap conn SQLiteConnection))

(defn ^:private limit-ordinal
  "Returns the integer ID for a limit keyword."
  [limit-kw]
  (let [^SQLiteLimits limit-enum (get limit-keywords limit-kw)]
    (when-not limit-enum
      (throw (IllegalArgumentException.
              (str "Unknown limit: " limit-kw
                   ". Valid limits: " (sort (keys limit-keywords))))))
    (.ordinal limit-enum)))

(defn get-limit
  "Returns the current value of a SQLite limit.

  limit-kw is one of: :length, :sql-length, :column, :expr-depth,
  :compound-select, :vdbe-op, :function-arg, :attached,
  :like-pattern-length, :variable-number, :trigger-depth,
  :worker-threads, :page-count."
  [^Connection conn limit-kw]
  (let [db (.getDatabase (->sqlite-conn conn))]
    (.limit db (limit-ordinal limit-kw) -1)))

(defn set-limit!
  "Sets a SQLite limit and returns the previous value.

  See get-limit for valid limit-kw values."
  [^Connection conn limit-kw value]
  (let [db (.getDatabase (->sqlite-conn conn))]
    (.limit db (limit-ordinal limit-kw) (int value))))
