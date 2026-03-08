(ns com.latacora.sqlite.bridge
  "Helpers for embedding Clojure into SQLite, providing functionality to add custom
  functions and listeners to SQLite."
  (:import
   (org.sqlite Function Function$Aggregate SQLiteConnection SQLiteCommitListener
              SQLiteUpdateListener SQLiteUpdateListener$Type)
   (org.sqlite.core Codes)
   (java.lang.reflect Method)
   (java.sql Connection)
   (java.util.concurrent ConcurrentHashMap)))

(def ^:private func-methods
  (->> Function
       .getDeclaredMethods
       (keep
        (fn [^Method m]
          (when-let [[base val-type]
                     (->> m .getName (re-matches #"args|result|error|value_([a-z]*)"))]
            (Method/.setAccessible m true)
            (let [ks (cond
                       (= base "args") [:args]
                       (= base "error") [:error]
                       (= base "result") [:result (-> m .getParameterTypes first)]
                       (= val-type "type") [:get-arg-type]
                       :else
                       [:get-arg
                        (case val-type
                          "int" "int"
                          "double" Codes/SQLITE_FLOAT
                          "text" Codes/SQLITE_TEXT
                          "blob" Codes/SQLITE_BLOB
                          "long" Codes/SQLITE_INTEGER)])]
              [ks m]))))
       (reduce (fn [acc [ks method]] (assoc-in acc ks method)) {})))

(defn ^:private resolve-args!
  [^Function func]
  (let [invoke (fn [^Method m & args]
                 (Method/.invoke m func (object-array args)))
        n-args (-> func-methods :args invoke)]
    (mapv
     (fn [i]
       (let [arg-type (-> func-methods :get-arg-type (invoke (int i)))]
         (if (= arg-type Codes/SQLITE_NULL)
           nil
           (-> func-methods (get-in [:get-arg arg-type]) (invoke (int i))))))
     (range n-args))))

(def ^:private unboxed-types
  "Mapping from boxed types (like Double) to unboxed types (like double)."
  {Double Double/TYPE
   Integer Integer/TYPE
   Long Long/TYPE})

(defn ^:private maybe-unbox-type
  "Given a maybe-boxed type, return the unboxed type."
  [maybe-boxed]
  (get unboxed-types maybe-boxed maybe-boxed))

(defn ^:private ->returnable-type
  "Given a return value from a function to be used as a SQLite function, coerce it
  to a value SQLite will understand."
  [return-val]
  (cond
    (boolean? return-val) (if return-val 1 0)
    :else return-val))

(defn ^:private return!
  [^Function func return-val]
  (let [return-val (->returnable-type return-val)
        return-type (-> return-val type maybe-unbox-type)
        method (get-in func-methods [:result return-type])]
    (Method/.invoke method func (object-array [return-val]))))

(defn ^:private ->Function
  "Converts a Clojure function to a SQLite Function."
  [f]
  (proxy [Function] []
    (xFunc []
      (try
        (->> this (resolve-args!) (apply f) (return! this))
        (catch Exception e
          (let [error-method (get func-methods :error)]
            (Method/.invoke error-method this (object-array [(str e)]))))))))

(defn ^:private ->Aggregate
  "Converts a map of Clojure functions to a SQLite Aggregate Function.

  The map must contain:
    :step  - (fn [acc & args]) called for each row, returns new accumulator
    :final - (fn [acc]) called after all rows, returns the aggregate result
    :init  - (fn []) called to create the initial accumulator for each group

  Note: clone() is intentionally not overridden. SQLite calls Object.clone() to
  create per-group instances, which preserves the native context pointer needed
  for arg/result access. Per-group state is stored in an external
  ConcurrentHashMap keyed by identity hash code."
  [{:keys [step final init]}]
  (let [^ConcurrentHashMap states (ConcurrentHashMap.)
        get-state (fn [this]
                    (.computeIfAbsent states (System/identityHashCode this)
                                      (reify java.util.function.Function
                                        (apply [_ _k] (atom (init))))))]
    (proxy [Function$Aggregate] []
      (xStep []
        (try
          (let [state (get-state this)
                args (resolve-args! this)]
            (swap! state #(apply step % args)))
          (catch Exception e
            (let [error-method (get func-methods :error)]
              (Method/.invoke error-method this (object-array [(str e)]))))))
      (xFinal []
        (try
          (let [state-atom (get-state this)
                result (final @state-atom)]
            (.remove states (System/identityHashCode this))
            (return! this result))
          (catch Exception e
            (let [error-method (get func-methods :error)]
              (Method/.invoke error-method this (object-array [(str e)])))))))))

(defn add-func!
  "Adds a Clojure function as a SQLite user-defined function."
  [^Connection conn func-name f]
  (Function/create conn func-name (->Function f)))

(defn add-aggregate!
  "Adds a Clojure aggregate function to a SQLite connection.

  agg-spec is a map with :step, :final, and :init — see ->Aggregate."
  [^Connection conn func-name agg-spec]
  (Function/create conn func-name (->Aggregate agg-spec)))

(defn remove-func!
  "Removes a previously added SQLite user-defined function (scalar or aggregate)."
  [^Connection conn func-name]
  (Function/destroy conn func-name))

(defmacro with-func
  "Executes body with a SQLite user-defined function temporarily added to the
  connection."
  [{:keys [conn func-name func]} & body]
  `(try
     (add-func! ~conn ~func-name ~func)
     ~@body
     (finally
       (remove-func! ~conn ~func-name))))

(defmacro with-aggregate
  "Executes body with a SQLite aggregate function temporarily added to the
  connection."
  [{:keys [conn func-name agg-spec]} & body]
  `(try
     (add-aggregate! ~conn ~func-name ~agg-spec)
     ~@body
     (finally
       (remove-func! ~conn ~func-name))))

;; Listener support

(def ^:private update-type->kw
  {SQLiteUpdateListener$Type/INSERT :insert
   SQLiteUpdateListener$Type/UPDATE :update
   SQLiteUpdateListener$Type/DELETE :delete})

(def ^:private listener-cache
  "Memoization cache: [conn clj-fn listener-type] -> Java listener object."
  (atom {}))

(defn ^:private ->sqlite-conn
  ^SQLiteConnection [^Connection conn]
  (.unwrap conn SQLiteConnection))

(defn ^:private cached-listener
  "Returns a cached Java listener for the given conn, fn, and type, creating it
  if necessary."
  [conn f listener-type make-listener]
  (let [k [conn f listener-type]]
    (or (get @listener-cache k)
        (let [listener (make-listener f)]
          (swap! listener-cache assoc k listener)
          listener))))

(defn ^:private uncache-listener
  [conn f listener-type]
  (let [k [conn f listener-type]]
    (when-let [listener (get @listener-cache k)]
      (swap! listener-cache dissoc k)
      listener)))

;; Update listeners

(defn ^:private ->update-listener
  [f]
  (reify SQLiteUpdateListener
    (onUpdate [_ type database table row-id]
      (f (update-type->kw type) database table row-id))))

(defn add-update-listener!
  "Registers a function as a SQLite update listener. The function will be called
  with (type database table row-id) where type is :insert, :update, or :delete.

  Returns a zero-arg function that deregisters the listener. The underlying Java
  listener object is available as :listener metadata on the returned function."
  [^Connection conn f]
  (let [sqlite-conn (->sqlite-conn conn)
        listener (cached-listener conn f ::update ->update-listener)]
    (.addUpdateListener sqlite-conn listener)
    (with-meta
      (fn [] (.removeUpdateListener sqlite-conn listener))
      {:listener listener})))

(defn remove-update-listener!
  "Removes a previously registered update listener."
  [^Connection conn f]
  (when-let [listener (uncache-listener conn f ::update)]
    (.removeUpdateListener (->sqlite-conn conn) listener)))

(defmacro with-update-listener
  "Executes body with an update listener registered for the duration."
  [{:keys [conn listener-fn]} & body]
  `(let [deregister# (add-update-listener! ~conn ~listener-fn)]
     (try
       ~@body
       (finally
         (deregister#)))))

;; Commit listeners

(defn ^:private ->commit-listener
  [f]
  (reify SQLiteCommitListener
    (onCommit [_] (f))
    (onRollback [_])))

(defn add-commit-listener!
  "Registers a zero-arg function as a SQLite commit listener.

  Returns a zero-arg function that deregisters the listener. The underlying Java
  listener object is available as :listener metadata on the returned function."
  [^Connection conn f]
  (let [sqlite-conn (->sqlite-conn conn)
        listener (cached-listener conn f ::commit ->commit-listener)]
    (.addCommitListener sqlite-conn listener)
    (with-meta
      (fn [] (.removeCommitListener sqlite-conn listener))
      {:listener listener})))

(defn remove-commit-listener!
  "Removes a previously registered commit listener."
  [^Connection conn f]
  (when-let [listener (uncache-listener conn f ::commit)]
    (.removeCommitListener (->sqlite-conn conn) listener)))

(defmacro with-commit-listener
  "Executes body with a commit listener registered for the duration."
  [{:keys [conn listener-fn]} & body]
  `(let [deregister# (add-commit-listener! ~conn ~listener-fn)]
     (try
       ~@body
       (finally
         (deregister#)))))

;; Rollback listeners

(defn ^:private ->rollback-listener
  [f]
  (reify SQLiteCommitListener
    (onCommit [_])
    (onRollback [_] (f))))

(defn add-rollback-listener!
  "Registers a zero-arg function as a SQLite rollback listener.

  Returns a zero-arg function that deregisters the listener. The underlying Java
  listener object is available as :listener metadata on the returned function."
  [^Connection conn f]
  (let [sqlite-conn (->sqlite-conn conn)
        listener (cached-listener conn f ::rollback ->rollback-listener)]
    (.addCommitListener sqlite-conn listener)
    (with-meta
      (fn [] (.removeCommitListener sqlite-conn listener))
      {:listener listener})))

(defn remove-rollback-listener!
  "Removes a previously registered rollback listener."
  [^Connection conn f]
  (when-let [listener (uncache-listener conn f ::rollback)]
    (.removeCommitListener (->sqlite-conn conn) listener)))

(defmacro with-rollback-listener
  "Executes body with a rollback listener registered for the duration."
  [{:keys [conn listener-fn]} & body]
  `(let [deregister# (add-rollback-listener! ~conn ~listener-fn)]
     (try
       ~@body
       (finally
         (deregister#)))))
