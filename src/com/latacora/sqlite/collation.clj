(ns com.latacora.sqlite.collation
  "Custom collation support for SQLite. Collations define how strings are
  compared for ORDER BY, equality, DISTINCT, and GROUP BY."
  (:import
   (org.sqlite Collation)
   (java.sql Connection)))

(defn ^:private ->Collation
  [f]
  (proxy [Collation] []
    (xCompare [s1 s2]
      (f s1 s2))))

(defn add-collation!
  "Registers a Clojure comparator as a SQLite collation.

  f is a comparator function (fn [s1 s2] -> int) following the same contract
  as java.util.Comparator: negative if s1 < s2, zero if equal, positive if
  s1 > s2."
  [^Connection conn collation-name f]
  (Collation/create conn collation-name (->Collation f)))

(defn remove-collation!
  "Removes a previously registered SQLite collation by name."
  [^Connection conn collation-name]
  (Collation/destroy conn collation-name))

(defmacro with-collation
  "Executes body with a SQLite collation temporarily registered on the connection."
  [{:keys [conn collation-name comparator]} & body]
  `(let [conn# ~conn
         name# ~collation-name]
     (try
       (add-collation! conn# name# ~comparator)
       ~@body
       (finally
         (remove-collation! conn# name#)))))
