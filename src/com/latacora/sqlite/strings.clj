(ns com.latacora.sqlite.strings
  "Unicode-aware string helpers for SQLite user-defined functions.

  Note: `casefold` uses Java's Locale/ROOT lowercasing. This is not full
  Unicode casefolding, but provides consistent, locale-independent behavior
  without adding ICU dependencies."
  (:require
   [com.latacora.sqlite.bridge :as bridge]
   [com.latacora.sqlite.collation :as collation])
  (:import
   (java.text Normalizer Normalizer$Form)
   (java.util Locale)))

(defn casefold
  "Locale-independent lowercasing for consistent comparisons." 
  [s]
  (when (some? s)
    (.toLowerCase ^String s Locale/ROOT)))

(defn normalize-nfc
  "Normalize a string to Unicode NFC." 
  [s]
  (when (some? s)
    (Normalizer/normalize s Normalizer$Form/NFC)))

(defn add-casefold!
  "Registers a `casefold` UDF for SQLite." 
  [conn]
  (bridge/add-func! conn "casefold" casefold))

(defmacro with-casefold
  "Registers the `casefold` UDF for the duration of body." 
  [{:keys [conn]} & body]
  `(bridge/with-func {:conn ~conn :func-name "casefold" :func casefold}
     ~@body))

(defn add-normalize-nfc!
  "Registers a `normalize_nfc` UDF for SQLite." 
  [conn]
  (bridge/add-func! conn "normalize_nfc" normalize-nfc))

(defmacro with-normalize-nfc
  "Registers the `normalize_nfc` UDF for the duration of body."
  [{:keys [conn]} & body]
  `(bridge/with-func {:conn ~conn :func-name "normalize_nfc" :func normalize-nfc}
     ~@body))

(defn unicode-ci-compare
  "Case-insensitive, NFC-normalized Unicode string comparator.
  Suitable for use as a SQLite collation."
  [^String s1 ^String s2]
  (let [normalize (fn [^String s] (-> s casefold normalize-nfc))]
    (.compareTo ^String (normalize s1) ^String (normalize s2))))

(defn add-unicode-ci!
  "Registers a `unicode_ci` collation for case-insensitive, NFC-normalized
  Unicode string comparison."
  [conn]
  (collation/add-collation! conn "unicode_ci" unicode-ci-compare))

(defn remove-unicode-ci!
  "Removes the `unicode_ci` collation."
  [conn]
  (collation/remove-collation! conn "unicode_ci"))

(defmacro with-unicode-ci
  "Registers the `unicode_ci` collation for the duration of body."
  [{:keys [conn]} & body]
  `(collation/with-collation {:conn ~conn
                              :collation-name "unicode_ci"
                              :comparator unicode-ci-compare}
     ~@body))
