(ns com.latacora.sqlite.regexp
  "Regexp helpers for SQLite user-defined functions."
  (:require
   [com.latacora.sqlite.bridge :as bridge]))

(defn regexp-matches?
  "Helper function that implements a regexp pattern matcher for SQLite."
  [re-str s]
  (some? (re-matches (re-pattern re-str) s)))

(defn add-regexp!
  "Adds a regexp function to SQLite that can be used with the REGEXP operator."
  [conn]
  (bridge/add-func! conn "regexp" regexp-matches?))

(defmacro with-regexp
  "Defines the `regexp` user defined function.

  This enables the `REGEXP` operator: SQLite translates `x REGEXP y` to
  `regexp(y, x)`."
  [{:keys [conn]} & body]
  `(bridge/with-func {:conn ~conn :func-name "regexp" :func regexp-matches?}
     ~@body))
