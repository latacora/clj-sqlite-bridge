# clj-sqlite-bridge

A small, focused library for embedding Clojure functions into SQLite via user-defined
functions (UDFs). It is intended to be used by Latacora libraries (for example
sqlite-cache) that need to expose Clojure functionality inside SQLite queries.

## Usage

Add the dependency and require the bridge namespace:

```clojure
(ns my.app
  (:require [com.latacora.sqlite.bridge :as bridge]))
```

Create a function and register it with a connection:

```clojure
(import (java.sql DriverManager))

(let [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
  (bridge/with-func {:conn conn
                     :func-name "double_it"
                     :func (fn [x] (* 2 (long x)))}
    ;; SQLite can call the function
    ;; SELECT double_it(5);
    ))
```

Add a REGEXP helper:

```clojure
(require '[com.latacora.sqlite.regexp :as regexp])

(regexp/with-regexp {:conn conn}
  ;; SELECT 'abc' REGEXP 'a.*';
  )
```

## API

- `com.latacora.sqlite.bridge/->Function` converts a Clojure function into a SQLite `Function`.
- `com.latacora.sqlite.bridge/add-func!` registers a Clojure function as a SQLite UDF.
- `com.latacora.sqlite.bridge/remove-func!` removes a previously registered SQLite UDF.
- `com.latacora.sqlite.bridge/with-func` temporarily registers a UDF for the duration of a body.
- `com.latacora.sqlite.regexp/regexp-matches?` implements a SQLite-compatible regexp predicate.
- `com.latacora.sqlite.regexp/add-regexp!` registers the `regexp` UDF.
- `com.latacora.sqlite.regexp/with-regexp` registers the `regexp` UDF for a body.
- `com.latacora.sqlite.strings/casefold` locale-independent lowercasing.
- `com.latacora.sqlite.strings/normalize-nfc` Unicode NFC normalization.
- `com.latacora.sqlite.strings/with-casefold` registers the `casefold` UDF.
- `com.latacora.sqlite.strings/with-normalize-nfc` registers the `normalize_nfc` UDF.

## Development

Use babashka as the entry point for project tasks:

- `bb test` runs Kaocha tests.
- `bb lint` runs clj-kondo against `src` and `test`.
- `bb maint` checks for outdated dependencies with antq.

## License

EPL-2.0.
