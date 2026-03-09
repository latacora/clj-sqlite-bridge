# clj-sqlite-bridge

A small, focused library for embedding Clojure functions into SQLite via user-defined
functions (UDFs). SQLite intentionally omits features like regular expressions and
Unicode case folding, but they're trivial to provide when you already have the JVM and
Clojure. This library bundles those common extras and makes it easy to add your own.

## Usage

Each module follows the same pattern: a pure Clojure function you can call directly, an
`add-*!` function to permanently register the UDF on a connection, and a `with-*` macro
that registers the UDF for the duration of a body and cleans up afterward.

### Custom UDFs (`com.latacora.sqlite.bridge`)

The bridge namespace lets you turn any Clojure function into a SQLite UDF. It handles
argument marshalling (integers, doubles, strings, blobs, and nulls) and coerces return
values—including booleans, which become `1`/`0`.

```clojure
(require '[com.latacora.sqlite.bridge :as bridge])
(import (java.sql DriverManager))

(with-open [conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
  (bridge/with-func {:conn conn
                     :func-name "double_it"
                     :func (fn [x] (* 2 (long x)))}
    ;; SELECT double_it(21);  => 42
    ;; SELECT double_it(-1);  => -2
    ))
```

For long-lived connections, use `add-func!` and `remove-func!` directly instead of
`with-func`.

### Aggregate UDFs

You can also register aggregate functions that accumulate state across rows (like
`SUM` or `GROUP_CONCAT`). An aggregate spec is a map with three keys:

- `:init` — zero-arg function returning the initial accumulator for each group
- `:step` — `(fn [acc & args])` called for each row, returns updated accumulator
- `:final` — `(fn [acc])` called after all rows, returns the aggregate result

```clojure
(bridge/with-aggregate {:conn conn
                        :func-name "clj_sum"
                        :agg-spec {:init (constantly 0)
                                   :step (fn [acc x] (+ acc (long x)))
                                   :final identity}}
  ;; SELECT clj_sum(amount) FROM sales;
  ;; SELECT region, clj_sum(amount) FROM sales GROUP BY region;
  )
```

For long-lived connections, use `add-aggregate!` to register and `remove-func!` to
unregister. SQLite stores scalar and aggregate functions in the same namespace, so
removal is always by name via `remove-func!`.

### Window Functions

Window functions extend aggregates with support for `OVER` clauses. A window spec
adds two keys to the aggregate spec:

- `:inverse` — `(fn [acc & args])` called when a row leaves the window frame (undo a step)
- `:value` — `(fn [acc])` called to get the current value mid-window (like `:final` but without finishing)

```clojure
(bridge/with-window {:conn conn
                     :func-name "clj_rsum"
                     :win-spec {:init    (constantly 0)
                                :step    (fn [acc x] (+ acc (long x)))
                                :final   identity
                                :inverse (fn [acc x] (- acc (long x)))
                                :value   identity}}
  ;; SELECT v, clj_rsum(v) OVER (ORDER BY v) FROM nums;
  ;; SELECT v, clj_rsum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM nums;
  )
```

For long-lived connections, use `add-window!` and `remove-func!`.

### Custom Collations (`com.latacora.sqlite.collation`)

Collations define how strings are compared for `ORDER BY`, `=`, `DISTINCT`, and
`GROUP BY`. Unlike UDFs which must be called explicitly, a collation registered on a
column permeates all operations automatically.

```clojure
(require '[com.latacora.sqlite.collation :as collation])

(collation/with-collation {:conn conn
                           :collation-name "reverse"
                           :comparator (fn [a b] (compare b a))}
  ;; SELECT name FROM t ORDER BY name COLLATE reverse;
  )
```

For long-lived connections, use `add-collation!` and `remove-collation!`.

The `strings` namespace ships a ready-made `unicode_ci` collation that combines
casefolding and NFC normalization:

```clojure
(strings/with-unicode-ci {:conn conn}
  ;; Case-insensitive, NFC-normalized comparisons:
  ;; SELECT DISTINCT name FROM t;  -- 'Ångström' = 'ångström' = 'ÅNGSTRÖM'
  ;; SELECT name FROM t ORDER BY name COLLATE unicode_ci;
  )
```

### Regexp (`com.latacora.sqlite.regexp`)

SQLite supports a `REGEXP` operator but leaves the implementation undefined; this
module supplies one backed by `java.util.regex`.

```clojure
(require '[com.latacora.sqlite.regexp :as regexp])

;; Pure function:
(regexp/regexp-matches? "a.*" "abc")   ;=> true
(regexp/regexp-matches? "z.*" "abc")   ;=> false

;; As a UDF (SQLite translates `x REGEXP y` into `regexp(y, x)`):
(regexp/with-regexp {:conn conn}
  ;; SELECT 'abc' REGEXP 'a.*';        => 1
  ;; SELECT 'abc' REGEXP 'z.*';        => 0
  )
```

### Unicode strings (`com.latacora.sqlite.strings`)

SQLite stores UTF-8 correctly and handles character indexing (`LENGTH`, `SUBSTR`), but
its string operations are largely ASCII-only: `UPPER()`, `LOWER()`, and case-insensitive
`LIKE` all ignore non-ASCII characters. These UDFs provide Unicode-aware alternatives
without requiring the ICU extension.

**`casefold`** — locale-independent lowercasing (uses `Locale/ROOT`):

```clojure
(require '[com.latacora.sqlite.strings :as strings])

(strings/casefold "Ångström")          ;=> "ångström"
(strings/casefold "Straße")            ;=> "straße"
(strings/casefold nil)                 ;=> nil

(strings/with-casefold {:conn conn}
  ;; SELECT casefold('Ångström');       => "ångström"
  )
```

**`normalize-nfc`** — Unicode NFC normalization, useful for ensuring consistent
string comparisons when data arrives in mixed normalization forms:

```clojure
(strings/normalize-nfc "e\u0301")      ;=> "é"  (NFD -> NFC)
(strings/normalize-nfc "é")            ;=> "é"  (already NFC)
(strings/normalize-nfc nil)            ;=> nil

(strings/with-normalize-nfc {:conn conn}
  ;; SELECT normalize_nfc('e' || char(0x0301));  => "é"
  )
```

## Development

Use babashka as the entry point for project tasks:

- `bb test` runs Kaocha tests.
- `bb lint` runs clj-kondo against `src` and `test`.
- `bb maint` checks for outdated dependencies with antq.

## License

EPL-2.0.
