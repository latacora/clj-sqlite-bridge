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
`with-func`. Under the hood, `->Function` wraps a Clojure function into the
`org.sqlite.Function` interface if you need the object itself.

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
