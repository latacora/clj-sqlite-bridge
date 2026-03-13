(ns com.latacora.sqlite.serialization
  "Serialize and deserialize SQLite databases to and from byte arrays."
  (:import
   (org.sqlite SQLiteConnection)
   (java.sql Connection)))

(defn ^:private ->sqlite-conn
  ^SQLiteConnection [^Connection conn]
  (.unwrap conn SQLiteConnection))

(defn serialize
  "Serializes a SQLite database to a byte array.

  Returns the entire database as a byte[], suitable for storage, transmission,
  or later restoration via deserialize.

  db-name defaults to \"main\"."
  (^bytes [^Connection conn]
   (serialize conn "main"))
  (^bytes [^Connection conn db-name]
   (.serialize (->sqlite-conn conn) db-name)))

(defn deserialize!
  "Deserializes a byte array into a SQLite database, replacing the current
  contents of the connection's database.

  data is a byte[] previously obtained from serialize.

  db-name defaults to \"main\"."
  ([^Connection conn ^bytes data]
   (deserialize! conn "main" data))
  ([^Connection conn db-name ^bytes data]
   (.deserialize (->sqlite-conn conn) db-name data)))
