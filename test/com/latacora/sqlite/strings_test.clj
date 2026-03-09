(ns com.latacora.sqlite.strings-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [com.latacora.sqlite.strings :as strings])
  (:import
   (java.sql DriverManager Connection PreparedStatement ResultSet)))

(defn ^:private with-conn
  [f]
  (Class/forName "org.sqlite.JDBC")
  (with-open [^Connection conn (DriverManager/getConnection "jdbc:sqlite::memory:")]
    (f conn)))

(defn ^:private query-single
  [^Connection conn sql param-setter]
  (with-open [^PreparedStatement stmt (.prepareStatement conn sql)]
    (param-setter stmt)
    (with-open [^ResultSet rs (.executeQuery stmt)]
      (when (.next rs)
        (.getObject rs 1)))))

(defn ^:private exec!
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn ^:private query-all
  [^Connection conn sql]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt sql)]
    (let [cols (.getColumnCount (.getMetaData rs))]
      (loop [rows []]
        (if (.next rs)
          (recur (conj rows (mapv #(.getObject rs (inc %)) (range cols))))
          rows)))))

(deftest casefold-behavior
  (testing "pure function"
    (is (= "ångström" (strings/casefold "Ångström")))
    (is (= "straße" (strings/casefold "Straße")))
    (is (nil? (strings/casefold nil))))
  (testing "sqlite udf"
    (with-conn
     (fn [conn]
       (strings/with-casefold {:conn conn}
         (is (= "ångström"
                (query-single conn "SELECT casefold(?);" #(.setString % 1 "Ångström"))))
         (is (= "straße"
                (query-single conn "SELECT casefold(?);" #(.setString % 1 "Straße")))))))))

(deftest normalize-nfc-behavior
  (let [nfd "e\u0301"
        nfc "é"]
    (testing "pure function"
      (is (= nfc (strings/normalize-nfc nfd)))
      (is (= nfc (strings/normalize-nfc nfc)))
      (is (nil? (strings/normalize-nfc nil))))
    (testing "sqlite udf"
      (with-conn
       (fn [conn]
         (strings/with-normalize-nfc {:conn conn}
           (is (= nfc
                  (query-single conn "SELECT normalize_nfc(?);" #(.setString % 1 nfd))))
           (is (= nfc
                  (query-single conn "SELECT normalize_nfc(?);" #(.setString % 1 nfc))))))))))

;; unicode_ci collation tests

(deftest unicode-ci-compare-behavior
  (testing "pure function"
    (is (zero? (strings/unicode-ci-compare "Ångström" "ångström")))
    (is (zero? (strings/unicode-ci-compare "Straße" "straße")))
    (is (zero? (strings/unicode-ci-compare "e\u0301" "é")))
    (is (neg? (strings/unicode-ci-compare "a" "b")))
    (is (pos? (strings/unicode-ci-compare "b" "a")))))

(deftest unicode-ci-order-by
  (testing "ORDER BY with unicode_ci collation"
    (with-conn
     (fn [conn]
       (strings/with-unicode-ci {:conn conn}
         (exec! conn "CREATE TABLE t (name TEXT)")
         (exec! conn "INSERT INTO t VALUES ('Ångström'), ('apple'), ('Banana')")
         (let [rows (query-all conn "SELECT name FROM t ORDER BY name COLLATE unicode_ci")]
           ;; After casefold+NFC: "apple" < "banana" < "ångström" (å = U+00E5, after z)
           (is (= [["apple"] ["Banana"] ["Ångström"]] rows))))))))

(deftest unicode-ci-equality
  (testing "case-insensitive equality via collation"
    (with-conn
     (fn [conn]
       (strings/with-unicode-ci {:conn conn}
         (exec! conn "CREATE TABLE t (name TEXT COLLATE unicode_ci)")
         (exec! conn "INSERT INTO t VALUES ('Ångström'), ('ångström'), ('ÅNGSTRÖM')")
         (let [rows (query-all conn "SELECT DISTINCT name FROM t")]
           (is (= 1 (count rows)))))))))

(deftest unicode-ci-nfc-normalization
  (testing "NFC-equivalent strings are equal under unicode_ci"
    (with-conn
     (fn [conn]
       (strings/with-unicode-ci {:conn conn}
         (exec! conn "CREATE TABLE t (name TEXT COLLATE unicode_ci)")
         ;; e + combining acute (NFD) vs precomposed é (NFC)
         (exec! conn "INSERT INTO t VALUES ('e' || char(769)), ('é')")
         (let [rows (query-all conn "SELECT DISTINCT name FROM t")]
           (is (= 1 (count rows)))))))))
