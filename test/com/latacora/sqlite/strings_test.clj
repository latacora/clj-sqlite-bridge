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
