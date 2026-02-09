(ns com.latacora.sqlite.regexp-test
  (:require
   [clojure.test :refer [deftest is]]
   [com.latacora.sqlite.regexp :as regexp])
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

(deftest regexp-function
  (with-conn
   (fn [conn]
     (regexp/with-regexp {:conn conn}
       (is (= 1 (query-single conn "SELECT 'abc' REGEXP ?;" #(.setString % 1 "a.*"))))
       (is (= 0 (query-single conn "SELECT 'abc' REGEXP ?;" #(.setString % 1 "z.*"))))))))
