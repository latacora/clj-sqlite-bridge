(ns com.latacora.sqlite.bridge-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [com.latacora.sqlite.bridge :as bridge])
  (:import
   (java.sql DriverManager Connection PreparedStatement ResultSet SQLException)))

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

(deftest with-func-adds-and-removes
  (with-conn
   (fn [conn]
     (bridge/with-func {:conn conn :func-name "echo" :func identity}
       (is (= "hi" (query-single conn "SELECT echo(?);" #(.setString % 1 "hi")))))
     (is (thrown? SQLException
                  (query-single conn "SELECT echo(?);" #(.setString % 1 "hi")))))))

(deftest udf-errors-surface
  (with-conn
   (fn [conn]
     (bridge/add-func! conn "boom" (fn [_] (throw (ex-info "boom" {}))))
     (is (thrown? SQLException
                  (query-single conn "SELECT boom(?);" #(.setLong % 1 1)))))))

(deftest boolean-return-coerces
  (with-conn
   (fn [conn]
     (bridge/with-func {:conn conn :func-name "is_even" :func (fn [n] (even? (long n)))}
       (is (= 1 (query-single conn "SELECT is_even(?);" #(.setLong % 1 4))))
       (is (= 0 (query-single conn "SELECT is_even(?);" #(.setLong % 1 5))))))))

(defspec echo-integers 100
  (prop/for-all [n gen/large-integer]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (= (long n)
            (query-single conn "SELECT echo(?);" #(.setLong % 1 (long n)))))))))

(defspec echo-doubles 100
  (prop/for-all [n (gen/double* {:NaN? false :infinite? false})]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (let [result (query-single conn "SELECT echo(?);" #(.setDouble % 1 n))]
           (<= (Math/abs (- (double result) n)) 1.0E-9)))))))

(defspec echo-strings 100
  (prop/for-all [s gen/string-alphanumeric]
    (with-conn
     (fn [conn]
       (bridge/with-func {:conn conn :func-name "echo" :func identity}
         (= s (query-single conn "SELECT echo(?);" #(.setString % 1 s))))))))
