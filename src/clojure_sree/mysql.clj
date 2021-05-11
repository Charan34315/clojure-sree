;; Using parameterized queries to prevent MySQL injection.
; A simple demonstration.

(ns clojure-sree.mysql
  (:require [clojure.java.jdbc :as j]))

;define the map of database connection
(def mysql-db {:dbtype "mysql"
               :dbname "hr"
               :classname "com.mysql.jdbc.Driver"
               :host "localhost"
               :subprotocol "mysql"
               :user "root"
               :password "Sreecharan1234"})

(defn call-proc
  [query]
  (j/query mysql-db [query]))

(defn call-prepared-query
  [query]
  (j/db-do-prepared mysql-db [query]))

(defn quer-proc-list
  [database]
  (str "SELECT specific_name FROM `information_schema`.`ROUTINES` WHERE  routine_schema='"
       database "';"))

;Query to list all the procedures
(mapcat vals (call-proc (quer-proc-list "hr")))

(defn proc-query
  [job-id]
  (#(str "CALL new_procedure('" % "')") job-id))

((comp call-proc proc-query) "IT_PROG")

(call-prepared-query "DROP PROCEDURE IF EXISTS new_rocedure;")

(def create-proc-query "CREATE PROCEDURE `new_rocedure`(IN last_nam VARCHAR(64)) BEGIN SELECT * FROM employees where last_name=last_nam; END;")

(call-prepared-query create-proc-query)

(defn proc-query-second
  [LAST-NAME]
  (#(str "CALL new_rocedure('" % "')") LAST-NAME))

((comp call-proc proc-query-second) "Austin")

(j/query mysql-db ["SELECT * FROM employees WHERE  job_id=?" "IT_PROG"])