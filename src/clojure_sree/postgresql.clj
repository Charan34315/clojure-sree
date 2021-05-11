;;connecting to a postgreSQL database using Clojure.

(ns clojure-sree.postgresql
  (:require [clojure.java.jdbc :as j]
            [clojure.pprint :as printp]
            [doric.core :as doric]))

(def db
  "define the (map of) database connection."
  {:dbtype "postgresql"
   :dbname "public_lan"
   :host "localhost"
   :classname "com.postgres.jdbc.Driver"
   :subprotocol "postgres"
   :user "public_user"
   :password "Sreecharan"})

(def state-sql
  "Query to create the `state` table. Creates a table consisting of
   information about various Indian states."
  (j/create-table-ddl :state [[:state_id :serial "PRIMARY KEY"]
                              [:state_name "VARCHAR(32) UNIQUE"]
                              [:population_2019 "INTEGER"]
                              [:abrv "VARCHAR(2)"]]))

;`psql` query to drop the respective tables.
(j/db-do-commands db ["drop table if exists state cascade"
                      "drop table if exists state_details"
                      "drop table if exists state_culture"])

;execute the query to create table
(j/execute! db [state-sql])

;insert the data records into `state` table.
; ----------------------------------------------------------------------------------------
(j/insert! db :state {:state_name "Andhra Pradesh" :abrv "AP" :population_2019 53903393})

(j/insert-multi! db :state [{:state_name "Karnataka" :abrv "KA" :population_2019 67562686}
                            {:state_name "Tamil Nadu" :abrv "TN" :population_2019 77841267}
                            {:state_name "Rajasthan" :abrv "ZZ" :population_2019 81032689}])

(j/update! db :state {:abrv "RJ"} ["abrv = ?" "ZZ"])

(j/db-do-prepared db ["INSERT INTO state (state_name,abrv,population_2019) values (? ,?, ?)"
                      ["Kerala" "KL" 35699443] ["Maharashtra" "MH" 123144223]] {:multi? true})

(j/query db ["SELECT * FROM state WHERE state_name=?" "Rajasthan"])

(def state-record-data
  [[237882725 "Uttar Pradesh" "UP"] [85358965 "Madhya Pradesh" "MP"]
   [13606320 "Jammu Kashmir" "JK"] [39362732 "Telangana" "TG"]
   [30141373  "Punjab" "PJ"] [29436231 "Chhattisgarh"  "CG"]
   [28204692  "Haryana" "HR"] [11250858 "Uttarakhand" "UK"]
   [4169794 "Tripura" "TR"] [1570458 "Arunachal Pradesh" "AN"]
   [690251 "Sikkim" "SK"]])

(dorun (map #(j/insert! db :state {:state_name (nth %1 1)
                                   :population_2019 (first %1)
                                   :abrv (nth %1 2)}) state-record-data))

(j/insert-multi! db :state [{:state_name "Bihar"  :abrv "BH" :population_2019  124799926}
                            {:state_name "Assam" :abrv "AS" :population_2019 35607039}
                            {:state_name "Jharkhand" :abrv "JH" :population_2019  38593948}
                            {:state_name "West Bengal"  :abrv "WB" :population_2019 91276115}
                            {:state_name "Gujarat" :abrv "GJ" :population_2019 60439692}
                            {:state_name "Odisha" :abrv "OD" :population_2019  46356334}])

;----------------------------------------------------------------------------------------
;query to get all the records from `state` table.
(j/query db ["SELECT * FROM state"] {:result-set-fn count})

(defn id-state
  "Get the data based on 'abrv' or 'state_name' fields."
  [n]
  (if (= 2 (count n))
    (j/query db ["SELECT state_id FROM state where abrv = ?" n]
             {:row-fn :state_id :result-set-fn first})
    (j/query db ["SELECT state_id FROM state where state_name = ?" n]
             {:row-fn :state_id :result-set-fn first})))

(id-state "AP")

(id-state "Sikkim")

;query to create `state_details` table.
(j/execute! db [(j/create-table-ddl :state_details [[:capital_city_id :serial "PRIMARY KEY"]
                                                    [:state_id :int "REFERENCES state ON UPDATE CASCADE ON DELETE CASCADE"]
                                                    [:capital "VARCHAR(20) UNIQUE"]
                                                    [:area_sq_km "INTEGER"]
                                                    [:NSDP_percapita_2018_19_rs "INTEGER"]])])

;insert various data records into `state_details` table.
;-----------------------------------------------------------------------------------------------------
(def query-data [["Vizaq" 162972 151173 1] ["Bengaluru" 191791 212477 2] ["Chennai" 130058 193964 3]])

(defn load-data-details-state!
  "Load the table (`state_details`) with data records."
  [vecc]
  (j/insert-multi! db :state_details (map #(hash-map :state_id (get %1 3)
                                                     :capital (first %1)
                                                     :area_sq_km (second %1)
                                                     :NSDP_percapita_2018_19_rs (get %1 2))
                                          vecc)))

(load-data-details-state! query-data)

(j/insert! db :state_details {:state_id 4 :capital "Jaipur" :area_sq_km 342248 :NSDP_percapita_2018_19_rs 110606})

(j/insert-multi! db :state_details [{:state_id 18 :capital "Patna" :area_sq_km  94163 :NSDP_percapita_2018_19_rs 40982}
                                    {:state_id 6 :capital "Mumbai" :area_sq_km 307713 :NSDP_percapita_2018_19_rs 191736}
                                    {:state_id 5 :capital "Trivandrum" :area_sq_km 38863 :NSDP_percapita_2018_19_rs  204105}])

(j/insert! db :state_details {:state_id 19 :capital "Dispur" :area_sq_km 78438 :NSDP_percapita_2018_19_rs 82837})

(j/insert! db :state_details {:state_id 8 :capital "Bhopal" :area_sq_km 308252 :NSDP_percapita_2018_19_rs 90165})

(def query_data_2 [["Lucknow" 240928 66512 7] ["Srinagar" 55538 92347 9] ["Hyderabad" 112077 204488 10]
                   ["Raipur" 135194 92413 12] ["Dehradun" 55483  198738 14]])

(load-data-details-state! query_data_2)
;------------------------------------------------------------------------------------------

;query to get information of `state_details` table.
(j/query db ["SELECT state_id from state_details"] {:result-set-fn count})

(j/query db ["SELECT constraint_name FROM information_schema.table_constraints WHERE table_name=?" "state_details"])

;----------------------------------------------------------------------------------------------------------------------------
;query to create `state_culture` table.
(def cult-sql (j/create-table-ddl :state_culture [[:demo_id :serial "PRIMARY KEY"] [:language "VARCHAR(20)"]
                                                  [:state_animal "VARCHAR(20)"] [:state_bird "VARCHAR(20)"]
                                                  [:state_id :int "REFERENCES state UNIQUE"]]))

(j/execute! db cult-sql)

;insert data records into `state_culture` table.
(j/insert-multi! db :state_culture [{:language "Kannada" :state_animal "Elephant" :state_bird "Indian Roller" :state_id 2}
                                    {:language "Telugu" :state_animal "Blackbuck" :state_bird "Rose ringed Parakeet" :state_id 1}
                                    {:language "Tamil" :state_animal "Nilgiri tahr" :state_bird "Emerald dove" :state_id 3}
                                    {:language "Malayalam" :state_animal "Elephant" :state_bird "Great Hornbill" :state_id 5}
                                    {:language "Hindi" :state_animal "Swamp deer" :state_bird "Paradise flycatcher" :state_id 8}
                                    {:language "Hindi" :state_animal "Camel" :state_bird "Great Indian bustard" :state_id 4}
                                    {:language "Marathi" :state_animal "Giant Squirrel" :state_bird "Green Pigeon" :state_id 6}
                                    {:language "Odia" :state_animal  "Sambar" :state_bird "Indian Roller" :state_id 23}])

(j/insert! db :state_culture {:language "Assamese" :state_animal "1-horned rhinoceros" :state_bird "White wood duck" :state_id 19})

(def culture-data (j/query db ["SELECT state_bird FROM state_culture"]))
;-------------------------------------------------------------------------------------------------------------

(printp/pprint (map (comp clojure.string/upper-case #(first (vals %))) culture-data))

(printp/pprint (j/get-by-id db :state_culture 3 :demo_id))

(printp/pprint (j/get-by-id db :state 12 :state_id))

(def raw-query "SELECT a.Nsdp_percapita_2018_19_rs,b.population_2019 from state_details a inner join state b
                using (state_id) where a.State_id=?")

(defn popul-gdp-id
  "Get the details based on state id."
  [id]
  (when-let [[x] (j/query db [raw-query id])]
    x))

(popul-gdp-id 2)

(printp/pprint (map #(:state_bird %) (j/query db ["SELECT a.abrv,b.state_bird,c.capital FROM state a inner join
 state_culture b USING (state_id) inner join state_details c USING (state_id)"])))

(print (doric/table (j/query db ["SELECT s.nsdp_percapita_2018_19_rs,a.state_name FROM state_details s inner join state a
using (state_id) order by s.nsdp_percapita_2018_19_rs desc limit 5"])))

(printp/pprint (j/query db ["EXPLAIN SELECT * FROM state WHERE state_name in ('Tamil Nadu', 'Andhra Pradesh','Rajasthan') "]))

(println (doric/table (j/query db ["SELECT s.nsdp_percapita_2018_19_rs,a.state_name,b.language FROM state_details s
inner join state a using (state_id) inner join state_culture b using (state_id) order by s.nsdp_percapita_2018_19_rs desc"])))

(println (doric/table (j/query db ["SELECT x.Area_sq_km,y.state_name FROM state_details x inner join state y
 using (state_id) order by x.Area_sq_km desc"])))

(printp/print-table (map #(select-keys % [:abrv :population_2019])
                         (j/query db ["SELECT * FROM state order by population_2019 desc limit 5 offset 2"])))

(def state-area (j/query db ["SELECT a.area_sq_km,b.state_name FROM state_details a
inner join state b using (state_id) order by a.area_sq_km desc"]))

(doseq [state-pop (map #(str "Area of " (:state_name %1) " is " (:area_sq_km %1) " sq. km.") state-area)]
  (println state-pop))

(println (doric/table (j/query db ["SELECT * FROM state"])))

(println (doric/table (j/query db ["SELECT * FROM state_details"])))

(println (doric/table (j/query db ["SELECT * FROM state_culture"])))

(println (doric/table (for [details_state_full (j/query db ["SELECT a.*, b.*, c.* from state a inner join state_details b
  using (state_id) inner join state_culture c using (state_id)"])]
                        details_state_full)))