(ns clojure-sree.sparql
  (:require [clojure.string :as s]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [clojure.pprint :as pretty]
            [clostache.parser :as p]
            [clojure.test :as t]))

(defn query-sparql
  ([endpoint query]
   (query-sparql endpoint query {}))
  ([endpoint query http-req]
   {:pre [(re-find #"http" endpoint)
          (re-find #"(?i)ASK|SELECT|CONSTRUCT|DESCRIBE" query)]}
   (let [response (http/get endpoint (merge (merge {:cookie-policy :standard} http-req)
                                            {:query-params {:query query}}))]
     (if (<= 200 (:status response) 299)
       (:body response)
       (throw
         (Error.
           (str "HTTP Error "
                (:status response)
                (:reason-phrase response)
                (:body response))))))))

(defn select-sparql
  "Function to attach the query to a given endpoint."
  ([endpoint query]
   (select-sparql endpoint query {}))
  ([endpoint query http-req]
   {:pre [(re-find #"http"  endpoint)
          (string? query)
          (re-find #"(?i)SELECT" query)]}
   (-> (query-sparql endpoint query (merge http-req {:accept "application/sparql-results+json"}))
       (json/read-str)
       (get "results")
       (get "bindings"))))

(def endpoint "http://localhost:3030/dataolymp/sparql")

(defn convert-to-decimal
  [token]
  (try
    (if (re-matches #"^[\d+\.]+$" token)
      (->> token
           (Double/parseDouble)
           (format "%.2f"))
      token)
    (catch Exception _)))

(defn mapfn
  "Get the result as a map from a response to the query request."
  [keysfn map-val]
  (letfn [(gt-key [map-param-key] (->> map-param-key
                                       (conj '("value"))
                                       (get-in map-val)
                                       convert-to-decimal))]
    (zipmap keysfn (map gt-key keysfn))))

(defn getnrecords
  "Get `n` records from the query."
  [n result]
  (let [keyfn ((comp keys first) result)
        mapfn (partial mapfn keyfn)]
    (->> result
         (map mapfn)
         (take n))))

; Illustrations:
; Firstly, load the `olympics` RDF triple store to the local jena fuseki server.

;----------------------------------------------------------------------------------
;Query to list each country alongside the average height and weight of its athletes

(def height-query
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   PREFIX dbo: <http://dbpedia.org/ontology/>
   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   SELECT ?countryCode
  (AVG(?height) As ?avgHeight)
  (AVG(?weight) As ?avgWeight)
  WHERE {
  ?noc dbo:ground ?team ;
       rdfs:label ?countryCode .
  ?athlete rdf:type   foaf:Person ;
            dbo:team   ?team ;
            dbo:height ?height ;
            dbo:weight ?weight .}
  GROUP BY ?countryCode
  ORDER BY DESC(?avgHeight)")

(def result-height (select-sparql endpoint height-query))

(pretty/print-table (getnrecords 4 result-height))

;----------------------------------------------------------------------------------------------
;Query to list an year alongside the oldest Judo competitor in that year.

(def judo-oldest-query
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   PREFIX dbp: <http://dbpedia.org/property/>
   SELECT ?year (MAX(?age) As ?maxAge)
   WHERE {
       ?instance walls:games   ?games ;
                 walls:event   ?event ;
                 walls:athlete ?athlete .
       ?event rdfs:subClassOf <http://wallscope.co.uk/resource/olympics/sport/Judo> .
                 ?games dbp:year ?year .
                 ?athlete foaf:age ?age .}
   GROUP BY ?year
   ORDER BY ?year")

(def judo-result (select-sparql endpoint judo-oldest-query))

(pretty/print-table (getnrecords 4 judo-result))

;---------------------------------------------------------------------------------
; Query to list names of every athlete with at least one medal, alongside their total number of medals

(def query
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   SELECT ?name (COUNT(?athlete) As ?noOfMedals)
   WHERE {
   ?instance walls:athlete ?athlete ;
             walls:medal   ?medal .
   ?athlete  rdfs:label    ?name .}
   GROUP BY ?name
   ORDER BY DESC(?noOfMedals)")

(def result-medal (select-sparql endpoint query))

;; return details of only top 3 players from the lazy sequence.

(pretty/print-table (getnrecords 3 result-medal))

; ----------------------------------------------------------------------------------------------------------------

;; For higher expressiveness in generating SPARQL, use a templating language `clostache`.

(def stored-procedure-limit
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  SELECT ?name (COUNT(?athlete) As ?noOfMedals)
   WHERE {
   ?instance walls:athlete ?athlete ;
            walls:medal   ?medal .
   ?athlete  rdfs:label    ?name .}
  GROUP BY ?name
  ORDER BY DESC(?noOfMedals) LIMIT {{limit}} OFFSET {{offset}}" )

(defn render-stored-mitigate-numeric 
  "A stored procedure to find out the player name and his/her total gold medals.
  Render the stored procedure iff the entered value is a positive integer."
  [limit offset]
  (if (and (int? offset) (pos-int? limit) (>= offset 0))
    (let [rndrd-qry (p/render stored-procedure-limit {:limit limit :offset offset})
          rndrd-rslt (select-sparql endpoint rndrd-qry)
          k-rslt ((comp keys first) rndrd-rslt)
          mapfn-s (partial mapfn k-rslt)]
      (mapv mapfn-s rndrd-rslt))
    "Enter a positive integer for the parameters."))

(pretty/print-table (render-stored-mitigate-numeric 2 4))

(def query-player-list
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  SELECT DISTINCT ?string
  WHERE {
  ?instance walls:athlete ?athlete ;
            walls:medal   ?medal .
  ?athlete  rdfs:label    ?name .
   BIND( STR(?name) AS ?string )}" )

(def player-list
  "Query to get the total player list."
  (map #(get-in % ["string" "value"]) (select-sparql endpoint query-player-list)))

(def stored-procedure-name
  "PREFIX walls: <http://wallscope.co.uk/ontology/olympics/>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  SELECT ?string (COUNT(?athlete) As ?noOfMedals)
  WHERE
  {?instance walls:athlete ?athlete ;
            walls:medal   ?medal .
  ?athlete  rdfs:label    ?name .
   BIND (STR(?name) AS ?string ).}
   GROUP BY ?string
   HAVING (?string = {{ name }} )" )

(defn execute-stored-procedure
  "Execute the stored procedure to fetch the total number of Gold Medal.
  Given the player name, how many medals he/she has won."
  [query name]
  (some->>
    {:name (str "'" name "'")}
    (p/render query)
    (select-sparql endpoint)
    (map #(get-in % ["noOfMedals" "value"]))
    first
    (re-find #"\d+")
    (Integer/parseInt)))

(defn mitigating-special-character
"Stored procedure with mitigating injection attacks.
 A generic function to deal with the issue."
    [query-m parameter]
  (let [patt (re-pattern "[|!#$&*%*{^}]")
        char-spec (re-seq patt parameter)
        bool-val (empty? char-spec)
        count-char (count char-spec)]
    (if-not bool-val
      {:exception "The parameter should not contain special characters."
       :security "injection-attack-mitigation:special-char"
       :fails char-spec
       :count count-char}
      (if (some #(= parameter %) player-list)
        (execute-stored-procedure query-m parameter)
        "Player name not found."))))

;return the total gold medlas of a random player from the player-list,
; after passing through mitigation check.
(mitigating-special-character stored-procedure-name (rand-nth player-list))

(def not-allowed-words
  "A set of all the words that are part of SPARQL code but should not be there in the parameter values."
  #{"SELECT" "DELETE" "WHERE" "INSERT" "GROUP" "HAVING" "ORDER" "FROM" "OPTIONAL" "FILTER"})

(defn mitigate-keyword
  [query parameter]
  (let [tru-val (some not-allowed-words (s/split (s/upper-case parameter) #" "))]
    (if-not (empty? tru-val)
      {:exception "SPARQL script should not be part of variable values."
       :security "injection-attack-mitigation:sparql-code"}
      (if (some #(= parameter %) player-list)
        (execute-stored-procedure query parameter)
        "Player name not found."))))

;return the total gold medlas of a random player from the player-list,
;after passing through mitigation check.
(mitigate-keyword stored-procedure-name (rand-nth player-list))

;; unit tests
(t/deftest testing-mitigate-keyword-stored-procedure

  (t/testing "with SELECT"
    (t/is (= "SPARQL script should not be part of variable values."
             (let [query-body (mitigate-keyword stored-procedure-name "Nikolay Yefim ovich Andrianov SELECT * FROM")]
               (:exception query-body)))))

  (t/testing "with special characters"
    (t/is (= 2 (let [query-body (mitigating-special-character stored-procedure-name "Rudolph Ludewyk *& Lewis" )]
                 (:count query-body)))))
  )

(t/run-tests)
