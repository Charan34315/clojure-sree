;; parse a given SPARQL query, resulting in a hash-map.
;; information to be utilized in later analysis and extraction.

(ns clojure-sree.parse-sparql
  (:require [clojure.string :as s]
            [clojure.pprint :as pretty]))

(def demo-query
  "PREFIX foaf:  <http://xmlns.com/foaf/0.1/> .
  SELECT ?first_name ?last_name
  FROM nutty_graph
  WHERE {
  subject:Fred foaf:lastName ?last_name ;
  foaf:firstName ?first_name .}
  ORDER BY ?last_name")

(defn nds-wth-if
  "Check if the token ends with a full-stop, and remove it if found."
  [token]
  (if (s/ends-with? token ".")
    (apply str (drop-last token))
    token)) 

(defn smpl-sntnc
  "Parse the original query to a simple sentence."
  [query]
  (->> query
       s/split-lines
       (map s/trim)
       (s/join #" ")  
       nds-wth-if))

(defn form
  "Get the basic SPARQL form of the query."
  [query]
  (when-let [form (->> query
                       s/upper-case
                       (re-seq #"\w+")
                       (some #{"CONSTRUCT" "SELECT" "ASK" "DESCRIBE"}))]
    form))

(defn variables
  "Get variables of the query."
  [query]
  (letfn [(md-strng [strng] (re-find #"(?<=(?i)ASK|CONSTRUCT|SELECT|DESCRIBE).*?(?=\{)" strng))
          (rslt-fn [strng] (->> strng
                                (re-seq #"(?<=\?)\w+")
                                (into (sorted-set))))]
    (when-let [md-strngd (md-strng query)]
      (if (re-find #" \*" md-strngd)
        (rslt-fn query)
        (rslt-fn md-strngd)))))

(defn prefixes
  "Get the prefixes of the query as hash-map."
  [query]
  (let [prfx (re-seq #"(?<=(?i)prefix).*?>" query)
        pr (mapcat #(re-seq #"(?<=\s).*?(?=:\s)" %) prfx)
        lnk (mapcat #(re-seq #"[^<]+(?=>)" %) prfx)
        rslt (map #(vector (keyword (s/trim %1)) %2) pr lnk)]
    (into (sorted-map) rslt)))

(defn graphs
  "Get the source graph from the query."
  [query]
  (if-let [frms (re-seq #"(?<=(?i)FROM|FROM NAMED)\S*(?:\s\S+)?" query)]
    (->> frms
         (map s/trim)
         (remove #{"NAMED"})
         (into #{}))
    "Default Reference."))

(defn orderby
  "Get the order by argument from the query."
  [query]
  (let [lmt-rslt (re-find #"(?i)limit.*" query)
        lmt-st (into #{} (when (some? lmt-rslt) (s/split lmt-rslt #" ")))]
    (some->> query
             (re-find #"(?<=(?i)order by).*")
             (re-seq #"\b(?i)(?!asc|desc\b)\w+")
             (remove lmt-st)
             (into (sorted-set)))))

(defn lookuptriple
  "A generic function to get triple-lookup-pattern from where part of the string."
  [query]
  (letfn [(trpld-str [str-frst] (let [trpl (some->> str-frst
                                                    (re-find #"(?<=\{).*(?=\.)")
                                                    s/trim)
                                      strtr (when-not (nil? trpl) (-> #"^\S*\s+\S+"
                                                                      (re-find trpl)
                                                                      (s/split #" ")
                                                                      first))
                                      rplc (fn [re str] ((comp #(s/replace % ";" " ")
                                                               #(re-find (re-pattern re) %)) str))
                                      trpld (when-not (and (nil? strtr) (nil? trpl))
                                              (if (.contains trpl ";")
                                                (str (rplc ".*;" trpl) ". "
                                                     strtr
                                                     (rplc ";.*" trpl))
                                                trpl))]
                                  (some-> trpld
                                          (s/split #"\.\s"))))
          (whr-rslt [strng] (->> strng
                                 trpld-str
                                 (filter #(re-find #"^(?!BIND|OPTIONAL|FILTER)" %))
                                 (map s/trim)
                                 (map #(s/split % #" "))
                                 (map #(remove #{"" "."} %))))
          (mr-thn-thr-fn [lst] (if (> (count lst) 3)
                                 (let [[frst & rst] lst
                                       cnj (->> rst
                                                (partition 2)
                                                (map #(conj % frst)))] cnj)
                                 lst))
          (fll-whr-rslt [pr-rslt] (->> pr-rslt
                                       whr-rslt
                                       (map mr-thn-thr-fn)))
          (whr-trpld [trp] (zipmap [:subject :predicate :object] (mapv nds-wth-if trp)))
          (rslt-fn [whr-tr] (if (and (= 3 (count whr-tr)) (every? string? whr-tr))
                              (whr-trpld whr-tr)
                              (map whr-trpld whr-tr)))] (->> query
                                                             fll-whr-rslt
                                                             (map rslt-fn)
                                                             flatten
                                                             distinct
                                                             vec)))

(def parse-keys
  "Vector of all the to-be-parsed keys."
  [:query-type :variables :reference-graphs :prefixes :lookup-pattern :order-value])

(def vals-fn
  "Vector of all the to-be-parsed functions."
  [form variables graphs prefixes lookuptriple orderby])

(defn parsed-query
  "Return a map of parsed information about the query.
  Using REGEXP wherever needed.
  Works for SELECT/ASk form accurately."
  [query]
  (->> vals-fn
       (map #(% (smpl-sntnc query)))
       (map hash-map parse-keys)
       (filter #(every? (comp not empty?) (vals %)))
       (into {}))) 

(pretty/pprint (parsed-query demo-query))

(defn question-mark-validator
  [query]
  (let [ptt-qs (re-pattern "(?i)(?<=select) .* (?=from)|(?i)(?<=select) .* (?=where)")
        snt-slct (re-find ptt-qs query)
        ntr-slct (when-not (nil? snt-slct) (re-find #"^[^\*]+$" snt-slct))
        bl-slct (if (some? ntr-slct) (s/trim ntr-slct) snt-slct)
        lst-slct (filterv #(not= "as" %) (s/split bl-slct #" "))
        mp-slct (map-indexed #(array-map %2 (s/starts-with? %2 "?")) lst-slct)
        slct? (every? true? (mapcat vals mp-slct))
        fnl-rslt (mapcat #(when (false? (first (vals %))) (keys %)) mp-slct)]
    (when-not slct?
      {:exception "Question mark is mandatory for variables in a SPARQL query"
       :security "Lexical error encountered."
       :fails fnl-rslt
       :count (count fnl-rslt)})))

((comp question-mark-validator smpl-sntnc) demo-query)

;; sample_result:
;{:query-type "SELECT"
;  :variables #{"avgHeight" "avgWeight" "countryCode" "height" "weight"}
;  :reference-graph "Default Reference."
;  :prefixes
;  {:dbo "http://dbpedia.org/ontology/"
;   :foaf "http://xmlns.com/foaf/0.1/"
;   :rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
;   :rdfs "http://www.w3.org/2000/01/rdf-schema#"
;   :walls "http://wallscope.co.uk/ontology/olympics/"}
;  :lookup-pattern
;  [{:subject "?noc", :predicate "dbo:ground", :object "?team"}
;   {:subject "?noc", :predicate "rdfs:label", :object "?countryCode"}
;   {:subject "?athlete", :predicate "rdf:type", :object "foaf:Person"}
;   {:subject "?athlete", :predicate "dbo:team", :object "?team"}
;   {:subject "?athlete", :predicate "dbo:height", :object "?height"}
;   {:subject "?athlete", :predicate "dbo:weight", :object "?weight"}]
;  :order-value #{"avgHeight"}}
