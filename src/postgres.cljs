(ns postgres
  "Calls the Modulr API to create a currency account for the card."
  (:require ["pg$default" :as pg]
            [aws-secrets]
            [camel-snake-kebab.core :as csk]
            [cljs-bean.core :as cb]
            [config]
            [data-definition]
            [promesa.core :as p]
            [clojure.set :as clj-set]))


(def Client (.-Client pg))

(def connected-client (atom nil))


(def pre-db-pg-client
  (Client. (clj->js (merge config/db-config
                           {:database "postgres"}))))


(defn lookup-db-credentials
  []
  (p/let [creds (aws-secrets/fetch-database-creds)]
    creds))


(defn connect
  []
  (p/let [creds  (lookup-db-credentials)
          client (Client. (clj->js (merge config/db-config creds)))
          _      (.connect client)]
    client))


(defn query
  [query-statement]
  (-> (p/let [client (connect)
              result (.query client query-statement)
              _      (.end client)]
        result)
      (p/catch (fn [error]
                 (println "Query Error" :query-statement query-statement)
                 (js/console.log "Error" error)))))


(defn parameterised-query
  [text values]
  (-> (p/let [client (connect)
              result (.query client text (clj->js values))
              _      (.end client)]
        result)
      (p/catch (fn [error]
                 (println "Parameterised-query Error" :text text :values values)
                 (js/console.log "Error" error)))))


(defn transact
  [text values]
  (-> (p/let [client (connect)
              _      (.query client "BEGIN")
              result (.query client text (clj->js values))
              _      (.query client "COMMIT")
              _      (.end client)]
        result)
      (p/catch (fn [error]
                 (println "Transaction Error" :text text :values values)
                 (js/console.log "Error" error)))))


(defn multi-transact
  [statements]
  (-> (p/let [client  (connect)
              _       (.query client "BEGIN")
              results (doseq [[text values] statements]
                        (.query client text (clj->js values)))
              _       (.query client "COMMIT")
              _       (.end client)]
        results)
      (p/catch (fn [error]
                 (println "Transaction Error" :statements statements)
                 (js/console.log "Error" error)))))


(defn process-rows
  [query-result processing-fn]
  (when query-result
    (p/->> (.-rows query-result)
           (map #(-> % cb/bean processing-fn)))))


;; TODO ... move these into test or delete

(def test-db-name
  (str "cardibees_test_db_" (str (rand-int (* 1024 16)))))


(defn comma-separated-text
  [coll]
  (->> coll (interpose ", ") (apply str)))


(defn sql-placeholders
  ([to-n]
   (sql-placeholders 0 to-n))
  ([from-n to-n]
   (map #(str "$" (inc %)) (range from-n to-n))))

(defn update-assignments
  [column-names]
  (-> (map (fn [col-name]
             (let [sql-col (-> col-name csk/->snake_case name)]
               (str sql-col " = EXCLUDED." sql-col)))
           column-names)
      comma-separated-text))


(defn data->sql-columns
  "Produce the SQL column names from a set of table column keywords (eg col_name) and
  a list of keywords (eg col-name)"
  [table-columns keywords]
  (let [data-properties (set (map csk/->snake_case keywords))]
    (->> data-properties
         (clj-set/intersection table-columns)
         sort
         (map name))))


(defn data->upsert-statement
  "Given the table data and some data to insert, produce an upsert statement and matching values.
  Only the matching keys in the data to the columns in the DB are considered interesting. It is assumed
  that all the data values need to be persisted.
  The data is ordered within this function to ensure that the data and columns match up
  in the SQL text and the placeholder values.
  The list of update-keys are used to produce an upsert when there is a conflict on that column or those
  columns.
  Note: This is a WEAK implementation of UPSERT. It relies on update-keys always being in the insert-data-map.
  The design should evolve as we encounter additional cases."
  [table-name table-definition insert-data-map update-keys]
  (let [insert-map         (into (sorted-map) insert-data-map)
        sql-values         (vec (vals insert-map))
        insert-cols        (keys insert-map)

        update-map         (into (sorted-map) (apply dissoc insert-data-map update-keys))
        update-cols        (keys update-map)

        table-columns      (set (map first table-definition))
        insert-sql-columns (data->sql-columns table-columns insert-cols)
        insert-cols-text   (comma-separated-text insert-sql-columns)
        insert-params      (-> (count insert-sql-columns) sql-placeholders comma-separated-text)

        update-assigns     (update-assignments update-cols)

        ;; TODO - code for multiple columns in conflict statement
        conflict-cols      (-> insert-data-map (select-keys update-keys) keys first csk/->snake_case name)

        insert-preamble    (str "INSERT INTO " (name table-name) " (")
        insert-text        (str insert-preamble insert-cols-text
                                ")\n VALUES (" insert-params ") "
                                " ON CONFLICT (" conflict-cols ") DO")

        sql-text           (str insert-text " UPDATE SET " update-assigns ";")]
    {:sql-text   sql-text
     :sql-values sql-values}))


(defn data-definition->insert-statement
  [table-name data-definition]
  (let [columns      (->> (reduce
                            (fn [definitions [column]]
                              (conj definitions (name column)))
                            [] data-definition)
                          (interpose ", \n")
                          (apply str))
        generated    (reduce
                       (fn [values [column & [options]]]
                         (let [{:keys [gen-fn]
                                :or   {gen-fn data-definition/rand-string}} options]
                           (conj values [column (gen-fn)])))
                       [] data-definition)
        values       (vec (map last generated))
        placeholders (->> (map-indexed (fn [index _item]
                                         (str "$" (inc index))) generated)
                          (interpose ", ")
                          (apply str))
        data         (->> generated
                          (map (fn [[k v]] (zipmap [k] [v])))
                          (apply merge))
        insert       (str "INSERT INTO " (name table-name) " ( \n")]
    {:insert-statement (str insert columns ")\n VALUES (" placeholders ");")
     :insert-values    values
     :data             data}))


(defn run-alter-statement
  [alter-text]
  (p/let [result (query alter-text)]
    result))


(defn data-definition->table-definition
  [table-name data-definition]
  (let [create (str "CREATE TABLE IF NOT EXISTS " (name table-name) " ( \n")]
    (str create
         (->> (reduce
                (fn [definitions [column & [options]]]
                  (let [{:keys [type primary-key]
                         :or   {type :text primary-key false}} options
                        params (cond-> (str "  " (name column) " " (name type))
                                       primary-key (str " primary key"))]
                    (conj definitions params)))
                [] data-definition)
              (interpose ", \n")
              (apply str))
         "\n);")))


(defn ensure-table-exists
  [table-definition]
  (-> (p/let [_result (postgres/query table-definition)]
        true)
      (p/catch (fn [error]
                 (js/console.log "Table creation failed. Definition:" table-definition)
                 (js/console.log "Error" error)))))


;; TODO - Move table definitions to their own structure for better organization
(def customer-account "C120J2PK")


(defn ensure-tables-exist
  []
  (p/let [customer-definition    (data-definition/pre-selected-customer-template customer-account)
          preselected-table      (data-definition->table-definition
                                   data-definition/preselected-table-name customer-definition)
          cross-reference-table  (data-definition->table-definition
                                   data-definition/cross-reference-table-name data-definition/cross-reference)
          card-history           (data-definition->table-definition
                                   data-definition/card-history-table-name data-definition/card-history)
          management-token-table (data-definition->table-definition
                                   data-definition/management-token-table-name data-definition/card-management-token)
          webhook-table          (data-definition->table-definition
                                   data-definition/customer-webhook-table-name data-definition/customer-webhook)
          statements-table       (data-definition->table-definition
                                   data-definition/statements-table-name data-definition/statements)
          billing-transactions   (data-definition->table-definition data-definition/billing-transactions-table-name
                                                                    data-definition/billing-transactions)
          tables-created?        (p/all (reduce (fn [acc table]
                                                  (conj acc (ensure-table-exists table)))
                                                [] [preselected-table
                                                    cross-reference-table
                                                    card-history
                                                    management-token-table
                                                    webhook-table
                                                    statements-table
                                                    billing-transactions]))]
    (every? true? tables-created?)))


(defn run-table-changes
  []
  (p/let [management-token-changes     (run-alter-statement data-definition/alter-card-management-token)
          cross-reference-changes      (run-alter-statement data-definition/alter-cross-reference)
          _card-history-changes        (run-alter-statement data-definition/alter-card-history)
          _statement-changes           (run-alter-statement data-definition/alter-statements)
          statement-changes            (run-alter-statement data-definition/alter-statements-datatypes)
          billing-transactions-changes (run-alter-statement data-definition/alter-billing-transactions)
          tables-changed?              (every? some? [management-token-changes cross-reference-changes statement-changes billing-transactions-changes])]
    tables-changed?))
