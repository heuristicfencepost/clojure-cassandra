(import '(org.apache.thrift.transport TFramedTransport TSocket)
        '(org.apache.thrift.protocol TBinaryProtocol)
        '(org.apache.cassandra.thrift Cassandra$Client SliceRange SlicePredicate ColumnParent KeyRange ConsistencyLevel)
        )

(defn connect [host port keyspace]
  "Connect to a Cassandra instance on the specified host and port.  Set things up to use the specified key space."
  (let [transport (TFramedTransport. (TSocket. host port))
        protocol (TBinaryProtocol. transport)
        client (Cassandra$Client. protocol)]
    (.open transport)
    (.set_keyspace client keyspace)
    client
    )
  )

(defn get_range_slices [client cf start end]
  "Simple front end into the get_range_slices function exposed via Thrift"
  (let [slice_range
        (doto (SliceRange.)
          (.setStart (byte-array 0))
          (.setFinish (byte-array 0))
          (.setReversed false)
          (.setCount 100)
          )
        slice_predicate
        (doto (SlicePredicate.)
          (.setSlice_range slice_range))
        column_parent (ColumnParent. cf)
        key_range
        (doto (KeyRange.)
          (.setStart_key (.getBytes start))
          (.setEnd_key (.getBytes end)))
        ]
    (.get_range_slices client column_parent slice_predicate key_range ConsistencyLevel/ONE) 
    )
  )

(defn range_slices_keys [slices]
  "Retrieve the set of keys in a get_range_slices result"
  (map #(String. (.getKey %)) slices) 
  )

(defn range_slices_columns [slices key]
  "Retrieve a map of the columns associated with the specified key in a get_range_slices result"
  (let [match (first (filter #(= key (String. (.getKey %))) slices))]
    (cond (nil? match) nil
          (true? true)
          (let [urcols (.getColumns match)
                cols (map #(.getColumn %) urcols)]
            (zipmap (map #(keyword (String. (.getName %))) cols)
                    (map #(String. (.getValue %)) cols))
            )
          )
    )
  )

(let [client (connect "localhost" 9160 "twitter")
      key_slices (get_range_slices client "authors" "!" "~")
      five_keys (take 5 (range_slices_keys key_slices))]

  (print five_keys)

  (let [formatfn
        (fn [key]
          (let [cols (range_slices_columns key_slices key)]
            (format "Key %s: name => %s, following => %s\n" key (cols :name) (cols :following))
            )
          )]
    (print (reduce str (map formatfn five_keys)))
    )
  )
