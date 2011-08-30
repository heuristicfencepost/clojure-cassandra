(ns fencepost.cassandra)

(import '(org.apache.cassandra.thrift Cassandra$Client SliceRange SlicePredicate ColumnParent KeyRange ConsistencyLevel ColumnPath Column)
	'(org.apache.thrift.transport TFramedTransport TSocket)
        '(org.apache.thrift.protocol TBinaryProtocol)
	'(java.nio ByteBuffer)
        )

; A quick note on usage.  The Cassandra Java API returns "this" for many of the setX() methods on the setters of core objects in the Thrift
; API.  Our usage here consistently employs the doto syntax rather than relying on these return values.  This is largely a matter of 
; convention, but in this case it appears to make the code a bit clearer to read.  Your mileage may vary.
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
  "Get a list of KeySlices for every key in the range beginning with start and ending with end"
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
  "Retrieve the set of keys in a list of KeySlices"
  (map #(String. (.getKey %)) slices) 
  )

(defn range_slices_columns [slices key]
  "Given a list of KeySlices retrieve a map of the columns associated with the specified key"
  (let [match (first (filter #(= key (String. (.getKey %))) slices))]
    (cond (nil? match) nil
          (true? true)
          (let [urcols (.getColumns match)
                cols (map #(.getColumn %) urcols)]
            (zipmap (map #(keyword (String. (.getName %))) cols)
                    (map #(.getValue %) cols))
            )
          )
    )
  )

(defn insert [client key cf colname colval_bytes]
  "Insert the specified column into the specified column family.  At present we don't support super columns"
  (let [key_bytes (.getBytes key)
       key_buffer (ByteBuffer/allocate (alength key_bytes))
       column_parent 
        (doto (ColumnParent.)
	  (.setColumn_family cf)
	)
       colname_bytes (.getBytes colname)
       column
	(doto (Column.)
	  (.setName colname_bytes)
	  (.setValue colname_bytes)
	  (.setTimestamp (System/currentTimeMillis))
	)]

       ; Populate the built ByteBuffer with the contents of the input key
       (.put key_buffer key_bytes)
       (.flip key_buffer)

       (.insert client key_buffer column_parent column ConsistencyLevel/ONE)
       )
)