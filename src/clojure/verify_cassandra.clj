(use '[fencepost.cassandra])

; Connect to a Cassandra instance, insert some data and then try to retrieve it again.
(let [keyspace "foo"
     column_family "bar"
     client (connect "localhost" 9160 keyspace)]

     ; Insert some "random" bytes.
     (insert client "foobar1" column_family "col1" (.getBytes "foobar1_col1"))
     (insert client "foobar1" column_family "col2" (.getBytes "foobar1_col2"))
     (insert client "foobar1" column_family "col3" (.getBytes "foobar1_col3"))
     (insert client "foobar2" column_family "col1" (.getBytes "foobar2_col1"))
     (insert client "foobar2" column_family "col2" (.getBytes "foobar2_col2"))
     (insert client "foobar3" column_family "col2" (.getBytes "foobar3_col2"))
     (insert client "foobar3" column_family "col3" (.getBytes "foobar3_col3"))

     ; We'll now retrieve the range slices for everything between "e" and "g".  This
     ; should include all three keys above
     (let [range_slices (get_range_slices client column_family "e" "g")]

     	  ; We should have three keys in range_slice
	  (println (range_slices_keys range_slices))

	  ; Should have three columns for key "foobar1", two columns for the other two
	  (println "Columns for key 'foobar1'")
	  (println (range_slices_columns range_slices "foobar1"))
	  (println "Columns for key 'foobar2'")
	  (println (range_slices_columns range_slices "foobar2"))
	  (println "Columns for key 'foobar3'")
	  (println (range_slices_columns range_slices "foobar3"))
     )
)
