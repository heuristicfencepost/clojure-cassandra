(use '[fencepost.cassandra])

; For a given column family print all keys in the keyspace and all columns for each key in the keyspace.
(let [client (connect "localhost" 9160 "foo")
      key_slices (get_range_slices client "bar" "!" "~")
      keys (range_slices_keys key_slices)]

  (let [formatfn
        (fn [key]
          (let [cols (range_slices_columns key_slices key)]
            (format "Key %s: columns => %s\n" key cols)
            )
          )]
    (print (reduce str (map formatfn keys)))
    )
  )
