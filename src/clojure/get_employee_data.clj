; Retrieve information from the Cassandra database about one of our employees

(use '[fencepost.avro])
(use '[fencepost.cassandra])

(defn evaluate_user [slices username]
      "Gather information for the specified user and display a minimal report about them"

      ; Note that the code below says nothing about types.  We specify the column names we
      ; wish to access but whatever Cassandra + Avro supplies for the value of that column
      ; is what we get.
     (let [user_data (range_slices_columns slices username)
      	   userid (decode_from_schema (user_data :userid))
      	   active (decode_from_schema (user_data :active))
      	   locationids (decode_from_schema (user_data :locationids))]
      (println (format "Username: %s" username))
      (println (format "Userid: %s" userid))
      (println (if (> userid 0) "Userid is greater than zero" "Userid is not greater than zero"))
      (println (format "Active: %s" active))
      (println (if active "User is active" "User is not active"))

      ; Every user should have at least one location ID.
      ;
      ; Well, they would if we were able to successfully handle an Avro record.
      ;(assert (> (count locationids) 0))
      )
)

(let [client (connect "localhost" 9160 "employees")
      key_slices (get_range_slices client "employee" "!" "~")
      keys (range_slices_keys key_slices)]
      (println (format "Found %d users" (count keys)))
      (dotimes [n (count keys)]
      	       (evaluate_user key_slices (nth keys n))      
      )
    )

