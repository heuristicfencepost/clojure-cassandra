; Populate data for a set of random users to a Cassandra instance.
;
; Users consist of the following set of data:
; - a username [String]
; - a user ID [integer]
; - a flag indicating whether the user is "active" [boolean]
; - a list of location IDs for each user [list of integer]
;
; User records are keyed by username rather than user IDs, mainly because at the moment 
; we only support strings for key values.  The Cassandra API exposes keys as byte arrays 
; so we could extend our Cassandra support to include other datatypes.

(use '[fencepost.avro])
(use '[fencepost.cassandra])

(import '(org.apache.commons.lang3 RandomStringUtils)
	'(java.util Random)
        )


; Utility function to combine our Avro lib with our Cassandra lib
(defn add_user [client username userid active locationids]
      (let [userid_data (encode_with_schema userid)
      	    active_data (encode_with_schema active)
	    locationids_data (encode_with_schema locationids)]
	        (insert client username "employee" "userid" userid_data)
		(insert client username "employee" "active" active_data)
      		(insert client username "employee" "locationids" locationids_data)
	    )
)

; Generate a list of random usernames
(let [client (connect "localhost" 9160 "employees")]
      (dotimes [n 10]
      	       (let [username (RandomStringUtils/randomAlphanumeric 16)
	       	     random (Random.)
		     userid (.nextInt random 1000)
		     active (.nextBoolean random)
		     locationids (into [] (repeatedly 10 #(.nextInt random 100)))]
      	       	    (add_user client username userid active locationids)
		    (println (format "Added user %s: [%s %s %s]" username userid active locationids))
		    )
      )
)
