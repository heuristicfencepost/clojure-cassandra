(use '[fencepost.avro])

; Dependencies
; commons-lang 3.0.1

(import '(java.io File FileOutputStream)
	'(java.util Random)
	'(org.apache.commons.lang3 RandomStringUtils)
        )

(defn create_avro_file [indata outfilename]

      (let [outfile (File. outfilename)
      	   outstream (FileOutputStream. outfile)]
      	   (println (format "Writing data %s to file %s" (.toString indata) (.getAbsolutePath outfile)))
      	   (.write outstream (encode_with_schema indata))
      	   (.flush outstream)
      	   (.close outstream)
	   )
)

; Avro supports a number of primitive types as well as a decent set of "complex" types (the expected array and map as 
; well as records, enums and others).  We don't need to test everything here, just a few of the base types and some 
; of the simpler complex types.  Support for the more complex "complex" types could be added in the future if
; necessary
(def arandom (Random.))
(create_avro_file (.nextInt arandom) "int.avro")
(create_avro_file (.nextFloat arandom) "float.avro")

; Leverage commons-lang here because I'm too lazy to write my own random string generator... in Clojure anyway.
(create_avro_file (RandomStringUtils/randomAlphanumeric 16) "string.avro")

(create_avro_file [1 2.0 "foo"] "array.avro")
(create_avro_file {"foo" 1, "bar" 2.0} "map.avro")