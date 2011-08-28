(use '[fencepost.avro])

(import '(java.io File FileOutputStream)
	'(java.util Random)
        )


; Avro supports a number of primitive types as well as a decent set of "complex" types (the expected array and map as 
; well as records, enums and others).  We don't need to test everything here, just a few of the base types and some 
; of the simpler complex types.  Support for the more complex "complex" types could be added in the future if
; necessary
(let [intfile (File. "int.avro")
      intstream (FileOutputStream. intfile)
      arandom (Random.)
      randomint (.nextInt arandom)
      data (encode_with_schema randomint)]
      (println (format "Writing random integer %s (%s) to file %s" randomint data (.getAbsolutePath intfile)))
      (.write intstream data)
      (.flush intstream)
      (.close intstream)
)
