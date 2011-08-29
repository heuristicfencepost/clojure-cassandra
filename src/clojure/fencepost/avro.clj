(ns fencepost.avro)

; Dependencies
; avro 1.5.2
; paranamer 2.0 (required by Avro)
; jackson (required by Avro)

(import '(org.apache.avro Schema)
	'(org.apache.avro.reflect ReflectData ReflectDatumWriter)
	'(org.apache.avro.io BufferedBinaryEncoder EncoderFactory)
	'(java.io ByteArrayOutputStream)
        )

(defn encode_with_schema [target]
      "Use Java reflection to generate a schema for the input object and return that schema along with encoded data"
      (let [targetschema (.getSchema (ReflectData/get) (.getClass target))
      	    targetwriter (ReflectDatumWriter. targetschema)
	    buffer (ByteArrayOutputStream.)
      	    encoder (.binaryEncoder (EncoderFactory.) buffer nil)]

	    ; Populate the buffer with Avro data for the target
	    (.write targetwriter target encoder)
	    (.flush encoder)

	    (let [targetdata (.toByteArray buffer)
	    	  ; Ideally we'd use a record for this sort of thing but doing so would require setX()
		  ; accessors for all fields.  Use of a map here is less precise but quite a bit easier
		  ; to code.
		  metaschema "{\"type\": \"map\", \"values\": \"bytes\"}"
	    	  metawriter (ReflectDatumWriter. (Schema/parse metaschema))]
		  (.reset buffer)
		  (.write metawriter {"schema" (.getBytes (.toString targetschema)) "data" targetdata} encoder)
		  (.flush encoder)
		  (.toByteArray buffer)
		  )
	    )
)


