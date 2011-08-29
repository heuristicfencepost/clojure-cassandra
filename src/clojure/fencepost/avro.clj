(ns fencepost.avro)

; Dependencies
; avro 1.5.2
; paranamer 2.0 (required by Avro)
; jackson (required by Avro)

(import '(org.apache.avro Schema)
	'(org.apache.avro.reflect ReflectData ReflectDatumWriter)
	'(org.apache.avro.generic GenericDatumReader)
	'(org.apache.avro.io BufferedBinaryEncoder EncoderFactory DecoderFactory)
	'(java.io ByteArrayOutputStream ByteArrayInputStream)
        )

(defn get_meta_schema []
      ; Ideally we'd use a record for this sort of thing but doing so would require setX()
      ; accessors for all fields.  Use of a map here is less precise but quite a bit easier
      ; to code.
      "{\"type\": \"map\", \"values\": \"bytes\"}"
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
	    	  metawriter (ReflectDatumWriter. (Schema/parse (get_meta_schema)))]
		  (.reset buffer)
		  (.write metawriter 
		  	  {"schema" (.getBytes (.toString targetschema)) "data" targetdata} 
			  encoder)
		  (.flush encoder)
		  (.toByteArray buffer)
		  )
	    )
)

(defn decode_from_schema [indata]
      "Use the meta_schema to extract data and schema and then extract raw data"
      (let [metareader (GenericDatumReader. (Schema/parse (get_meta_schema)))
      	    buffer (ByteArrayInputStream. indata)
      	    decoder (.binaryDecoder (DecoderFactory.) buffer nil)

	    ; The data type returned from the underlying Avro Java code needs a bit
	    ; of massaging before we can move forward.  Avro decoders return strings
	    ; as instances of Utf8 objects so we have to apply "str" to them directly
	    ; in order to get back something we can work with.
	    middata (.read metareader nil decoder)
	    {schema "schema" data "data"} (zipmap (map str (keys middata)) (vals middata))
	    schemabytes (byte-array (.capacity schema))
	    databytes (byte-array (.capacity data))]

	    (.get schema schemabytes)
	    (.get data databytes)

	    (let [targetreader (GenericDatumReader. (Schema/parse (String. schemabytes)))
	    	 targetdecoder (.binaryDecoder (DecoderFactory.) databytes nil)]
	    	 (.read targetreader nil targetdecoder)
		 )
	)
)
