(use '[fencepost.avro])

(import '(java.io File FileOutputStream)
	'(java.util Random)
	'(org.apache.commons.lang3 RandomStringUtils)
        )

; Basic sanity check to make sure Clojure implementation of encode and decode work well together.
(let [arandom (Random.)
     randomint (.nextInt arandom)
     randomfloat (.nextFloat arandom)
     randomstr (RandomStringUtils/randomAlphanumeric 16)]
     (assert (.equals randomint (decode_from_schema (encode_with_schema randomint))))
     (assert (.equals randomfloat (decode_from_schema (encode_with_schema randomfloat))))

     ; Here again we get back an org.apache.avro.util.Utf8 object, forcing us to invoke apply
     ; "str" to it in order to get back what we want.  Probably should apply this as part of
     ; the stock decode function, but doing so would require traversal of supported complex
     ; types in order to decode any instances embedded within them.
     (assert (.equals randomstr (str (decode_from_schema (encode_with_schema randomstr)))))
)
