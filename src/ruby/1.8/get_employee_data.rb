require 'rubygems'
require 'avro'
require 'cassandra'

def evaluate_avro_data bytes

  # Define the meta-schema
  meta_schema = Avro::Schema.parse("{\"type\": \"map\", \"values\": \"bytes\"}")

  # Read the meta source and extract the contained data and schema
  meta_datum_reader = Avro::IO::DatumReader.new(meta_schema)
  meta_val = meta_datum_reader.read(Avro::IO::BinaryDecoder.new(StringIO.new(bytes)))

  # Build a new reader which can handle the indicated schema
  schema = Avro::Schema.parse(meta_val["schema"])
  datum_reader = Avro::IO::DatumReader.new(schema)
  val = datum_reader.read(Avro::IO::BinaryDecoder.new(StringIO.new(meta_val["data"])))
end

client = Cassandra.new('employees', '127.0.0.1:9160')
client.get_range(:employee,{:start_key => "!",:finish_key => "~"}).each do |k,v|
  userid = evaluate_avro_data v["userid"]
  active = evaluate_avro_data v["active"]
  locationids = evaluate_avro_data v["locationids"]
  puts "Username: #{k}, user ID: #{userid}, active: #{active}"
  puts "User ID #{(userid > 0) ? "is" : "is not"} greater than zero"
  puts "User #{active ? "is" : "is not"} active"

  # Ruby's much more flexible notion of truthiness makes the tests above somewhat less
  # compelling.  For extra validation we add the following
  "Oops, it's not a number" unless userid.is_a? Fixnum
end
