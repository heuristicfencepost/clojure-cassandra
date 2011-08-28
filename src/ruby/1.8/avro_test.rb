require 'rubygems'
require 'avro'

# Whip up a simple schema and use it to populate a minimal data file
schema = <<SCHEMA
{"type": "record",
 "name": "BlahBlah",
 "fields": [
  {"name": "field1", "type": "string"},
  {"name": "field2", "type": "int"}
]}
SCHEMA

filename = "/tmp/avro.dat"

# Write some stuff to the data file
s = Avro::Schema.parse(schema)
dw = Avro::IO::DatumWriter.new(s)
df = Avro::DataFile::Writer.new(File.open("/tmp/avro.dat","wb"),dw,s)
df << { "field1" => "foobar", "field2" => 999 }
df.close

reader = File.new(filename,"r")
dr = Avro::IO::DatumReader.new()
df2 = Avro::DataFile::Reader.new(reader,dr)
puts "#{df2.datum_reader.writers_schema}"

