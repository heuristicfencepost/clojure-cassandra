require 'cassandra/0.8'
client = Cassandra.new('foo','127.0.0.1:9160')
#puts client.get(:bar, "somekey")
#puts client.get(:bar, "someotherkey")
#puts client.get_range(:bar,{:start_key => "!",:end_key => "~"})
client.get_range(:bar,{:start_key => "!",:end_key => "~"}).each do |k,v|
  puts v
end
