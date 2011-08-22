require 'cassandra/0.8'
client = Cassandra.new('foo','127.0.0.1:9160')
client.insert(:bar, "somekey", {'someval' => 'val1'})
client.insert(:bar, "someotherkey", {'someotherval' => 'val2'})

