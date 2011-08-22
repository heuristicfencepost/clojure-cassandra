# Shortcut wrapper to run thrift_cassandra_client

# Make sure to include fencepost Clojure libs
CLASSPATH=src/clojure

# Re-use JARs included in the Cassandra distribution
CLASSPATH=$CLASSPATH:~/local/cassandra/lib/apache-cassandra-thrift-0.8.2.jar:~/local/cassandra/lib/libthrift-0.6.jar:~/local/cassandra/lib/slf4j-api-1.6.1.jar:~/local/cassandra/lib/slf4j-log4j12-1.6.1.jar:~/local/cassandra/lib/log4j-1.2.16.jar

~/local/bin/clojure src/clojure/thrift_cassandra_client.clj
