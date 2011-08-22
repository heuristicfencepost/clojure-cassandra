package org.fencepost.cassandra

import scala.collection.JavaConversions

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport._
import org.apache.cassandra.service._
import org.apache.cassandra.thrift._

object ThriftCassandraClient {

  def connect(host:String,port:Int,keyspace:String):Cassandra.Client = {

    val transport = new TFramedTransport(new TSocket(host, port))
    val protocol = new TBinaryProtocol(transport)
    val client = new Cassandra.Client(protocol)
    transport.open()
    client set_keyspace keyspace
    client
  }

  // Execute a range slice query against the specified Cassandra instance.  Method returns
  // an object suitable for later interrogation by range_slices_keys() or range_slices_columns()
  def get_range_slices(client:Cassandra.Client,cf:String,start:String,end:String):Iterable[KeySlice] = {

    val sliceRange = new SliceRange()
    sliceRange setStart new Array[Byte](0)
    sliceRange setFinish new Array[Byte](0)
    sliceRange setReversed false
    sliceRange setCount 100
    val slicePredicate = new SlicePredicate()
    slicePredicate setSlice_range sliceRange
    val columnParent = new ColumnParent(cf)
    val keyRange = new KeyRange()
    keyRange setStart_key (start getBytes)
    keyRange setEnd_key (end getBytes)

    val javakeys = client.get_range_slices(columnParent,slicePredicate,keyRange,ConsistencyLevel.ONE)

    // Return from Thrift Java client is List<KeySlice> so we have to explicitly convert it here
    JavaConversions asScalaIterable javakeys
  }

  // Return an Iterable for all keys in an input query state object
  def range_slices_keys(slices:Iterable[KeySlice]) = slices map { c => new String(c.getKey) }

  // Return an Option containing column information for the specified key in the input query
  // state object.  If the key isn't found None is returned, otherwise the Option contains a
  // map of column names to column values.
  def range_slices_columns(slices:Iterable[KeySlice], key:String):Option[Map[String,String]] = {
    
    slices find { c => new String(c.getKey()) == key } match {
      case None => None
      case Some(keyval) =>
      
        val urcols = JavaConversions asScalaIterable (keyval getColumns)
        val cols:Seq[Column] = (urcols map (_ getColumn)).toSeq
        Some(Map(cols map { c => (new String(c.getName())) -> (new String(c.getValue())) }:_*))
    }
  }

  def main(args: Array[String]): Unit = {

    val client = connect("localhost", 9160,"twitter")
    val slices = get_range_slices(client,"authors","!","~")
    val fivekeys = range_slices_keys(slices) take 5
    println("fivekeys: " + fivekeys)
    for (key <- fivekeys) {
      range_slices_columns(slices,key) match {
        case None =>
        case Some(cols) =>
          println(
            "Key " + key +
            ": name => " + (cols getOrElse ("name","")) +
            ", following => " + (cols getOrElse ("following",""))
          )
      }
    }
  }
}
