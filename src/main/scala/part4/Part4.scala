package part4

import org.apache.spark.Partitioner
import setup.User

/** Part 4: Partitioner
  *
  * The partitioner is responsible for dividing the items contained in an RDD
  * across a fixed number of partitions.
  *
  * This is done by mapping all objects to an integer between 0 and numPartitions.
  */
object Part4 {
  /** Create a partitioner which makes sure all users of
    * the same age end up in the same partition.
    *
    * This partitioner doesn't have to handle any other types
    * beside User.
    *
    * The partitioner should use as many partitions as the
    * input "nrOfPartitions" defines.
    */
  def userPartitioner(nrOfPartitions: Int): Partitioner = {

    class UserPartitioner extends Partitioner {
      override def numPartitions: Int = ???

      override def getPartition(key: Any): Int = {
        ???
      }
    }

    new UserPartitioner
  }
}
