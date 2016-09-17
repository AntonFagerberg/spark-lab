package part5

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Part 5: iterative / recursive function.
  *
  * In this part we will use the RDD in an iterative / recursive
  * manner in order to find all the largest graphs.
  *
  * Input is an RDD with a list of two nodes which you can
  * "travel between". Your task is to build all the largest graphs.
  *
  * Example input:
  * List(1, 2) -- you can get from 1 to 2 and from 2 to 1
  * List(2, 3) -- you can get from 2 to 3 and from 3 to 2, and since you can get from 1 to 2, you can travel between 1, 2 & 3.
  * List(4, 3) -- and so on
  * List(6, 5) -- 6 and 5 are not connected to 1, 2, 3 or 4.
  *
  * Example output:
  * List(1, 2, 3, 4)
  * List(6, 5)
  */
object Part5 {
  def largestGraphs(input: RDD[List[Int]]): RDD[List[Int]] = {
    ???
  }
}
