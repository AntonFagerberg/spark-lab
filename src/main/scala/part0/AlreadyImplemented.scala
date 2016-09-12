package part0

import org.apache.spark.rdd.RDD

/* You don't have to do anything here.
 * Just make sure the test in part0 works!
 */
object AlreadyImplemented {
  def multiplyAllNumbersByTwo(numbers: RDD[Int]): RDD[Int] = {
    numbers.map(_ * 2)
  }
}
