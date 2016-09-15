package part3

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import setup.User

/** Part 3: Shared variables - accumulator and broadcast variables.
  *
  * In this part we will implement and use accumulators
  * and broadcast variables.
  *
  * You can read more about them here:
  * http://spark.apache.org/docs/latest/programming-guide.html#shared-variables
  */

object Part3 {
  /** Using the Spark context, create a broadcast variable
    * containing the string "Hello, World!".
    */
  def helloWorldBroadcastVariable(sparkContext: SparkContext): Broadcast[String] = {
    sparkContext.broadcast("Hello, World!")
  }

  def longAccumulator: AccumulatorV2[Long, Long] = {
    /** Implement an accumulator of type Long (input and output).
      */
    class LongAccumulator extends AccumulatorV2[Long, Long] {
      var currentValue = 0l
      /**
        * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
        * value; for a list accumulator, Nil is zero value.
        */
      override def isZero: Boolean = {
        currentValue == 0l
      }

      /**
        * Creates a new copy of this accumulator.
        */
      override def copy(): AccumulatorV2[Long, Long] = {
        val l = new LongAccumulator()
        l.currentValue = currentValue
        l
      }

      /**
        * Resets this accumulator, which is zero value. i.e. call `isZero` must
        * return true.
        */
      override def reset(): Unit = {
        currentValue = 0l
      }

      /**
        * Takes the inputs and accumulates.
        */
      override def add(v: Long): Unit = {
        currentValue += v
      }

      /**
        * Merges another same-type accumulator into this one and update its state, i.e. this should be
        * merge-in-place.
        */
      override def merge(other: AccumulatorV2[Long, Long]): Unit = {
        currentValue += other.value
      }

      /**
        * Defines the current value of this accumulator
        */
      override def value: Long = currentValue
    }

    new LongAccumulator()
  }

  /** Now you will use the accumulator you created in the previous method.
    * Thus: you need to implement "longAccumulator" before doing this.
    *
    * Increment "accumulator" by one for each user in "users".
    */
  def useTheLongAccumulator(users: RDD[User], accumulator: AccumulatorV2[Long, Long]): Unit = {
    users.foreach(_ => accumulator.add(1l))
  }

  /** We will now implement an accumulator with different input and output.
    *
    * The input will be User objects, the output should be a list of all
    * the unique ages´
    */
  def userAgeAccumulator: AccumulatorV2[User, List[Int]] = {
    class UserAgeAccumulator extends AccumulatorV2[User, List[Int]] {
      var currentValue = List.empty[Int]
      /**
        * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
        * value; for a list accumulator, Nil is zero value.
        */
      override def isZero: Boolean = {
        currentValue.isEmpty
      }

      /**
        * Creates a new copy of this accumulator.
        */
      override def copy(): AccumulatorV2[User, List[Int]] = {
        val l = new UserAgeAccumulator()
        l.currentValue = currentValue
        l
      }

      /**
        * Resets this accumulator, which is zero value. i.e. call `isZero` must
        * return true.
        */
      override def reset(): Unit = {
        currentValue = Nil
      }

      /**
        * Takes the inputs and accumulates.
        */
      override def add(v: User): Unit = {
        currentValue = (v.age :: currentValue).distinct
      }

      /**
        * Merges another same-type accumulator into this one and update its state, i.e. this should be
        * merge-in-place.
        */
      override def merge(other: AccumulatorV2[User, List[Int]]): Unit = {
        currentValue = (other.value ++ currentValue).distinct
      }

      /**
        * Defines the current value of this accumulator
        */
      override def value: List[Int] = currentValue
    }

    new UserAgeAccumulator()
  }
}
