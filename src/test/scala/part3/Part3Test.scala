package part3

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class Part3Test extends SparkTest {

  test("Broadcast variable - Hello, World!") {
    Part3
      .helloWorldBroadcastVariable(sparkContext)
      .value shouldBe "Hello, World!"
  }

  test("Using a broadcast variable") {
    val nicknamesMap = Map("Calista" -> "Cali", "Benedict" -> "Ben", "Velma" -> "Vel")

    val nicknames = sparkContext.broadcast(nicknamesMap)

    val usersRDD = sparkContext.parallelize(users)

    val result = Part3.usingBroadcastVariable(usersRDD, nicknames).collect

    val expected =
      users
        .flatMap { user =>
          nicknamesMap.lift(user.name).map(_ -> user.surname)
        }

    result should contain theSameElementsAs expected
  }

  test("Long Accumulator") {
    val usersRDD = sparkContext.parallelize(users)

    val accumulator = Part3.longAccumulator

    sparkContext.register(accumulator)

    usersRDD.foreach(_ => accumulator.add(1l))

    accumulator.value shouldBe users.length
  }

  test("Use the long Accumulator") {
    val usersRDD = sparkContext.parallelize(users)

    val accumulator = Part3.longAccumulator

    sparkContext.register(accumulator)

    Part3.useTheLongAccumulator(usersRDD, accumulator)

    accumulator.value shouldBe users.length
  }

  test("User age accumulator") {
    val usersRDD = sparkContext.parallelize(users)

    val accumulator = Part3.userAgeAccumulator

    sparkContext.register(accumulator)

    usersRDD.foreach(accumulator.add)

    accumulator.value.sorted shouldBe users.map(_.age).distinct.sorted
  }
}