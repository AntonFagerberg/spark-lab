package part2

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class Part2Test extends SparkTest {

  val nicknames = List("Calista" -> "Cali", "Benedict" -> "Ben", "Velma" -> "Vel")

  test("Keyed by age") {
    val usersRDD = sparkContext.parallelize(users)

    val result = Part2.keyedByAge(usersRDD).collect()

    val expected = users.map(user => user.age -> user)

    result should contain theSameElementsAs expected
  }

  test("Age count") {
    val usersRDD = sparkContext.parallelize(users)

    val result = Part2.ageCount(usersRDD).collect()

    val expected = users.groupBy(_.age).mapValues(_.length)

    result should contain theSameElementsAs expected
  }

  test("Most common age") {
    val usersRDD = sparkContext.parallelize(users)

    val result = Part2.mostCommonAge(usersRDD)

    val expected =
      users
        .groupBy(_.age)
        .mapValues(_.length)
        .maxBy { case (_, count) => count }

    result shouldBe expected
  }

  test("Users grouped by first letter") {
    val usersRDD = sparkContext.parallelize(users)

    val result = Part2.usersGroupedByFirstLetter(usersRDD).collect()

    val expected = users.groupBy(_.name.head).toList

    result.length shouldBe expected.length

    result
      .sortBy { case (letter, _) => letter }
      .zip(
        expected
          .sortBy { case (letter, _) => letter }
      ).foreach { case ((letter1, users1), (letter2, users2)) =>
        letter1 shouldBe letter2
        users1 should contain theSameElementsAs users2
      }
  }

  test("Nicknames") {
    val usersRDD = sparkContext.parallelize(users)
    val nicknamesRDD = sparkContext.parallelize(nicknames)

    val result = Part2.nickNames(usersRDD, nicknamesRDD).collect()

    val nicknameMap = nicknames.toMap

    val expected =
      users
        .flatMap { user =>
          nicknameMap.lift(user.name).map(_ -> user.surname)
        }

    result should contain theSameElementsAs expected
  }

  test("Nicknames with default value") {
    val usersRDD = sparkContext.parallelize(users)
    val nicknamesRDD = sparkContext.parallelize(nicknames)

    val result = Part2.nickNamesWithDefaultValue(usersRDD, nicknamesRDD).collect()

    val nicknameMap = nicknames.toMap

    val expected =
      users
        .map { user =>
          nicknameMap.lift(user.name).getOrElse("Nope") -> user.surname
        }

    result should contain theSameElementsAs expected
  }

  test("Shortest names by age") {
    val usersRDD = sparkContext.parallelize(users)
    val nicknamesRDD = sparkContext.parallelize(nicknames)

    val result = Part2.shortestNamesByAge(usersRDD, nicknamesRDD).collect()

    val nicknameMap = nicknames.toMap

    val expected =
      users
        .foldLeft(Map.empty[Int, List[String]]) { (acc, user) =>
          acc
            .lift(user.age)
            .map { nameList =>
              if (nameList.head.length > user.name.length) {
                acc + (user.age -> List(user.name))
              } else if (nameList.head.length < user.name.length) {
                acc
              } else {
                acc + (user.age -> (user.name :: nameList))
              }
            } getOrElse {
              acc + (user.age -> List(user.name))
            }
        }
        .mapValues(_.sorted)
        .toList

    val resultSorted = result.map { case (age, names) => age -> names.sorted }

    resultSorted should contain theSameElementsAs expected
  }
}
