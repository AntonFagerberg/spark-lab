package part1

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class Part1Test extends SparkTest {

  test("Make it upper case") {
    val prideAndPrejudiceRDD = sparkContext.parallelize(prideAndPrejudice)

    val result = Part1.makeItUpperCase(prideAndPrejudiceRDD).collect()

    val expected = prideAndPrejudice.map(_.toUpperCase)

    result should contain theSameElementsAs expected
  }

  test("Mr. Darcy") {
    val prideAndPrejudiceRDD = sparkContext.parallelize(prideAndPrejudice)

    val result = Part1.mrDarcy(prideAndPrejudiceRDD).collect()

    val expected = prideAndPrejudice.filter(_.contains("Darcy"))

    result should contain theSameElementsAs expected
  }

  test("How many words") {
    val prideAndPrejudiceRDD = sparkContext.parallelize(prideAndPrejudice)

    val result = Part1.howManyWords(prideAndPrejudiceRDD)

    val expected = prideAndPrejudice.flatMap(_.split(' ')).length

    result shouldBe expected
  }

  test("Shortest non-empty line") {
    val prideAndPrejudiceRDD = sparkContext.parallelize(prideAndPrejudice)

    val result = Part1.shortestNonEmptyLine(prideAndPrejudiceRDD)

    //noinspection SimplifiableFoldOrReduce
    val expected =
      prideAndPrejudice
        .map(_.length)
        .filter(_ > 0)
        .min

    result shouldBe expected
  }

  test("'a' is my favourite") {
    val prideAndPrejudiceRDD = sparkContext.parallelize(prideAndPrejudice)

    val result = Part1.aIsMyFavourite(prideAndPrejudiceRDD)

    val expected =
      prideAndPrejudice
        .map(_.filter(_ == 'a'))
        .reduce(_ + _)

    result shouldBe expected
  }
}
