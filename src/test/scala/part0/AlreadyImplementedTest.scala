package part0

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class AlreadyImplementedTest extends SparkTest {
  test("The numbers should be multiplied with two") {
    val input = 0 until 10000
    val expectedResult = input.map(_ * 2)

    val inputRDD = sparkContext.parallelize(input)
    val result = AlreadyImplemented.multiplyAllNumbersByTwo(inputRDD)

    result.collect().sorted shouldBe expectedResult
  }
}