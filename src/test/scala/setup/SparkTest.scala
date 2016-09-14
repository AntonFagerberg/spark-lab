package setup

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.Codec

trait SparkTest extends FunSuite with BeforeAndAfterAll with Matchers {
  var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    val sparkConf =
      new SparkConf()
        .setAppName("SparkLab")
        .setMaster("local[*]")

    sparkContext = new SparkContext(sparkConf)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    super.afterAll()
  }

  def readFile(filename: String): List[String] = {
    scala
      .io
      .Source
      .fromURL(
        getClass
          .getClassLoader
          .getResource(filename)
      )(Codec.UTF8)
      .getLines()
      .toList
  }

  lazy val prideAndPrejudice = readFile("pride_and_prejudice.txt")
}
