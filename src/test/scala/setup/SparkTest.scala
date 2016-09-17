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

  lazy val users =
    readFile("users.txt")
      .sliding(7, 7)
      .map { case List(_, name, surname, city, country, age, _) =>
        User(
          name.drop(1).dropRight(2),
          surname.drop(1).dropRight(2),
          city.drop(1).dropRight(2),
          country.drop(1).dropRight(2),
          age.toInt
        )
      }
      .toList

  lazy val largestGraph =
    readFile("largest_graph.txt")
    .drop(3)
    .map(_.splitAt(2))
    .map { case (from, to) =>
      List(from.trim.toInt, to.trim.toInt)
    }
}