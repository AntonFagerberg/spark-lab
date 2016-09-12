package setup

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

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
}
