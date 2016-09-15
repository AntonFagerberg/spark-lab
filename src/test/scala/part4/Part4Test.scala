package part4

import org.apache.spark.storage.StorageLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class Part4Test extends SparkTest {

  test("User partitioner") {
    val partitionCount = 10

    val partitioner = Part4.userPartitioner(partitionCount)

    val partitionedRDD =
      sparkContext
        .parallelize(users)
        .keyBy(Predef.identity)
        .partitionBy(partitioner)
        .persist(StorageLevel.MEMORY_ONLY)

    partitionedRDD.getNumPartitions shouldBe partitionCount

    val result =
      partitionedRDD
        .glom()
        .collect()
        .map(_.map { case (user, _) => user.age }.distinct)

    result
      .combinations(2)
      .exists { case Array(l1, l2) =>
        l1.exists(l2.contains)
      } shouldBe false
  }
}
