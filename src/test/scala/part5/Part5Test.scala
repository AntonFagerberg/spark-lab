package part5

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

import scala.annotation.tailrec

@RunWith(classOf[JUnitRunner])
class Part5Test extends SparkTest {

  test("Largest graphs") {
    val largestGraphRDD = sparkContext.parallelize(largestGraph)

    val result = Part5.largestGraphs(largestGraphRDD).collect().map(_.sorted)

    @tailrec
    def findExpectedLargestGraphs(input: List[List[Int]]): List[List[Int]] = {
      val iteration =
        input.foldLeft(List.empty[List[Int]]) { case (acc, item) =>
          val (matches, noMatches) = acc.partition(_.exists(item.contains))

          if (matches.nonEmpty) {
            matches.foldLeft(item)(_ ++ _).distinct :: noMatches
          } else {
            item :: acc
          }
        }

      if (iteration.size < input.size) findExpectedLargestGraphs(iteration)
      else iteration.map(_.sorted.distinct)
    }

    val expected = findExpectedLargestGraphs(largestGraph.map(_.sorted))

    result should contain theSameElementsAs expected
  }
}
