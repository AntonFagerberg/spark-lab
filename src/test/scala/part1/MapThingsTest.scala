package part1

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import setup.SparkTest

@RunWith(classOf[JUnitRunner])
class MapThingsTest extends SparkTest {
  test("We should map things") {
    MapThings.mapSomething(sparkContext.parallelize("nothing yet" :: Nil)).collect() shouldBe empty
  }
}
