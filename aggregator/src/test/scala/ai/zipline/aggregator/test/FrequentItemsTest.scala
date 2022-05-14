package ai.zipline.aggregator.test
import ai.zipline.aggregator.base.{FrequentItems, SimpleAggregator, TopK}
import ai.zipline.api.StringType
import com.google.gson.Gson
import com.yahoo.sketches.frequencies.ErrorType
import junit.framework.TestCase
import org.junit.Assert._

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter

class FrequentItemsTest extends TestCase {

  def baseHarness[Input, IR, Output](agg: SimpleAggregator[Input, IR, Output],
                                     chunkSize: Int,
                                     elems: Stream[Input]): IR = {
    def toIr(chunk: Seq[Input]): IR = {
      chunk.tail.foldLeft(agg.prepare(chunk.head))(agg.update)
    }
    val mergedIr = elems.sliding(chunkSize, chunkSize).map(toIr).reduce(agg.merge)
    val directIr = toIr(elems)

//    val output = agg.finalize(directIr)
//    val gson = new Gson()
//    val mergedString = gson.toJson(output)
//    val directString = gson.toJson(agg.finalize(mergedIr))
//    assertEquals(mergedString, directString)
    agg.denormalize(agg.normalize(mergedIr))
  }

  def generateData(prefix: String,
                   skewSize: Int = 64,
                   skewRatio: Double = 0.4,
                   numRange: Int = 10000000,
                   numCount: Int = 10000000): (Array[String], Stream[String]) = {
    val skewItems: Array[String] =
      (0 until skewSize).map(_ => s"${prefix}${Math.floor(Math.random() * numRange).toLong}").toArray
    skewItems -> Stream.fill(numCount)(
      if (Math.random() < skewRatio) {
        val skewIndex = Math.floor(Math.random() * skewItems.length).toInt
        skewItems(skewIndex)
      } else {
        val num = Math.floor(Math.random() * numRange).toInt
        s"${prefix}${num}"
      }
    )
  }

  def testTopK: Unit = {}
  def testBasic: Unit = {
    def runTest(agg: FrequentItems): Unit = {
      println(s"errorType: ${agg.errorType.toString}")
      val (skewItems, itemStream) = generateData("user")
      val detectedIr = baseHarness(agg, 10000, itemStream)
      println(s"sketchSize: ${agg.normalize(detectedIr).length}")
      val detected = agg.finalize(detectedIr)
      println(skewItems.length)
      println(detected.size)
      assertEquals(skewItems.sorted.mkString(", "), detected.keys.toArray.sorted.mkString(", "))
    }
    runTest(new FrequentItems(1024, ErrorType.NO_FALSE_POSITIVES))
  }
}
