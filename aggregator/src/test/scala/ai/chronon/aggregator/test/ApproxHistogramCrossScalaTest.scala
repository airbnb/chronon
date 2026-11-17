package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.{ApproxHistogram, ApproxHistogramIr}
import junit.framework.TestCase
import org.junit.Assert._

import java.util

// APPROX_HISTOGRAM_K IRs are written with Java serialization, by Spark/Flink jobs on one Scala version and read by
// fetchers that may run on another. The fixtures below are hex dumps of ApproxHistogram.normalize output, written
// once on Scala 2.12.12 and once on 2.13.6. They have to be recorded bytes: a test JVM runs one Scala version, so it
// cannot produce the other version's encoding, and a same-version round trip passes even without the fix.
//
// Each fixture is one of these two IRs (ApproxHistogram[String](8)):
//   map:    over "FR", "FR", an exact histogram
//           ApproxHistogramIrSerializable(isApprox = false, histogram = Some(HashMap("FR" -> 2L)), sketch = None)
//   sketch: over "a", v1..v20, "a" x 50, "b" x 30, more than k distinct items, so a sketch
//           ApproxHistogramIrSerializable(isApprox = true, histogram = None, sketch = Some(<ItemsSketch bytes>))
//
// The 2.12 and 2.13 encodings of the same IR differ in two places:
//   serialVersionUID: 6421ce63b8164aae (7215275005709798062) on 2.12, 844f855097df3f78 (-8912758556351053960) on 2.13
//   None:             a plain scala.None$ object on 2.12, a scala.runtime.ModuleSerializationProxy(classOf[None$]) on
//                     2.13
// Whatever version runs this test must read all four.
class ApproxHistogramCrossScalaTest extends TestCase {
  private val agg = new ApproxHistogram[String](8)

  // what each map fixture must decode to, on any version
  private val ExpectedMap =
    ApproxHistogramIr[String](isApprox = false, sketch = None, histogram = Some(histogram("FR" -> 2L)))
  // a sketch has no equals, so the sketch fixtures are compared by their finalized frequent items
  private val ExpectedSketchItems = histogram("a" -> 53L, "b" -> 33L)

  // map IR written on Scala 2.12.12: ApproxHistogramIrSerializable(false, Some(HashMap("FR" -> 2L)), None)
  private val map212 =
    "aced00057372003861692e6368726f6e6f6e2e61676772656761746f722e626173652e417070726f78486973746f6772616d497253657269616c697a" +
      "61626c656421ce63b8164aae0200035a00086973417070726f784c0009686973746f6772616d74000e4c7363616c612f4f7074696f6e3b4c0006736b" +
      "6574636871007e00017870007372000a7363616c612e536f6d651122f2695ea18b740200014c000576616c75657400124c6a6176612f6c616e672f4f" +
      "626a6563743b7872000c7363616c612e4f7074696f6efe6937fddb0e66740200007870737200116a6176612e7574696c2e486173684d61700507dac1" +
      "c31660d103000246000a6c6f6164466163746f724900097468726573686f6c6478703f4000000000000c770800000010000000017400024652737200" +
      "0e6a6176612e6c616e672e4c6f6e673b8be490cc8f23df0200014a000576616c7565787200106a6176612e6c616e672e4e756d62657286ac951d0b94" +
      "e08b02000078700000000000000002787372000b7363616c612e4e6f6e6524465024f653ca94ac0200007871007e0005"

  // sketch IR written on Scala 2.12.12: ApproxHistogramIrSerializable(true, None, Some(<ItemsSketch bytes>))
  private val sketch212 =
    "aced00057372003861692e6368726f6e6f6e2e61676772656761746f722e626173652e417070726f78486973746f6772616d497253657269616c697a" +
      "61626c656421ce63b8164aae0200035a00086973417070726f784c0009686973746f6772616d74000e4c7363616c612f4f7074696f6e3b4c0006736b" +
      "6574636871007e00017870017372000b7363616c612e4e6f6e6524465024f653ca94ac0200007872000c7363616c612e4f7074696f6efe6937fddb0e" +
      "667402000078707372000a7363616c612e536f6d651122f2695ea18b740200014c000576616c75657400124c6a6176612f6c616e672f4f626a656374" +
      "3b7871007e0004757200025b42acf317f8060854e002000078700000003b0004010a0303000000020000000000000065000000000000000300000000" +
      "0000001e00000000000000320000000000000001000000620100000061"

  // map IR written on Scala 2.13.6: ApproxHistogramIrSerializable(false, Some(HashMap("FR" -> 2L)), None)
  private val map213 =
    "aced00057372003861692e6368726f6e6f6e2e61676772656761746f722e626173652e417070726f78486973746f6772616d497253657269616c697a" +
      "61626c65844f855097df3f780200035a00086973417070726f784c0009686973746f6772616d74000e4c7363616c612f4f7074696f6e3b4c0006736b" +
      "6574636871007e00017870007372000a7363616c612e536f6d651122f2695ea18b740200014c000576616c75657400124c6a6176612f6c616e672f4f" +
      "626a6563743b7872000c7363616c612e4f7074696f6efe6937fddb0e66740200007870737200116a6176612e7574696c2e486173684d61700507dac1" +
      "c31660d103000246000a6c6f6164466163746f724900097468726573686f6c6478703f4000000000000c770800000010000000017400024652737200" +
      "0e6a6176612e6c616e672e4c6f6e673b8be490cc8f23df0200014a000576616c7565787200106a6176612e6c616e672e4e756d62657286ac951d0b94" +
      "e08b0200007870000000000000000278737200267363616c612e72756e74696d652e4d6f64756c6553657269616c697a6174696f6e50726f78790000" +
      "0000000000010200014c000b6d6f64756c65436c6173737400114c6a6176612f6c616e672f436c6173733b78707672000b7363616c612e4e6f6e6524" +
      "465024f653ca94ac0200007871007e0005"

  // sketch IR written on Scala 2.13.6: ApproxHistogramIrSerializable(true, None, Some(<ItemsSketch bytes>))
  private val sketch213 =
    "aced00057372003861692e6368726f6e6f6e2e61676772656761746f722e626173652e417070726f78486973746f6772616d497253657269616c697a" +
      "61626c65844f855097df3f780200035a00086973417070726f784c0009686973746f6772616d74000e4c7363616c612f4f7074696f6e3b4c0006736b" +
      "6574636871007e0001787001737200267363616c612e72756e74696d652e4d6f64756c6553657269616c697a6174696f6e50726f7879000000000000" +
      "00010200014c000b6d6f64756c65436c6173737400114c6a6176612f6c616e672f436c6173733b78707672000b7363616c612e4e6f6e6524465024f6" +
      "53ca94ac0200007872000c7363616c612e4f7074696f6efe6937fddb0e667402000078707372000a7363616c612e536f6d651122f2695ea18b740200" +
      "014c000576616c75657400124c6a6176612f6c616e672f4f626a6563743b7871007e0007757200025b42acf317f8060854e002000078700000003b00" +
      "04010a03030000000200000000000000650000000000000003000000000000001e00000000000000320000000000000001000000620100000061"

  private def bytes(hex: String): Array[Byte] = hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

  private def histogram(counts: (String, Long)*): util.HashMap[String, Long] = {
    val result = new util.HashMap[String, Long]()
    counts.foreach { case (k, v) => result.put(k, v) }
    result
  }

  private def assertReadsMap(hex: String): Unit = {
    val ir = agg.denormalize(bytes(hex))
    // None is a singleton, so a second None$ instance read from 2.12 bytes on 2.13 would not equal ExpectedMap
    assertEquals(ExpectedMap, ir)
    assertEquals(histogram("FR" -> 2L), agg.finalize(ir))
    // and it keeps working as an IR
    assertEquals(histogram("FR" -> 3L), agg.finalize(agg.update(ir, "FR")))
  }

  private def assertReadsSketch(hex: String): Unit = {
    val ir = agg.denormalize(bytes(hex))
    assertTrue(ir.isApprox)
    assertEquals(None, ir.histogram)
    assertEquals(ExpectedSketchItems, agg.finalize(ir))
  }

  def testReadsMapWrittenByScala212(): Unit = assertReadsMap(map212)
  def testReadsSketchWrittenByScala212(): Unit = assertReadsSketch(sketch212)
  def testReadsMapWrittenByScala213(): Unit = assertReadsMap(map213)
  def testReadsSketchWrittenByScala213(): Unit = assertReadsSketch(sketch213)

  // Writers must keep producing the bytes they always have, so readers on the same version that do not have
  // ApproxHistogramIrInputStream yet keep working during a rollout. Pinning the serialVersionUID would break this on
  // one of the versions.
  def testWritesTheSameBytesAsBefore(): Unit = {
    val fixtures = scala.util.Properties.versionNumberString match {
      case v if v.startsWith("2.12.") => Some((map212, sketch212))
      case v if v.startsWith("2.13.") => Some((map213, sketch213))
      case _                          => None // no fixtures recorded for other versions
    }
    fixtures.foreach {
      case (mapHex, sketchHex) =>
        def hex(ir: Any): String = ir.asInstanceOf[Array[Byte]].map("%02x".format(_)).mkString
        assertEquals(mapHex, hex(agg.normalize(agg.update(agg.prepare("FR"), "FR"))))
        val sketchInput = Seq("a") ++ (1 to 20).map(i => s"v$i") ++ Seq.fill(50)("a") ++ Seq.fill(30)("b")
        assertEquals(sketchHex,
                     hex(agg.normalize(sketchInput.tail.foldLeft(agg.prepare(sketchInput.head))(agg.update))))
    }
  }

  def testRoundTrip(): Unit = {
    val ir = agg.update(agg.prepare("FR"), "FR")
    assertEquals(agg.finalize(ir), agg.finalize(agg.denormalize(agg.normalize(ir))))
  }
}
