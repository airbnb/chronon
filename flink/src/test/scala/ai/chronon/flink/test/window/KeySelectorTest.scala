package ai.chronon.flink.test.window

import ai.chronon.api.Builders
import ai.chronon.flink.window.KeySelector
import org.junit.Test

class KeySelectorTest {
  @Test
  def TestChrononFlinkJobCorrectlyKeysByAGroupbysEntityKeys(): Unit = {
    // We expect something like this to come out of the SparkExprEval operator
    val sampleSparkExprEvalOutput: Map[String, Any] =
      Map("card_number" -> 4242, "ip" -> "192.168.0.1", "merchant" -> "stripe")

    val groupByWithOneEntityKey = Builders.GroupBy(keyColumns = Seq("card_number"))
    val keyFunctionOne = KeySelector.getKeySelectionFunction(groupByWithOneEntityKey)
    assert(
      keyFunctionOne(sampleSparkExprEvalOutput) == List(4242)
    )

    val groupByWithTwoEntityKey = Builders.GroupBy(keyColumns = Seq("card_number", "merchant"))
    val keyFunctionTwo = KeySelector.getKeySelectionFunction(groupByWithTwoEntityKey)
    assert(
      keyFunctionTwo(sampleSparkExprEvalOutput) == List(4242, "stripe")
    )
  }

  @Test
  def testKeySelectorFunctionReturnsSameHashesForListsWithTheSameContent(): Unit = {
    // This is more of a sanity check. It's not comprehensive.
    // SINGLE ENTITY KEY
    val map1: Map[String, Any] =
    Map("card_number" -> 4242, "ip" -> "192.168.0.1", "merchant" -> "stripe")
    val map2: Map[String, Any] =
      Map("card_number" -> 4242, "ip" -> "10.0.0.1", "merchant" -> "notstripe")
    val groupBySingleKey = Builders.GroupBy(keyColumns = Seq("card_number"))
    val keyFunctionOne = KeySelector.getKeySelectionFunction(groupBySingleKey)
    assert(
      keyFunctionOne(map1).hashCode() == keyFunctionOne(map2).hashCode()
    )

    // TWO ENTITY KEYS
    val map3: Map[String, Any] =
      Map("card_number" -> 4242, "ip" -> "192.168.0.1", "merchant" -> "stripe")
    val map4: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "card_number" -> 4242, "merchant" -> "notstripe")
    val groupByTwoKeys = Builders.GroupBy(keyColumns = Seq("card_number", "ip"))
    val keyFunctionTwo = KeySelector.getKeySelectionFunction(groupByTwoKeys)
    assert(
      keyFunctionTwo(map3).hashCode() == keyFunctionTwo(map4).hashCode()
    )

    val map5: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "card_number" -> null)
    val map6: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "card_number" -> null)
    assert(keyFunctionTwo(map5).hashCode() == keyFunctionTwo(map6).hashCode())
  }
}
