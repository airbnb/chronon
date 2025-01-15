package ai.chronon.flink.test.window

import ai.chronon.api.Builders
import ai.chronon.flink.window.KeySelector
import org.junit.Test

class KeySelectorTest {
  @Test
  def TestChrononFlinkJobCorrectlyKeysByAGroupbysEntityKeys(): Unit = {
    // We expect something like this to come out of the SparkExprEval operator
    val sampleSparkExprEvalOutput: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")

    val groupByWithOneEntityKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelector.getKeySelectionFunction(groupByWithOneEntityKey)
    assert(
      keyFunctionOne(sampleSparkExprEvalOutput) == List(4242)
    )

    val groupByWithTwoEntityKey = Builders.GroupBy(keyColumns = Seq("number", "user"))
    val keyFunctionTwo = KeySelector.getKeySelectionFunction(groupByWithTwoEntityKey)
    assert(
      keyFunctionTwo(sampleSparkExprEvalOutput) == List(4242, "abc")
    )
  }

  @Test
  def testKeySelectorFunctionReturnsSameHashesForListsWithTheSameContent(): Unit = {
    // This is more of a sanity check. It's not comprehensive.
    // SINGLE ENTITY KEY
    val map1: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val map2: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "10.0.0.1", "user" -> "notabc")
    val groupBySingleKey = Builders.GroupBy(keyColumns = Seq("number"))
    val keyFunctionOne = KeySelector.getKeySelectionFunction(groupBySingleKey)
    assert(
      keyFunctionOne(map1).hashCode() == keyFunctionOne(map2).hashCode()
    )

    // TWO ENTITY KEYS
    val map3: Map[String, Any] =
      Map("number" -> 4242, "ip" -> "192.168.0.1", "user" -> "abc")
    val map4: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> 4242, "user" -> "notabc")
    val groupByTwoKeys = Builders.GroupBy(keyColumns = Seq("number", "ip"))
    val keyFunctionTwo = KeySelector.getKeySelectionFunction(groupByTwoKeys)
    assert(
      keyFunctionTwo(map3).hashCode() == keyFunctionTwo(map4).hashCode()
    )

    val map5: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    val map6: Map[String, Any] =
      Map("ip" -> "192.168.0.1", "number" -> null)
    assert(keyFunctionTwo(map5).hashCode() == keyFunctionTwo(map6).hashCode())
  }
}
