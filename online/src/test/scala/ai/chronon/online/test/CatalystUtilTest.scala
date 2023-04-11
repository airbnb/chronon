package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.CatalystUtil
import junit.framework.TestCase
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util

class CatalystUtilTest extends TestCase {

  private val innerStruct = StructType("inner", Array(StructField("d", LongType), StructField("e", FloatType)))
  private val outerStruct = StructType(
    "root",
    Array(
      StructField("a", IntType),
      StructField("b", DoubleType),
      StructField("c", innerStruct),
      StructField("mp", MapType(StringType, innerStruct)),
      StructField("ls", ListType(innerStruct))
    )
  )

  @Test
  def testCatalystUtil(): Unit = {
    val ctUtil = new CatalystUtil(
      Seq("a_plus" -> "a + 1",
          "b_str" -> "CAST(b as string)",
          "c_e" -> "c.e",
          "c" -> "c",
          "mp_d" -> "element_at(mp, 'key').d",
          "mp" -> "mp",
          "ls" -> "ls"),
      outerStruct
    )

    val mapVal = new util.HashMap[Any, Any]()
    mapVal.put("key", Map("e" -> 7.0f, "d" -> 8L))

    val listVal = new util.ArrayList[Any]()
    listVal.add(Map("e" -> 2.3f, "d" -> 7L))

    val result =
      ctUtil.performSql(Map("a" -> 1, "b" -> 0.58, "c" -> Map("e" -> 3.6f, "d" -> 9L), "mp" -> mapVal, "ls" -> listVal))
    val expected = Some(Map("a_plus" -> 2,
                       "b_str" -> "0.58",
                       "c_e" -> 3.6f,
                       "c" -> Map("e" -> 3.6f, "d" -> 9L),
                       "mp_d" -> 8L,
                       "mp" -> mapVal,
                       "ls" -> listVal))
    assertEquals(expected, result)
  }

  @Test
  def testCatalystUtilFilters(): Unit = {
    val ctUtil = new CatalystUtil(
      Seq("a_plus" -> "a + 1",
        "b_str" -> "CAST(b as string)",
        "c_e" -> "c.e",
        "c" -> "c",
        "mp_d" -> "element_at(mp, 'key').d",
        "mp" -> "mp",
        "ls" -> "ls"),
      outerStruct,
      Seq(
        "a > 0",
        "c IS NOT NULL AND c.e > 0"
      )
    )

    val mapVal = new util.HashMap[Any, Any]()
    mapVal.put("key", Map("e" -> 7.0f, "d" -> 8L))

    val listVal = new util.ArrayList[Any]()
    listVal.add(Map("e" -> 2.3f, "d" -> 7L))

    val result =
      ctUtil.performSql(Map("a" -> 1, "b" -> 0.58, "c" -> Map("e" -> 3.6f, "d" -> 9L), "mp" -> mapVal, "ls" -> listVal))
    assert(result.isDefined, "Expect a non-filtered row")

    val filteredA =
      ctUtil.performSql(Map("a" -> 0, "b" -> 0.58, "c" -> Map("e" -> 3.6f, "d" -> 9L), "mp" -> mapVal, "ls" -> listVal))
    assert(filteredA.isEmpty, "Expect row to be filtered on a > 0 check")

    val filteredC =
      ctUtil.performSql(Map("a" -> 1, "b" -> 0.58, "c" -> Map("e" -> -3.6f, "d" -> 9L), "mp" -> mapVal, "ls" -> listVal))
    assert(filteredC.isEmpty, "Expect row to be filtered on c.e > 0 check")
  }
}
