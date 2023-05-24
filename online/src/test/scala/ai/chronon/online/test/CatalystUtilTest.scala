package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.CatalystUtil
import junit.framework.TestCase
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util

trait CatalystUtilTestSparkSQLStructs {
  import org.apache.spark.sql.types._

  val CommonScalarsSSS: StructType =
    StructType(
      StructField("bool_x", BooleanType, nullable = false) ::
        StructField("int32_x", IntegerType, nullable = false) ::
        StructField("int64_x", LongType, nullable = false) ::
        StructField("float64_x", DoubleType, nullable = false) ::
        StructField("string_x", StringType, nullable = false) ::
        StructField("bytes_x", BinaryType, nullable = false) :: Nil
    )

  val CommonScalarsRow: Map[String, Any] = Map(
    "bool_x" -> true,
    "int32_x" -> Int.MaxValue,
    "int64_x" -> Long.MaxValue,
    "float64_x" -> Double.MaxValue,
    "string_x" -> "hello",
    "bytes_x" -> "world".getBytes()
  )

  val CommonScalarsOptionalSSS: StructType =
    StructType(
      StructField("bool_x", BooleanType, nullable = true) ::
        StructField("int32_x", IntegerType, nullable = true) ::
        StructField("int64_x", LongType, nullable = true) ::
        StructField("float64_x", DoubleType, nullable = true) ::
        StructField("string_x", StringType, nullable = true) ::
        StructField("bytes_x", BinaryType, nullable = true) :: Nil
    )

  val CommonScalarsNullRow: Map[String, Any] = Map(
    "bool_x" -> null,
    "int32_x" -> null,
    "int64_x" -> null,
    "float64_x" -> null,
    "string_x" -> null,
    "bytes_x" -> null
  )

  val NestedInnerSSS: StructType = StructType(
    StructField("int32_req", IntegerType, nullable = false) :: StructField(
      "int32_opt",
      IntegerType,
      nullable = true
    ) :: Nil
  )

  val NestedOuterSSS: StructType = StructType(
    StructField("inner_req", NestedInnerSSS, nullable = false) :: StructField(
      "inner_opt",
      NestedInnerSSS,
      nullable = true
    ) :: Nil
  )

  val NestedRow: Map[String, Any] = Map(
    "inner_req" -> Map("int32_req" -> 12, "int32_opt" -> 34),
    "inner_opt" -> Map("int32_req" -> 56, "int32_opt" -> 78)
  )

  val NestedNullRow: Map[String, Any] = Map(
    "inner_req" -> Map("int32_req" -> 12, "int32_opt" -> null),
    "inner_opt" -> null
  )

  val ListContainersSSS: StructType = StructType(
    StructField("bools", ArrayType(BooleanType, containsNull = false), nullable = false) ::
      StructField("int32s", ArrayType(IntegerType, containsNull = false), nullable = false) ::
      StructField("int64s", ArrayType(LongType, containsNull = false), nullable = false) ::
      StructField("float64s", ArrayType(DoubleType, containsNull = false), nullable = false) ::
      StructField("strings", ArrayType(StringType, containsNull = false), nullable = false) ::
      StructField("bytess", ArrayType(BinaryType, containsNull = false), nullable = false) :: Nil
  )

  def makeArrayList(vals: Any*): util.ArrayList[Any] =
    new util.ArrayList[Any](util.Arrays.asList(vals: _*))

  val ListContainersRow: Map[String, Any] = Map(
    "bools" -> makeArrayList(false, true, false),
    "int32s" -> makeArrayList(1, 2, 3),
    "int64s" -> makeArrayList(4L, 5L, 6L),
    "float64s" -> makeArrayList(7.7, 8.7, 9.9),
    "strings" -> makeArrayList("hello", "world"),
    "bytess" -> makeArrayList("hello".getBytes(), "world".getBytes())
  )

  val MapContainersSSS: StructType = StructType(
    StructField(
      "bools",
      MapType(IntegerType, BooleanType, valueContainsNull = false),
      nullable = false
    ) ::
      StructField(
        "int32s",
        MapType(IntegerType, IntegerType, valueContainsNull = false),
        nullable = false
      ) ::
      StructField(
        "int64s",
        MapType(IntegerType, LongType, valueContainsNull = false),
        nullable = false
      ) ::
      StructField(
        "float64s",
        MapType(StringType, DoubleType, valueContainsNull = false),
        nullable = false
      ) ::
      StructField(
        "strings",
        MapType(StringType, StringType, valueContainsNull = false),
        nullable = false
      ) ::
      StructField(
        "bytess",
        MapType(StringType, BinaryType, valueContainsNull = false),
        nullable = false
      ) :: Nil
  )

  def makeHashMap(kvs: (Any, Any)*): util.HashMap[Any, Any] = {
    val m = new util.HashMap[Any, Any]()
    kvs.foreach { case (k, v) => { m.put(k, v) } }
    m
  }

  val MapContainersRow: Map[String, Any] = Map(
    "bools" -> makeHashMap(1 -> false, 2 -> true, 3 -> false),
    "int32s" -> makeHashMap(1 -> 1, 2 -> 2, 3 -> 3),
    "int64s" -> makeHashMap(1 -> 4L, 2 -> 5L, 3 -> 6L),
    "float64s" -> makeHashMap("a" -> 7.7, "b" -> 8.7, "c" -> 9.9),
    "strings" -> makeHashMap("a" -> "hello", "b" -> "world"),
    "bytess" -> makeHashMap("a" -> "hello".getBytes(), "b" -> "world".getBytes())
  )

}

class OldCatalystUtilTest extends TestCase {

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
