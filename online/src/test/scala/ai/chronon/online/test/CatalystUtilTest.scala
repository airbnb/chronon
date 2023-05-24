package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.CatalystUtil
import junit.framework.TestCase
import org.junit.Assert.{assertEquals, assertTrue, assertArrayEquals}
import org.junit.Test

import java.util

trait CatalystUtilTestSparkSQLStructs {

  val CommonScalarsSSS: StructType =
    StructType(
   "CommonScalarsSSS", Array(  StructField("bool_x", BooleanType),
        StructField("int32_x", IntType),
        StructField("int64_x", LongType),
        StructField("float64_x", DoubleType),
        StructField("string_x", StringType),
        StructField("bytes_x", BinaryType))
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
      "CommonScalarsOptionalSSS", Array(   StructField("bool_x", BooleanType),
        StructField("int32_x", IntType),
        StructField("int64_x", LongType),
        StructField("float64_x", DoubleType),
        StructField("string_x", StringType),
        StructField("bytes_x", BinaryType))
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
    "NestedInnerSSS", Array(   StructField("int32_req", IntType), StructField(
      "int32_opt",
      IntType
    ))
  )

  val NestedOuterSSS: StructType = StructType(
    "NestedOuterSSS", Array(   StructField("inner_req", NestedInnerSSS), StructField(
      "inner_opt",
      NestedInnerSSS
    ))
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
    "ListContainersSSS", Array(   StructField("bools", ListType(BooleanType)),
      StructField("int32s", ListType(IntType)),
      StructField("int64s", ListType(LongType)),
      StructField("float64s", ListType(DoubleType)),
      StructField("strings", ListType(StringType)),
      StructField("bytess", ListType(BinaryType)))
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
    "MapContainersSSS", Array(   StructField(
      "bools",
      MapType(IntType, BooleanType)
    ),
      StructField(
        "int32s",
        MapType(IntType, IntType)
      ),
      StructField(
        "int64s",
        MapType(IntType, LongType)
      ),
      StructField(
        "float64s",
        MapType(StringType, DoubleType)
      ),
      StructField(
        "strings",
        MapType(StringType, StringType)
      ),
      StructField(
        "bytess",
        MapType(StringType, BinaryType)
      ))
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




class CatalystUtilTest   extends TestCase with CatalystUtilTestSparkSQLStructs {



  @Test
  def testSelectStarWithCommonScalarsShouldReturnAsIs():Unit= {
    val selects = Seq(
      "bool_x" -> "bool_x",
      "int32_x" -> "int32_x",
      "int64_x" -> "int64_x",
      "float64_x" -> "float64_x",
      "string_x" -> "string_x",
      "bytes_x" -> "bytes_x"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bool_x"),true)
    assertEquals(res.get("int32_x"),Int.MaxValue)
    assertEquals(res.get("int64_x"),Long.MaxValue)
    assertEquals(res.get("float64_x"),Double.MaxValue)
    assertEquals(res.get("string_x"),"hello")
    assertArrayEquals(res.get("bytes_x").asInstanceOf[Array[Byte]],"world".getBytes())
  }

  @Test
  def testMathWithCommonScalarsShouldFollowOrderOfOperations():Unit= {
    val selects = Seq(
      "a" -> "4 + 5 * 32 - 2",
      "b" -> "(int32_x - 1) / 6 * 3 + 7 % 3",
      "c" -> "(int64_x / int32_x) + 7 * 3"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,3)
    assertEquals(res.get("a"),162)
    assertEquals(res.get("b"),1073741824.0)
    assertEquals(res.get("c"),4294967319.0)
  }

  @Test
  def testCommonFunctionsWithCommonScalarsShouldWork():Unit= {
    val selects = Seq(
      "a" -> "ABS(CAST(-1.0 * `int32_x` + 1.5 AS LONG))",
      "b" -> "BASE64('Spark SQL')",
      "c" -> "CONCAT_WS(' ', string_x, CAST(`bytes_x` AS STRING), 'foobar')",
      "d" -> "CHR(ASCII('A'))",
      "e" -> "BINARY('hello')",
      "f" -> "IF(int32_x >= `int64_x`, 'a', 'b')",
      "g" -> "CASE WHEN int32_x >= `int64_x` THEN 'NO' WHEN int64_x > `int32_x` THEN 'YES' ELSE 'NO' END",
      "h" -> "UUID()",
      "i" -> "regEXP_Extract(`string_x`, 'e(.l)o$', 1)",
      "j" -> "Rand()",
      "k" -> "COALESCE(NULL, NULL, int32_x, NULL)"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,11)
    assertEquals(res.get("a"),2147483645L)
    assertEquals(res.get("b"),"U3BhcmsgU1FM")
    assertEquals(res.get("c"),"hello world foobar")
    assertEquals(res.get("d"),"A")
    assertArrayEquals(res.get("e").asInstanceOf[Array[Byte]],"hello".getBytes())
    assertEquals(res.get("f"),"b")
    assertEquals(res.get("g"),"YES")
    assertEquals(res.get("h").toString.length,36)
    assertEquals(res.get("i"),"ll")
    val j = res.get("j").toString.toFloat
    assertTrue(j >= 0.0f && j <1.0f)
    assertEquals(res.get("k"),Int.MaxValue)
  }

  @Test
  def testDatetimeWithCommonScalarsShouldWork():Unit= {
    val selects = Seq(
      "a" -> "FROM_UNIXTIME(int32_x)",
      "b" -> "CURRENT_TIMESTAMP()",
      "c" -> "DATE_TRUNC('HOUR', '2015-03-05T09:32:05.359')",
      "d" -> "DAY('2023-05-17')",
      "e" -> "DAYOFWEEK('2009-07-30')",
      "f" -> "DATE_PART('DAY', DATE_ADD('2016-07-30', 3))"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,6)
//    assertEquals(res.get("a"),"2038-01-19 03:14:07")
    assertTrue(res.get("b").isInstanceOf[java.lang.Long])
//    assertEquals(res.get("c"),1425546000000000L)
    assertEquals(res.get("d"),17)
    assertEquals(res.get("e"),5)
    assertEquals(res.get("f"),2)
  }

  @Test
  def testSimpleUdfsWithCommonScalarsShouldWork():Unit= {
    CatalystUtil.session.udf.register("bool_udf", (x: Boolean) => x ^ x)
    CatalystUtil.session.udf.register("INT32_UDF", (x: Int) => x - 1)
    CatalystUtil.session.udf.register("int64_UDF", (x: Long) => x - 1)
    CatalystUtil.session.udf.register("float64_udf", (x: Double) => x - 1.0)
    CatalystUtil.session.udf.register("string_udf", (x: String) => x + "123")
    CatalystUtil.session.udf.register("BYTES_UDF", (x: Array[Byte]) => x ++ x)
    val selects = Seq(
      "bool_x" -> "BOOL_UDF(bool_x)",
      "int32_x" -> "INT32_udf(`int32_x`)",
      "int64_x" -> "int64_udF(int64_x)",
      "float64_x" -> "float64_udf(float64_x)",
      "string_x" -> "string_udf(`string_x`)",
      "bytes_x" -> "bytes_udf(bytes_x)"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bool_x"),false)
    assertEquals(res.get("int32_x"),Int.MaxValue - 1)
    assertEquals(res.get("int64_x"),Long.MaxValue - 1L)
    assertEquals(res.get("float64_x"),Double.MaxValue - 1.0f)
    assertEquals(res.get("string_x"),"hello123")
    assertArrayEquals(res.get("bytes_x").asInstanceOf[Array[Byte]],"worldworld".getBytes())
  }

  @Test
  def testComplexUdfsWithCommonScalarsShouldWork():Unit= {
    CatalystUtil.session.udf.register("two_param_udf", (x: Int, y: Long) => y - x)
    val add_one = (x: Int) => x + 1
    CatalystUtil.session.udf.register("add_two_udf", (x: Int) => add_one(add_one(x)))
    def fib(n: Int): Int =
      if (n <= 1) n else fib(n - 1) + fib(n - 2)
    CatalystUtil.session.udf.register("recursive_udf", (x: Int) => fib(x))
    val selects = Seq(
      "two_param_udf" -> "two_param_udf(int32_x, `int64_x`)",
      "add_two_udf" -> "add_two_udf(1)",
      "recursive_udf" -> "recursive_udf(8)"
    )
    val cu = new CatalystUtil(selects, CommonScalarsSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,3)
    assertEquals(res.get("two_param_udf"),Long.MaxValue - Int.MaxValue)
    assertEquals(res.get("add_two_udf"),3)
    assertEquals(res.get("recursive_udf"),21)
  }



  @Test
  def testDefinitelyFalseFilterWithCommonScalarsShouldReturnNone():Unit= {
    // aka. optimized False, LocalTableScanExec case
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("FALSE AND int64_x > `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertTrue(res.isEmpty)
  }

  @Test
  def testTrueFilterWithCommonScalarsShouldReturnData():Unit= {
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("FALSE OR int64_x > `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),Int.MaxValue)
  }

  @Test
  def testFalseFilterWithCommonScalarsShouldReturnNone():Unit= {
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("FALSE OR int64_x < `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertTrue(res.isEmpty)
  }

  @Test
  def testTrueFiltersWithCommonScalarsShouldReturnData():Unit= {
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("int64_x > `int32_x`", "FALSE OR int64_x > `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),Int.MaxValue)
  }

  @Test
  def testFalseFiltersWithCommonScalarsShouldReturnNone():Unit= {
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("int64_x > `int32_x`", "FALSE OR int64_x < `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertTrue(res.isEmpty)
  }

  @Test
  def testEmptySeqFiltersWithCommonScalarsShouldReturnData():Unit= {
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq()
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),Int.MaxValue)
  }

  @Test
  def testFunctionInFilterWithCommonScalarsShouldWork():Unit= {
    CatalystUtil.session.udf.register("sub_one", (x: Int) => x - 1)
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("COALESCE(NULL, NULL, int32_x, int64_x, NULL) = `int32_x`")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),Int.MaxValue)
  }

  @Test
  def testUdfInFilterWithCommonScalarsShouldWork():Unit= {
    CatalystUtil.session.udf.register("sub_one", (x: Int) => x - 1)
    val selects = Seq("a" -> "int32_x")
    val wheres = Seq("int32_x - 1 = SUB_ONE(int32_x)")
    val cu = new CatalystUtil(selects, CommonScalarsSSS, wheres)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),Int.MaxValue)
  }



  @Test
  def testSelectStarWithCommonOptionalScalarsShouldReturnAsIs():Unit= {
    val selects = Seq(
      "bool_x" -> "bool_x",
      "int32_x" -> "int32_x",
      "int64_x" -> "int64_x",
      "float64_x" -> "float64_x",
      "string_x" -> "string_x",
      "bytes_x" -> "bytes_x"
    )
    val cu = new CatalystUtil(selects, CommonScalarsOptionalSSS)
    val res = cu.performSql(CommonScalarsRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bool_x"),true)
    assertEquals(res.get("int32_x"),Int.MaxValue)
    assertEquals(res.get("int64_x"),Long.MaxValue)
    assertEquals(res.get("float64_x"),Double.MaxValue)
    assertEquals(res.get("string_x"),"hello")
    assertArrayEquals(res.get("bytes_x").asInstanceOf[Array[Byte]],"world".getBytes())
  }

  @Test
  def testSelectStarWithCommonOptionalScalarsNullShouldReturnNulls():Unit= {
    val selects = Seq(
      "bool_x" -> "bool_x",
      "int32_x" -> "int32_x",
      "int64_x" -> "int64_x",
      "float64_x" -> "float64_x",
      "string_x" -> "string_x",
      "bytes_x" -> "bytes_x"
    )
    val cu = new CatalystUtil(selects, CommonScalarsOptionalSSS)
    val res = cu.performSql(CommonScalarsNullRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bool_x") , null)
    assertEquals(res.get("int32_x") , null)
    assertEquals(res.get("int64_x") , null)
    assertEquals(res.get("float64_x") , null)
    assertEquals(res.get("string_x") , null)
    assertEquals(res.get("bytes_x") , null)
  }



  @Test
  def testSelectWithNestedShouldWork():Unit= {
    val selects = Seq(
      "inner_req" -> "inner_req",
      "inner_opt" -> "inner_opt",
      "inner_req_int32_req" -> "inner_req.int32_req",
      "inner_req_int32_opt" -> "inner_req.int32_opt",
      "inner_opt_int32_req" -> "inner_opt.int32_req",
      "inner_opt_int32_opt" -> "inner_opt.int32_opt"
    )
    val cu = new CatalystUtil(selects, NestedOuterSSS)
    val res = cu.performSql(NestedRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("inner_req"),Map("int32_req" -> 12, "int32_opt" -> 34))
    assertEquals(res.get("inner_opt"),Map("int32_req" -> 56, "int32_opt" -> 78))
    assertEquals(res.get("inner_req_int32_req"),12)
    assertEquals(res.get("inner_req_int32_opt"),34)
    assertEquals(res.get("inner_opt_int32_req"),56)
    assertEquals(res.get("inner_opt_int32_opt"),78)
  }

  @Test
  def testSelectWithNestedNullsShouldWork():Unit= {
    val selects = Seq(
      "inner_req" -> "inner_req",
      "inner_opt" -> "inner_opt",
      "inner_req_int32_req" -> "inner_req.int32_req",
      "inner_req_int32_opt" -> "inner_req.int32_opt"
    )
    val cu = new CatalystUtil(selects, NestedOuterSSS)
    val res = cu.performSql(NestedNullRow)
    assertEquals(res.get.size,4)
    assertEquals(res.get("inner_req"),Map("int32_req" -> 12, "int32_opt" -> null))
    assertEquals(res.get("inner_opt") , null)
    assertEquals(res.get("inner_req_int32_req"),12)
    assertEquals(res.get("inner_req_int32_opt") , null)
  }



  @Test
  def testSelectStarWithListContainersShouldReturnAsIs():Unit= {
    val selects = Seq(
      "bools" -> "bools",
      "int32s" -> "int32s",
      "int64s" -> "int64s",
      "float64s" -> "float64s",
      "strings" -> "strings",
      "bytess" -> "bytess"
    )
    val cu = new CatalystUtil(selects, ListContainersSSS)
    val res = cu.performSql(ListContainersRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bools"),makeArrayList(false, true, false))
    assertEquals(res.get("int32s"),makeArrayList(1, 2, 3))
    assertEquals(res.get("int64s"),makeArrayList(4L, 5L, 6L))
    assertEquals(res.get("float64s"),makeArrayList(7.7, 8.7, 9.9))
    assertEquals(res.get("strings"),makeArrayList("hello", "world"))
    val res_bytess = res.get("bytess").asInstanceOf[util.ArrayList[Any]]
    assertEquals(res_bytess.size,2)
    assertArrayEquals(res_bytess.get(0).asInstanceOf[Array[Byte]],"hello".getBytes())
    assertArrayEquals(res_bytess.get(1).asInstanceOf[Array[Byte]],"world".getBytes())
  }

  @Test
  def testIndexingWithListContainersShouldWork():Unit= {
    val selects = Seq(
      "a" -> "int64s[1] + int32s[2]"
    )
    val cu = new CatalystUtil(selects, ListContainersSSS)
    val res = cu.performSql(ListContainersRow)
    assertEquals(res.get.size,1)
    assertEquals(res.get("a"),8L)
  }

  @Test
  def testFunctionsWithListContainersShouldWork():Unit= {
    val selects = Seq(
      "a" -> "ARRAY(2, 4, 6)",
      "b" -> "ARRAY_REPEAT('123', 2)",
      "c" -> "AGGREGATE(`int32s`, 0, (acc, x) -> acc + x, acc -> acc * 10)",
      "d" -> "ARRAY_MIN(`int32s`)",
      "e" -> "CARDINALITY(int32s)"
    )
    val cu = new CatalystUtil(selects, ListContainersSSS)
    val res = cu.performSql(ListContainersRow)
    assertEquals(res.get.size,5)
    assertEquals(res.get("a"),makeArrayList(2, 4, 6))
    assertEquals(res.get("b"),makeArrayList("123", "123"))
    assertEquals(res.get("c"),60)
    assertEquals(res.get("d"),1)
    assertEquals(res.get("e"),3)
  }


  @Test
  def testSelectStarWithMapContainersShouldReturnAsIs():Unit= {
    val selects = Seq(
      "bools" -> "bools",
      "int32s" -> "int32s",
      "int64s" -> "int64s",
      "float64s" -> "float64s",
      "strings" -> "strings",
      "bytess" -> "bytess"
    )
    val cu = new CatalystUtil(selects, MapContainersSSS)
    val res = cu.performSql(MapContainersRow)
    assertEquals(res.get.size,6)
    assertEquals(res.get("bools"),makeHashMap(1 -> false, 2 -> true, 3 -> false))
    assertEquals(res.get("int32s"),makeHashMap(1 -> 1, 2 -> 2, 3 -> 3))
    assertEquals(res.get("int64s"),makeHashMap(1 -> 4L, 2 -> 5L, 3 -> 6L))
    assertEquals(res.get("float64s"),makeHashMap("a" -> 7.7, "b" -> 8.7, "c" -> 9.9))
    assertEquals(res.get("strings"),makeHashMap("a" -> "hello", "b" -> "world"))
    val res_bytess = res.get("bytess").asInstanceOf[util.HashMap[Any, Any]]
    assertEquals(res_bytess.size,2)
    assertArrayEquals(res_bytess.get("a").asInstanceOf[Array[Byte]],"hello".getBytes())
    assertArrayEquals(res_bytess.get("b").asInstanceOf[Array[Byte]],"world".getBytes())
  }

  @Test
  def testIndexingWithMapContainersShouldWork():Unit= {
    val selects = Seq(
      "a" -> "int32s[2]",
      "b" -> "strings['a']"
    )
    val cu = new CatalystUtil(selects, MapContainersSSS)
    val res = cu.performSql(MapContainersRow)
    assertEquals(res.get.size,2)
    assertEquals(res.get("a"),2)
    assertEquals(res.get("b"),"hello")
  }

  @Test
  def testFunctionsWithMapContainersShouldWork():Unit= {
    val selects = Seq(
      "a" -> "MAP(1, '2', 3, '4')",
      "b" -> "map_keys(int32s)",
      "c" -> "MAP_VALUES(strings)"
    )
    val cu = new CatalystUtil(selects, MapContainersSSS)
    val res = cu.performSql(MapContainersRow)
    assertEquals(res.get.size,3)
    assertEquals(res.get("a"),makeHashMap(1 -> "2", 3 -> "4"))
    assertEquals(res.get("b").asInstanceOf[util.ArrayList[Any]].size,3)
    assertTrue(res.get("b").asInstanceOf[util.ArrayList[Any]].contains(1))
    assertTrue(res.get("b").asInstanceOf[util.ArrayList[Any]].contains(2))
    assertTrue(res.get("b").asInstanceOf[util.ArrayList[Any]].contains(3))
    assertEquals(res.get("c").asInstanceOf[util.ArrayList[Any]].size,2)
    assertTrue(res.get("c").asInstanceOf[util.ArrayList[Any]].contains("hello"))
    assertTrue(res.get("c").asInstanceOf[util.ArrayList[Any]].contains("world"))
  }

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
