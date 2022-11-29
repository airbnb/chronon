package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.SparkConversions
import ai.chronon.online.test.CatalystUtil.IteratorWrapper
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util
import scala.collection.mutable
import scala.util.ScalaToJavaConversions.IteratorOps

// TODO: move these classes to online module. We would need to figure out how to relocate spark deps so that they don't
// pollute apps that depend on Chronon fetcher.
object InternalRowConversions {
  // the identity function
  private def id(x: Any): Any = x

  // recursively convert sparks byte array based internal row to chronon's fetcher result type (map[string, any])
  // The purpose of this class is to be used on fetcher output in a fetching context
  // we take a data type and build a function that operates on actual value
  // we want to build functions where we only branch at construction time, but not at function execution time.
  def from(dataType: types.DataType): Any => Any = {
    val unguardedFunc: Any => Any = dataType match {
      case types.MapType(keyType, valueType, _) =>
        val keyConverter = from(keyType)
        val valueConverter = from(valueType)

        def mapConverter(x: Any): Any = {
          val mapData = x.asInstanceOf[MapData]
          val result = new util.HashMap[Any, Any]()
          val size = mapData.numElements()
          val keys = mapData.keyArray()
          val values = mapData.valueArray()
          var idx = 0
          while (idx < size) {
            result.put(keyConverter(keys.get(idx, keyType)), valueConverter(values.get(idx, valueType)))
            idx += 1
          }
          result
        }

        mapConverter
      case types.ArrayType(elementType, _) =>
        val elementConverter = from(elementType)

        def arrayConverter(x: Any): Any = {
          val arrayData = x.asInstanceOf[ArrayData]
          val size = arrayData.numElements()
          val result = new util.ArrayList[Any](size)
          var idx = 0
          while (idx < size) {
            result.add(elementConverter(arrayData.get(idx, elementType)))
            idx += 1
          }
          result
        }

        arrayConverter
      case types.StructType(fields) =>
        val funcs = fields.map { _.dataType }.map { from }
        val types = fields.map { _.dataType }
        val names = fields.map { _.name }
        val size = funcs.length

        def structConverter(x: Any): Any = {
          val internalRow = x.asInstanceOf[InternalRow]
          val result = new mutable.HashMap[Any, Any]()
          var idx = 0
          while (idx < size) {
            val value = internalRow.get(idx, types(idx))
            result.put(names(idx), funcs(idx)(value))
            idx += 1
          }
          result.toMap
        }

        structConverter
      case types.StringType =>
        def stringConvertor(x: Any): Any = x.asInstanceOf[UTF8String].toString

        stringConvertor
      case _ => id
    }
    def guardedFunc(x: Any): Any = if (x == null) x else unguardedFunc(x)
    guardedFunc
  }

  // recursively convert fetcher result type - map[string, any] to internalRow.
  // The purpose of this class is to be used on fetcher output in a fetching context
  // we take a data type and build a function that operates on actual value
  // we want to build functions where we only branch at construction time, but not at function execution time.
  def to(dataType: types.DataType): Any => Any = {
    val unguardedFunc: Any => Any = dataType match {
      case types.MapType(keyType, valueType, _) =>
        val keyConverter = to(keyType)
        val valueConverter = to(valueType)

        def mapConverter(x: Any): Any = {
          val mapData = x.asInstanceOf[util.HashMap[Any, Any]]
          val keyArray: ArrayData = new GenericArrayData(
            mapData.entrySet().iterator().toScala.map(_.getKey).map(keyConverter).toArray)
          val valueArray: ArrayData = new GenericArrayData(
            mapData.entrySet().iterator().toScala.map(_.getValue).map(valueConverter).toArray)
          new ArrayBasedMapData(keyArray, valueArray)
        }

        mapConverter
      case types.ArrayType(elementType, _) =>
        val elementConverter = to(elementType)

        def arrayConverter(x: Any): Any = {
          val arrayData = x.asInstanceOf[util.ArrayList[Any]]
          new GenericArrayData(arrayData.iterator().toScala.map(elementConverter).toArray)
        }

        arrayConverter
      case types.StructType(fields) =>
        val funcs = fields.map { _.dataType }.map { to }
        val names = fields.map { _.name }

        def structConverter(x: Any): Any = {
          val structMap = x.asInstanceOf[Map[Any, Any]]
          val valueArr =
            names.iterator.zip(funcs.iterator).map { case (name, func) => structMap.get(name).map(func).orNull }.toArray
          new GenericInternalRow(valueArr)
        }

        structConverter
      case types.StringType =>
        def stringConvertor(x: Any): Any = { UTF8String.fromString(x.asInstanceOf[String]) }

        stringConvertor
      case _ => id
    }
    def guardedFunc(x: Any): Any = if (x == null) x else unguardedFunc(x)
    guardedFunc
  }
}

object CatalystUtil {
  val inputTable: String = "input_table"
  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }
}

class CatalystUtil(expressions: Map[String, String], inputSchema: StructType, session: SparkSession) {
  private val selectClauses = expressions.map { case (name, expr) => s"$expr as $name" }.mkString(", ")
  private val sessionTable = s"q${math.abs(selectClauses.hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  private val query = s"SELECT $selectClauses FROM $sessionTable"
  private val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
  private val (sparkSQLTransformerBuffer: BufferedRowIterator, outputSparkSchema: types.StructType) =
    initializeIterator(iteratorWrapper)
  private val outputDecoder = InternalRowConversions.from(outputSparkSchema)
  @transient lazy val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = InternalRowConversions.to(SparkConversions.fromChrononSchema(inputSchema))

  def performSql(values: Map[String, Any]): Map[String, Any] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    iteratorWrapper.put(internalRow)
    while (sparkSQLTransformerBuffer.hasNext) {
      val resultInternalRow = sparkSQLTransformerBuffer.next()
      val outputVal = outputDecoder(resultInternalRow)
      return Option(outputVal).map(_.asInstanceOf[Map[String, Any]]).orNull
    }
    null
  }

  private def initializeIterator(
      iteratorWrapper: IteratorWrapper[InternalRow]): (BufferedRowIterator, types.StructType) = {
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val outputSchema = session.sql(query).schema
    val logicalPlan = session.sessionState.sqlParser.parsePlan(query)
    val plan = session.sessionState.executePlan(logicalPlan)
    val executedPlan = plan.executedPlan
    val codeGenerator = executedPlan.asInstanceOf[WholeStageCodegenExec]
    val (ctx, cleanedSource) = codeGenerator.doCodeGen()
    val (clazz, _) = CodeGenerator.compile(cleanedSource)
    val references = ctx.references.toArray
    val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
    buffer.init(0, Array(iteratorWrapper))
    (buffer, outputSchema)
  }
}

class CatalystUtilTest extends TestCase {
  lazy val session = SparkSession
    .builder()
    .appName("catalyst_test")
    .master("local[*]")
    .getOrCreate()
  @Test
  def testCatalystUtil(): Unit = {
    val innerStruct = StructType("inner", Array(StructField("d", LongType), StructField("e", FloatType)))
    val ctUtil = new CatalystUtil(
      Map("a_plus" -> "a + 1",
          "b_str" -> "CAST(b as string)",
          "c_e" -> "c.e",
          "c" -> "c",
          "mp_d" -> "element_at(mp, 'key').d",
          "mp" -> "mp",
          "ls" -> "ls"),
      StructType(
        "root",
        Array(
          StructField("a", IntType),
          StructField("b", DoubleType),
          StructField("c", innerStruct),
          StructField("mp", MapType(StringType, innerStruct)),
          StructField("ls", ListType(innerStruct))
        )
      ),
      session
    )

    val mapVal = new util.HashMap[Any, Any]()
    mapVal.put("key", Map("e" -> 7.0f, "d" -> 8L))

    val listVal = new util.ArrayList[Any]()
    listVal.add(Map("e" -> 2.3f, "d" -> 7L))

    val result =
      ctUtil.performSql(Map("a" -> 1, "b" -> 0.58, "c" -> Map("e" -> 3.6f, "d" -> 9L), "mp" -> mapVal, "ls" -> listVal))
    val expected = Map("a_plus" -> 2,
                       "b_str" -> "0.58",
                       "c_e" -> 3.6f,
                       "c" -> Map("e" -> 3.6f, "d" -> 9L),
                       "mp_d" -> 8L,
                       "mp" -> mapVal,
                       "ls" -> listVal)
    assertEquals(expected, result)
  }
}
