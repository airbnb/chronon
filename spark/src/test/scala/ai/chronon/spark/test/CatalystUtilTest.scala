package ai.chronon.spark.test

import ai.chronon.api.{
  BinaryType,
  BooleanType,
  ByteType,
  DataType,
  DateType,
  DoubleType,
  FloatType,
  IntType,
  ListType,
  LongType,
  MapType,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType,
  UnknownType
}
import ai.chronon.spark.Conversions
import ai.chronon.spark.test.CatalystUtil.IteratorWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.{SparkSession, types}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util
import scala.collection.mutable

object CatalystUtil {
  val inputTable: String = "input_table"
  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }
}

class CatalystUtil(query: String, inputSchema: StructType) {
  private val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
  private val (sparkSQLTransformerBuffer: BufferedRowIterator, outputSparkSchema: types.StructType) =
    initializeIterator(iteratorWrapper)
  private val outputSchema = Conversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = internalConvert(outputSparkSchema)
  private val outputNames = outputSchema.map(_._1)
  def performSql(values: Map[String, Any]): Map[String, Any] = {
    val internalRow = valueMapToInternalRow(values)
    iteratorWrapper.put(internalRow)
    while (sparkSQLTransformerBuffer.hasNext) {
      val resultInternalRow = sparkSQLTransformerBuffer.next()
      val outputVal = outputDecoder(resultInternalRow)
      return Option(outputVal).map(_.asInstanceOf[Map[String, Any]]).orNull
    }
    null
  }

  // recursively convert sparks byte array based internal row to chronon's type system
  private def id(x: Any): Any = x
  private def internalConvert(dataType: types.DataType): Any => Any = {
    val unguardedFunc: Any => Any = dataType match {
      case types.MapType(keyType, valueType, _) => {
        val keyConverter = internalConvert(keyType)
        val valueConverter = internalConvert(valueType)
        def mapConverter(x: Any): Any = {
          val mapData = x.asInstanceOf[MapData]
          val result = new util.HashMap[Any, Any]()
          val size = mapData.numElements()
          val keys = mapData.keyArray()
          val values = mapData.valueArray()
          var idx = 0
          while (idx < size) {
            result.put(keyConverter(keys.get(idx, keyType)), valueConverter(values.get(idx, keyType)))
            idx += 1
          }
          result
        }
        mapConverter
      }
      case types.ArrayType(elementType, _) => {
        val elementConverter = internalConvert(elementType)
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
      }
      case types.StructType(fields) => {
        val funcs = fields.map { _.dataType }.map { internalConvert }
        val types = fields.map { _.dataType }
        val names = fields.map { _.name }
        val size = funcs.size
        def structConverter(x: Any): Any = {
          val internalRow = x.asInstanceOf[InternalRow]
          val result = new mutable.HashMap[Any, Any]()
          var idx = 0
          while (idx < size) {
            println(internalRow, idx)
            val value = internalRow.get(idx, types(idx))
            result.put(names(idx), funcs(idx)(value))
            idx += 1
          }
          result.toMap
        }
        structConverter
      }
      case _ => id
    }
    def guardedFunc(x: Any): Any = if (x == null) x else unguardedFunc(x)
    guardedFunc
  }

  private def valueMapToInternalRow(values: Map[String, Any]): InternalRow = {
    if (values == null) return null
    val size = inputSchema.fields.length
    val arr = new Array[Any](size)
    val it = inputSchema.fields.iterator
    var idx = 0
    while (it.hasNext) {
      val col = it.next().name
      arr.update(idx, values.get(col).orNull)
      idx += 1
    }
    InternalRow.fromSeq(arr)
  }

  private def initializeIterator(
      iteratorWrapper: IteratorWrapper[InternalRow]): (BufferedRowIterator, types.StructType) = {
    val s = SparkSession.builder.appName("Test").master("local").getOrCreate()
    val emptyRowRdd = s.emptyDataFrame.rdd
    val inputSparkSchema = Conversions.fromChrononSchema(inputSchema)
    val emptyDf = s.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(CatalystUtil.inputTable)
    val outputSchema = s.sql(query).schema
    val logicalPlan = s.sessionState.sqlParser.parsePlan(query)
    val plan = s.sessionState.executePlan(logicalPlan)
    val executedPlan = plan.executedPlan
    val codeGenerator = executedPlan.asInstanceOf[WholeStageCodegenExec]
    val (ctx, cleanedSource) = codeGenerator.doCodeGen()
    val (clazz, _) = CodeGenerator.compile(cleanedSource)
    val references = ctx.references.toArray
    val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
    s.close()
    buffer.init(0, Array(iteratorWrapper))
    (buffer, outputSchema)
  }
}

class CatalystUtilTest {

  @Test
  def testCatalystUtil(): Unit = {
    val innerStruct = StructType("inner", Array(StructField("d", LongType), StructField("e", FloatType)))
    val mapType = MapType(StringType, innerStruct)
    val listType =
    val util = new CatalystUtil(
      s"select a + 1 as a_plus, CAST(b as string) as b_str, c.e as c_e, c as c from ${CatalystUtil.inputTable} ",
      StructType("root",
                 Array(
                   StructField("a", IntType),
                   StructField("b", DoubleType),
                   StructField("c", innerStruct)
                 )))
    val result = util.performSql(Map("a" -> 1, "b" -> 0.58, "c" -> Array(9L, 5.0)))
    assertEquals(null, result)
  }
}
