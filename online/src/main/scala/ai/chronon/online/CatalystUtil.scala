package ai.chronon.online

import ai.chronon.api.{DataType, StructType}
import ai.chronon.online.CatalystUtil.{IteratorWrapper, PoolInput, poolMap}
import ai.chronon.online.Extensions.StructTypeOps
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.{BufferedRowIterator, ProjectExec, WholeStageCodegenExec}
import org.apache.spark.sql.{SparkSession, types}

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}
import java.util.function
import java.util.function.Supplier
import scala.collection.mutable

object CatalystUtil {
  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }

  lazy val session: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(s"catalyst_test_${Thread.currentThread().toString}")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class PoolInput(expressions: collection.Seq[(String, String)], inputSchema: StructType)
  val poolMap: Pool[PoolInput, CatalystUtil] = new Pool[PoolInput, CatalystUtil](pi => new CatalystUtil(pi.expressions, pi.inputSchema))
}

class Pool[Input, Entry](createFunc: Input => Entry, poolSize: Int = 100) {
  val poolMap: ConcurrentHashMap[Input, ArrayBlockingQueue[Entry]] = new ConcurrentHashMap[Input, ArrayBlockingQueue[Entry]]()
  def getPool(input: Input): ArrayBlockingQueue[Entry] = poolMap.computeIfAbsent(
    input, (t: Input) => {
      val result = new ArrayBlockingQueue[Entry](poolSize)
      var i = 0
      while(i < poolSize) {
        result.add(createFunc(t))
        i += 1
      }
      result
    }
  )

  def performWithEntry[Output](pool: ArrayBlockingQueue[Entry])(func: Entry => Output): Output = {
    val entry = pool.take()
    try {
      func(entry)
    } catch {
      case e: Exception => throw e
    } finally {
      pool.add(entry)
    }
  }
}

class PooledCatalystUtil(expressions: collection.Seq[(String, String)], inputSchema: StructType) {
  private val cuPool = poolMap.getPool(PoolInput(expressions, inputSchema))
  def performSql(values: Map[String, Any]): Map[String, Any] = poolMap.performWithEntry(cuPool) {_.performSql(values)}
  def outputChrononSchema: Array[(String, DataType)] = poolMap.performWithEntry(cuPool) { _.outputChrononSchema}
}

// This class by itself it not thread safe because of the transformBuffer
class CatalystUtil(expressions: collection.Seq[(String, String)], inputSchema: StructType) {
  private val selectClauses = expressions.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  private val (transformFunc: (InternalRow => InternalRow), outputSparkSchema: types.StructType) = initialize()
  @transient lazy val outputChrononSchema = SparkConversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)
  @transient lazy val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(inputSparkSchema)
  private val inputArrEncoder = SparkInternalRowConversions.to(inputSparkSchema, false)
  private lazy val outputArrDecoder = SparkInternalRowConversions.from(outputSparkSchema, false)

  def performSql(values: Array[Any]): Array[Any] = {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRow = transformFunc(internalRow)
    val outputVal = outputArrDecoder(resultRow)
    Option(outputVal).map(_.asInstanceOf[Array[Any]]).orNull
  }

  def performSql(values: Map[String, Any]): Map[String, Any] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    val resultRow = transformFunc(internalRow)
    val outputVal = outputDecoder(resultRow)
    Option(outputVal).map(_.asInstanceOf[Map[String, Any]]).orNull
  }

  private def initialize(): (InternalRow => InternalRow, types.StructType) = {
    val session = CatalystUtil.session

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val df = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)

    // extract transform function from the df spark plan
    val func: InternalRow => InternalRow = df.queryExecution.executedPlan match {
      case whc: WholeStageCodegenExec => {
        val (ctx, cleanedSource) = whc.doCodeGen()
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val references = ctx.references.toArray
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
        buffer.init(0, Array(iteratorWrapper))
        def codegenFunc(row: InternalRow): InternalRow = {
          iteratorWrapper.put(row)
          while (buffer.hasNext) {
            return buffer.next()
          }
          null
        }
        codegenFunc
      }
      case ProjectExec(projectList, childPlan) => {
        val unsafeProjection = UnsafeProjection.create(projectList, childPlan.output)
        def projectFunc(row: InternalRow): InternalRow = {
          unsafeProjection.apply(row)
        }
        projectFunc
      }

      case unknown => throw new RuntimeException(s"Unrecognized stage in codegen: ${unknown.getClass}")
    }

    (func, df.schema)
  }
}
