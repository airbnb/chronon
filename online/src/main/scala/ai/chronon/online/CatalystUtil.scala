package ai.chronon.online

import ai.chronon.api.StructType
import ai.chronon.online.CatalystUtil.IteratorWrapper
import ai.chronon.online.Extensions.StructTypeOps
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.{SparkSession, types}

import scala.collection.mutable

object CatalystUtil {
  val inputTable: String = "input_table"
  private class IteratorWrapper[T] extends Iterator[T] {
    def put(elem: T): Unit = elemArr.enqueue(elem)

    override def hasNext: Boolean = elemArr.nonEmpty

    override def next(): T = elemArr.dequeue()

    private val elemArr: mutable.Queue[T] = mutable.Queue.empty[T]
  }

  lazy val session: SparkSession = SparkSession
    .builder()
    .appName(s"catalyst_test_${Thread.currentThread().toString}")
    .master("local[*]")
    .getOrCreate()

}

class CatalystUtil(expressions: Map[String, String], inputSchema: StructType) {
  private val selectClauses = expressions.map { case (name, expr) => s"$expr as $name" }.mkString(", ")
  private val sessionTable = s"q${math.abs(selectClauses.hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  private val query = s"SELECT $selectClauses FROM $sessionTable"
  private val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
  private val (sparkSQLTransformerBuffer: BufferedRowIterator, outputSparkSchema: types.StructType) =
    initializeIterator(iteratorWrapper)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)
  @transient lazy val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(SparkConversions.fromChrononSchema(inputSchema))

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
    val session = CatalystUtil.session
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
