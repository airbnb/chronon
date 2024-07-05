/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.api.{DataType, StructType}
import ai.chronon.online.CatalystUtil.{IteratorWrapper, PoolKey, poolMap}
import ai.chronon.online.Extensions.StructTypeOps
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.{
  BufferedRowIterator,
  FilterExec,
  LocalTableScanExec,
  ProjectExec,
  RDDScanExec,
  WholeStageCodegenExec
}
import org.apache.spark.sql.{SparkSession, types}

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}
import java.util.function
import scala.collection.{Seq, mutable}

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
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class PoolKey(expressions: collection.Seq[(String, String)], inputSchema: StructType)
  val poolMap: PoolMap[PoolKey, CatalystUtil] = new PoolMap[PoolKey, CatalystUtil](pi =>
    new CatalystUtil(pi.inputSchema, pi.expressions))
}

class PoolMap[Key, Value](createFunc: Key => Value, maxSize: Int = 100, initialSize: Int = 2) {
  val map: ConcurrentHashMap[Key, ArrayBlockingQueue[Value]] = new ConcurrentHashMap[Key, ArrayBlockingQueue[Value]]()
  def getPool(input: Key): ArrayBlockingQueue[Value] =
    map.computeIfAbsent(
      input,
      new function.Function[Key, ArrayBlockingQueue[Value]] {
        override def apply(t: Key): ArrayBlockingQueue[Value] = {
          val result = new ArrayBlockingQueue[Value](maxSize)
          var i = 0
          while (i < initialSize) {
            result.add(createFunc(t))
            i += 1
          }
          result
        }
      }
    )

  def performWithValue[Output](key: Key, pool: ArrayBlockingQueue[Value])(func: Value => Output): Output = {
    var value = pool.poll()
    if (value == null) {
      value = createFunc(key)
    }
    try {
      func(value)
    } catch {
      case e: Exception => throw e
    } finally {
      pool.offer(value)
    }
  }
}

class PooledCatalystUtil(expressions: collection.Seq[(String, String)], inputSchema: StructType) {
  private val poolKey = PoolKey(expressions, inputSchema)
  private val cuPool = poolMap.getPool(PoolKey(expressions, inputSchema))
  def performSql(values: Map[String, Any]): Option[Map[String, Any]] =
    poolMap.performWithValue(poolKey, cuPool) { _.performSql(values) }
  def outputChrononSchema: Array[(String, DataType)] =
    poolMap.performWithValue(poolKey, cuPool) { _.outputChrononSchema }
}

// This class by itself it not thread safe because of the transformBuffer
class CatalystUtil(inputSchema: StructType, selects: Seq[(String, String)], wheres: Seq[String] = Seq.empty) {
  private val selectClauses = selects.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  private val whereClauseOpt = Option(wheres)
    .filter(_.nonEmpty)
    .map { w =>
      s"${w.mkString(" AND ")}"
    }

  private val (transformFunc: (InternalRow => Option[InternalRow]), outputSparkSchema: types.StructType) = initialize()
  @transient lazy val outputChrononSchema = SparkConversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)
  @transient lazy val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(inputSparkSchema)
  private val inputArrEncoder = SparkInternalRowConversions.to(inputSparkSchema, false)
  private lazy val outputArrDecoder = SparkInternalRowConversions.from(outputSparkSchema, false)

  def performSql(values: Array[Any]): Option[Array[Any]] = {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRowOpt = transformFunc(internalRow)
    val outputVal = resultRowOpt.map(resultRow => outputArrDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Array[Any]])
  }

  def performSql(values: Map[String, Any]): Option[Map[String, Any]] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    performSql(internalRow)
  }

  def performSql(row: InternalRow): Option[Map[String, Any]] = {
    val resultRowMaybe = transformFunc(row)
    val outputVal = resultRowMaybe.map(resultRow => outputDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Map[String, Any]])
  }

  def getOutputSparkSchema: types.StructType = outputSparkSchema

  private def initialize(): (InternalRow => Option[InternalRow], types.StructType) = {
    val session = CatalystUtil.session

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val df = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)
    val filteredDf = whereClauseOpt.map(df.where(_)).getOrElse(df)

    // extract transform function from the df spark plan
    val func: InternalRow => Option[InternalRow] = filteredDf.queryExecution.executedPlan match {
      case whc: WholeStageCodegenExec => {
        val (ctx, cleanedSource) = whc.doCodeGen()
        val (clazz, _) = CodeGenerator.compile(cleanedSource)
        val references = ctx.references.toArray
        val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
        val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
        buffer.init(0, Array(iteratorWrapper))
        def codegenFunc(row: InternalRow): Option[InternalRow] = {
          iteratorWrapper.put(row)
          while (buffer.hasNext) {
            return Some(buffer.next())
          }
          None
        }
        codegenFunc
      }
      case ProjectExec(projectList, fp @ FilterExec(condition, child)) => {
        val unsafeProjection = UnsafeProjection.create(projectList, fp.output)

        def projectFunc(row: InternalRow): Option[InternalRow] = {
          val r = ScalaVersionSpecificCatalystHelper.evalFilterExec(row, condition, child.output)
          if (r)
            Some(unsafeProjection.apply(row))
          else
            None
        }

        projectFunc
      }
      case ProjectExec(projectList, childPlan) => {
        childPlan match {
          // This WholeStageCodegenExec case is slightly different from the one above as we apply a projection.
          case whc @ WholeStageCodegenExec(fp: FilterExec) =>
            val unsafeProjection = UnsafeProjection.create(projectList, childPlan.output)
            val (ctx, cleanedSource) = whc.doCodeGen()
            val (clazz, _) = CodeGenerator.compile(cleanedSource)
            val references = ctx.references.toArray
            val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
            val iteratorWrapper: IteratorWrapper[InternalRow] = new IteratorWrapper[InternalRow]
            buffer.init(0, Array(iteratorWrapper))
            def codegenFunc(row: InternalRow): Option[InternalRow] = {
              iteratorWrapper.put(row)
              while (buffer.hasNext) {
                return Some(unsafeProjection.apply(buffer.next()))
              }
              None
            }
            codegenFunc
          case _ =>
            val unsafeProjection = UnsafeProjection.create(projectList, childPlan.output)
            def projectFunc(row: InternalRow): Option[InternalRow] = {
              Some(unsafeProjection.apply(row))
            }
            projectFunc
        }
      }
      case ltse: LocalTableScanExec => {
        // Input `row` is unused because for LTSE, no input is needed to compute the output
        def projectFunc(row: InternalRow): Option[InternalRow] =
          ltse.executeCollect().headOption

        projectFunc
      }
      case rddse: RDDScanExec => {
        val unsafeProjection = UnsafeProjection.create(rddse.schema)
        def projectFunc(row: InternalRow): Option[InternalRow] =
          Some(unsafeProjection.apply(row))

        projectFunc
      }
      case unknown => throw new RuntimeException(s"Unrecognized stage in codegen: ${unknown.getClass}")
    }

    (func, df.schema)
  }
}
