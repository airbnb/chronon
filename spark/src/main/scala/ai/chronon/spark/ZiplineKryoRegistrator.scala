package ai.chronon.spark

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.yahoo.sketches.cpc.CpcSketch
import org.apache.spark.serializer.KryoRegistrator

class CpcSketchKryoSerializer extends Serializer[CpcSketch] {
  override def write(kryo: Kryo, output: Output, sketch: CpcSketch): Unit = {
    val bytes = sketch.toByteArray
    output.writeInt(bytes.size)
    output.writeBytes(sketch.toByteArray)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[CpcSketch]): CpcSketch = {
    val size = input.readInt()
    val bytes = input.readBytes(size)
    CpcSketch.heapify(bytes)
  }
}

class ChrononKryoRegistrator extends KryoRegistrator {
  // registering classes tells kryo to not send schema on the wire
  // helps shuffles and spilling to disk
  override def registerClasses(kryo: Kryo) {
    //kryo.setWarnUnregisteredClasses(true)
    val names = Seq(
      "org.apache.spark.sql.execution.joins.UnsafeHashedRelation",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableFileStatus",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex$SerializableBlockLocation",
      "org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage",
      "org.apache.spark.sql.execution.datasources.ExecutedWriteSummary",
      "org.apache.spark.sql.execution.datasources.BasicWriteTaskStats",
      "org.apache.spark.sql.execution.datasources.WriteTaskResult",
      "org.apache.spark.sql.execution.datasources.InMemoryFileIndex",
      "org.apache.spark.sql.execution.streaming.sources.ForeachWriterCommitMessage$",
      "org.apache.spark.sql.types.Metadata",
      "ai.chronon.spark.KeyWithHash",
      "ai.chronon.aggregator.windowing.BatchIr",
      "ai.chronon.online.Fetcher$Request",
      "ai.chronon.aggregator.windowing.FinalBatchIr",
      "ai.chronon.online.LoggableResponse",
      "ai.chronon.online.LoggableResponseBase64",
      "com.yahoo.sketches.kll.KllFloatsSketch",
      "java.util.HashMap",
      "java.util.ArrayList",
      "java.util.HashSet",
      "org.apache.spark.sql.Row",
      "org.apache.spark.sql.catalyst.InternalRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema",
      "org.apache.spark.sql.catalyst.expressions.UnsafeRow",
      "org.apache.spark.sql.types.StructField",
      "org.apache.spark.sql.types.StructType",
      "org.apache.spark.sql.types.LongType$", // dollar stands for case objects
      "org.apache.spark.sql.types.StringType",
      "org.apache.spark.sql.types.StringType$",
      "org.apache.spark.sql.types.IntegerType$",
      "org.apache.spark.sql.types.BinaryType",
      "org.apache.spark.sql.types.DoubleType$",
      "org.apache.spark.sql.types.BooleanType$",
      "org.apache.spark.sql.types.BinaryType$",
      "org.apache.spark.sql.types.DateType$",
      "org.apache.spark.sql.types.TimestampType$",
      "org.apache.spark.util.sketch.BitArray",
      "org.apache.spark.util.sketch.BloomFilterImpl",
      "scala.reflect.ManifestFactory$$anon$10",
      "scala.reflect.ClassTag$$anon$1",
      "scala.math.Ordering$$anon$4",
      "org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering",
      "org.apache.spark.sql.catalyst.expressions.SortOrder",
      "org.apache.spark.sql.catalyst.expressions.BoundReference",
      "org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$8",
      "org.apache.spark.sql.catalyst.trees.Origin",
      "org.apache.spark.sql.catalyst.expressions.Ascending$",
      "org.apache.spark.sql.catalyst.expressions.Descending$",
      "org.apache.spark.sql.catalyst.expressions.NullsFirst$",
      "org.apache.spark.sql.catalyst.expressions.NullsLast$",
      "org.apache.spark.sql.catalyst.InternalRow$$anonfun$getAccessor$5",
      "scala.collection.IndexedSeqLike$Elements"
    )
    names.foreach { name =>
      kryo.register(Class.forName(name))
      kryo.register(Class.forName(s"[L$name;")) // represents array of a type to jvm
    }
    kryo.register(classOf[Array[Array[Array[AnyRef]]]])
    kryo.register(classOf[Array[Array[AnyRef]]])
    kryo.register(classOf[CpcSketch], new CpcSketchKryoSerializer())
  }
}
