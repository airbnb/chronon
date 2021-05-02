package ai.zipline.spark

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

class ZiplineKryoRegistrator extends KryoRegistrator {
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
      "org.apache.spark.sql.types.Metadata",
      "ai.zipline.spark.KeyWithHash",
      "ai.zipline.aggregator.windowing.BatchIr",
      "ai.zipline.fetcher.Fetcher$Request",
      "java.util.HashMap",
      "java.util.ArrayList",
      "java.util.HashSet",
      "org.apache.spark.sql.Row",
      "org.apache.spark.sql.catalyst.InternalRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRow",
      "org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema",
      "org.apache.spark.sql.types.StructField",
      "org.apache.spark.sql.types.StructType",
      "org.apache.spark.sql.types.LongType$", // dollar stands for case objects
      "org.apache.spark.sql.types.StringType",
      "org.apache.spark.sql.types.StringType$",
      "org.apache.spark.sql.types.IntegerType$",
      "org.apache.spark.sql.types.BinaryType",
      "org.apache.spark.sql.types.BinaryType$",
      "org.apache.spark.util.sketch.BitArray",
      "org.apache.spark.util.sketch.BloomFilterImpl",
      "scala.reflect.ManifestFactory$$anon$10",
      "scala.reflect.ClassTag$$anon$1"
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
