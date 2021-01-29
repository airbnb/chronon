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
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CpcSketch], new CpcSketchKryoSerializer())
  }
}
