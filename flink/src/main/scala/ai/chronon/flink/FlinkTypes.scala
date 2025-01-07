package ai.chronon.flink

import ai.chronon.api.{LongType, StructType}
import ai.chronon.online.{AvroCodec, AvroConversions}
import ai.chronon.online.KVStore.PutRequest
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Objects
import scala.reflect.ClassTag

// Note: The classes in this file are POJOs to allow us to safely evolve the schema without breaking Flink jobs.
// See: go/flink-pojo-types for details on what safe evolution is.
// Not all of these are currently persisted in Flink state (only AvroCodecOutput and MementoOutput) are but we
// stick to using Pojos for all our operators to be consistent and avoid future issues.

// Captures metadata related to the Kafka event that we want to track
class EventMetadata(var kafkaTimestamp: Option[Long]) {
  def this() = this(None)

  override def hashCode(): Int =
    Objects.hash(kafkaTimestamp)

  override def equals(other: Any): Boolean =
    other match {
      case e: EventMetadata =>
        kafkaTimestamp == e.kafkaTimestamp
      case _ => false
    }
}

// Trait to capture details of the event & its metadata. This object
// is what we serve up from the first Shepherd kroxy source operators into
// the Shepherd Flink app
sealed trait Event[T] {
  def metadata: EventMetadata
  def data: T
}


// Output emitted by the Spark expression evaluation operator
class SparkExprOutput(var metadata: EventMetadata, var data: Map[String, Any]) {
  def this() = this(new EventMetadata(None), Map.empty[String, Any])

  override def hashCode(): Int =
    Objects.hash(metadata, data)

  override def equals(other: Any): Boolean =
    other match {
      case s: SparkExprOutput =>
        metadata == s.metadata && data == s.data
      case _ => false
    }

  override def toString = s"SparkExprOutput($metadata, $data)"
}

// Output emitted by the AvroCodecFn operator
class AvroCodecOutput(var putRequest: PutRequest, var kafkaTimestamp: Option[Long]) {
  def this() = this(null, None)

  override def hashCode(): Int =
    Objects.hash(
      putRequest.keyBytes.deep,
      putRequest.valueBytes.deep,
      putRequest.dataset,
      putRequest.tsMillis,
      kafkaTimestamp
    )

  override def equals(other: Any): Boolean =
    other match {
      case o: AvroCodecOutput =>
        (putRequest.keyBytes.sameElements(o.putRequest.keyBytes)) &&
          (putRequest.valueBytes.sameElements(o.putRequest.valueBytes)) &&
          putRequest.dataset == o.putRequest.dataset &&
          putRequest.tsMillis == o.putRequest.tsMillis &&
          kafkaTimestamp == o.kafkaTimestamp
      case _ => false
    }
}
