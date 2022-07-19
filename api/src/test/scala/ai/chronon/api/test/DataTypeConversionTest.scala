package ai.chronon.api.test

import ai.chronon.api._
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TSimpleJSONProtocol
import org.junit.Assert._
import org.junit.Test

class DataTypeConversionTest {
  @Test
  def testDataTypeToThriftAndBack(): Unit = {
    // build some complex type
    val dType = StructType(
      "root",
      Array(
        StructField("map", MapType(
          StructType("key", Array(
            StructField("a", IntType),
            StructField("b", FloatType)
          )),
          StructType("value", Array(
            StructField("c", StructType("inner",
              Array(StructField("d", IntType)))))
            )
          )
        )))
    val thriftType = DataType.toTDataType(dType)

    // serialize with TSimpleJson - this is what python code will do
    val jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory())
    val json = new String(jsonSerializer.serialize(thriftType))
    println(json)

    val reversedTType = ThriftJsonCodec.fromJsonStr[TDataType](json, check = true, classOf[TDataType])
    val reversed = DataType.fromTDataType(reversedTType)
    assertEquals(dType, reversed)
  }
}
