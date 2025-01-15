package ai.chronon.spark;
object EncoderUtil {

        def apply(structType: StructType): Encoder[Row] = RowEncoder(structType)

        }
