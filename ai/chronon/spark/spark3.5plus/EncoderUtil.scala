object EncoderUtil{

        def apply(structType:StructType):Encoder[Row]=Encoders.row(structType)

        }
