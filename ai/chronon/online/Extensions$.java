package ai.chronon.online;object Extensions {

        implicit class ChrononStructTypeOps(schema: api.StructType) {
                def catalogString: String = {
                        SparkConversions.fromChrononSchema(schema).catalogString
                        }
                }

        implicit class StructTypeOps(schema: StructType) {
                def pretty: String = {
                        val schemaTuples = schema.fields.map { field =>
                                field.dataType.simpleString -> field.name
                                }

                        // pad the first column so that the second column is aligned vertically
                                val padding = if (schemaTuples.isEmpty) 0 else schemaTuples.map(_._1.length).max
                        schemaTuples
                                .map {
                                case (typ, name) => s"  ${typ.padTo(padding, ' ')} : $name"
                                }
                                .mkString("\n")
                        }

                def toChrononSchema(name: String = null): api.StructType =
                        api.StructType.from(name, SparkConversions.toChrononSchema(schema))

                def toAvroSchema(name: String = null): Schema = AvroConversions.fromChrononSchema(toChrononSchema(name))

                def toAvroCodec(name: String = null): AvroCodec = new AvroCodec(toAvroSchema(name).toString())
                }
        }
