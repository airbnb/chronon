package ai.chronon.spark;object LogUtils {
        def buildLogSchemaGroupBy(logTable: String,
                name: Option[String] = Some("logging_schema.v1"),
                namespace: Option[String] = None,
                team: Option[String] = Some("chronon"),
                backfillStartDate: Option[String] = None): ai.chronon.api.GroupBy = {
                val groupBy = Builders.GroupBy(
                        keyColumns = Seq(s"${Constants.SchemaHash}"),
                        sources = Seq(
                                Builders.Source.events(
                                        table = logTable,
                                        query = Builders.Query(
                                                selects = Builders.Selects.exprs(
                                                        Constants.SchemaHash -> "decode(unbase64(key_base64), 'utf-8')",
                                                        "schema_value" -> "decode(unbase64(value_base64), 'utf-8')"
                                                        ),
                                                wheres = Seq(s"name='${Constants.SchemaPublishEvent}'"),
                                                timeColumn = "ts_millis"
                                                )
                                        )
                                ),
                        aggregations = Seq(
                                Builders.Aggregation(
                                        inputColumn = "schema_value",
                                        operation = Operation.LAST
                                        )
                                ),
                        accuracy = Accuracy.SNAPSHOT,
                        metaData = Builders.MetaData()
                        )

                name.foreach(name => groupBy.metaData.setName(name))
                namespace.foreach(namespace => groupBy.metaData.setOutputNamespace(namespace))
                team.foreach(team => groupBy.metaData.setTeam(team))
                backfillStartDate.foreach(backfillStartDate => groupBy.setBackfillStartDate(backfillStartDate))

                groupBy
                }
        }
