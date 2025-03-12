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

package ai.chronon.api

import ai.chronon.api.DataType.toTDataType
import ai.chronon.api.Extensions.WindowUtils

import scala.collection.Seq
import scala.util.ScalaJavaConversions._

// mostly used by tests to define confs easily
object Builders {

  object Selects {
    def apply(clauses: String*): Map[String, String] = {
      clauses.map { col => col -> col }.toMap
    }

    def exprs(clauses: (String, String)*): Map[String, String] = {
      clauses.map {
        case (col, expr) => col -> expr
      }.toMap
    }
  }

  object Query {
    def apply(selects: Map[String, String] = null,
              wheres: Seq[String] = null,
              startPartition: String = null,
              endPartition: String = null,
              timeColumn: String = null,
              setups: Seq[String] = null,
              mutationTimeColumn: String = null,
              reversalColumn: String = null,
              partitionColumn: String = null): Query = {
      val result = new Query()
      if (selects != null)
        result.setSelects(selects.toJava)
      if (wheres != null)
        result.setWheres(wheres.toJava)
      result.setStartPartition(startPartition)
      result.setEndPartition(endPartition)
      result.setTimeColumn(timeColumn)
      if (setups != null)
        result.setSetups(setups.toJava)
      result.setMutationTimeColumn(mutationTimeColumn)
      result.setReversalColumn(reversalColumn)
      result.setPartitionColumn(partitionColumn)
      result
    }
  }

  object AggregationPart {
    def apply(operation: Operation,
              inputColumn: String,
              window: Window = WindowUtils.Unbounded,
              argMap: Map[String, String] = null,
              bucket: String = null): AggregationPart = {
      val result = new AggregationPart()
      result.setOperation(operation)
      result.setInputColumn(inputColumn)
      result.setWindow(window)
      if (argMap != null)
        result.setArgMap(argMap.toJava)
      if (bucket != null) {
        result.setBucket(bucket)
      }
      result
    }
  }

  object Aggregation {
    def apply(operation: Operation,
              inputColumn: String,
              windows: Seq[Window] = null,
              argMap: Map[String, String] = null,
              buckets: Seq[String] = null): Aggregation = {
      val result = new Aggregation()
      result.setOperation(operation)
      result.setInputColumn(inputColumn)
      if (argMap != null)
        result.setArgMap(argMap.toJava)
      if (windows != null)
        result.setWindows(windows.toJava)
      if (buckets != null)
        result.setBuckets(buckets.toJava)
      result
    }
  }

  object Source {
    def entities(query: Query,
                 snapshotTable: String,
                 mutationTable: String = null,
                 mutationTopic: String = null): Source = {
      val result = new EntitySource()
      result.setQuery(query)
      result.setSnapshotTable(snapshotTable)
      result.setMutationTable(mutationTable)
      result.setMutationTopic(mutationTopic)
      val source = new Source()
      source.setEntities(result)
      source
    }

    def events(query: Query, table: String, topic: String = null, isCumulative: Boolean = false): Source = {
      val result = new EventSource()
      result.setQuery(query)
      result.setTable(table)
      result.setTopic(topic)
      result.setIsCumulative(isCumulative)
      val source = new Source()
      source.setEvents(result)
      source
    }

    def joinSource(join: Join, query: Query): Source = {
      val joinSource = new JoinSource()
      joinSource.setJoin(join)
      joinSource.setQuery(query)
      val source = new Source()
      source.setJoinSource(joinSource)
      source
    }
  }

  object GroupBy {
    def apply(
        metaData: MetaData = null,
        sources: Seq[Source] = null,
        keyColumns: Seq[String] = null,
        aggregations: Seq[Aggregation] = null,
        accuracy: Accuracy = null,
        derivations: Seq[Derivation] = null,
        backfillStartDate: String = null
    ): GroupBy = {
      val result = new GroupBy()
      result.setMetaData(metaData)
      if (sources != null)
        result.setSources(sources.toJava)
      if (keyColumns != null)
        result.setKeyColumns(keyColumns.toJava)
      if (aggregations != null)
        result.setAggregations(aggregations.toJava)
      if (accuracy != null)
        result.setAccuracy(accuracy)
      if (backfillStartDate != null)
        result.setBackfillStartDate(backfillStartDate)
      if (derivations != null)
        result.setDerivations(derivations.toJava)
      result
    }
  }

  object Join {
    def apply(metaData: MetaData = null,
              left: Source = null,
              joinParts: Seq[JoinPart] = null,
              externalParts: Seq[ExternalPart] = null,
              labelPart: LabelPart = null,
              bootstrapParts: Seq[BootstrapPart] = null,
              rowIds: Seq[String] = null,
              derivations: Seq[Derivation] = null,
              skewKeys: Map[String, Seq[String]] = null): Join = {
      val result = new Join()
      result.setMetaData(metaData)
      result.setLeft(left)
      if (joinParts != null)
        result.setJoinParts(joinParts.toJava)
      if (externalParts != null)
        result.setOnlineExternalParts(externalParts.toJava)
      if (labelPart != null)
        result.setLabelPart(labelPart)
      if (bootstrapParts != null)
        result.setBootstrapParts(bootstrapParts.toJava)
      if (rowIds != null)
        result.setRowIds(rowIds.toJava)
      if (derivations != null)
        result.setDerivations(derivations.toJava)
      if (skewKeys != null)
        result.setSkewKeys(skewKeys.mapValues(_.toJava).toMap.toJava)
      result
    }
  }

  object ExternalSource {
    def apply(metadata: MetaData, keySchema: DataType, valueSchema: DataType): ExternalSource = {
      val result = new ExternalSource()
      result.setMetadata(metadata)
      result.setKeySchema(toTDataType(keySchema))
      result.setValueSchema(toTDataType(valueSchema))
      result
    }
  }

  object ContextualSource {
    def apply(fields: Array[StructField]): ExternalSource = {
      val result = new ExternalSource
      result.setMetadata(MetaData(name = Constants.ContextualSourceName))
      result.setKeySchema(toTDataType(StructType(Constants.ContextualSourceKeys, fields)))
      result.setValueSchema(toTDataType(StructType(Constants.ContextualSourceValues, fields)))
    }
  }

  object ExternalPart {
    def apply(
        externalSource: ExternalSource,
        keyMapping: Map[String, String] = null,
        prefix: String = null
    ): ExternalPart = {
      val result = new ExternalPart()
      result.setSource(externalSource)
      if (keyMapping != null)
        result.setKeyMapping(keyMapping.toJava)
      result.setPrefix(prefix)
      result
    }
  }

  object LabelPart {
    def apply(labels: Seq[JoinPart] = null, leftStartOffset: Int = 0, leftEndOffset: Int = 0): LabelPart = {
      val result = new LabelPart()
      result.setLeftStartOffset(leftStartOffset)
      result.setLeftEndOffset(leftEndOffset)
      if (labels != null)
        result.setLabels(labels.toJava)
      result
    }
  }

  object JoinPart {
    def apply(
        groupBy: GroupBy = null,
        keyMapping: Map[String, String] = null,
        prefix: String = null
    ): JoinPart = {
      val result = new JoinPart()
      result.setGroupBy(groupBy)
      if (keyMapping != null)
        result.setKeyMapping(keyMapping.toJava)
      result.setPrefix(prefix)
      result
    }
  }

  object MetaData {
    def apply(
        name: String = null,
        online: Boolean = false,
        production: Boolean = false,
        customJson: String = null,
        dependencies: Seq[String] = null,
        namespace: String = null,
        team: String = null,
        samplePercent: Double = 100,
        consistencySamplePercent: Double = 5,
        tableProperties: Map[String, String] = Map.empty,
        historicalBackill: Boolean = true,
        deprecationDate: String = null
    ): MetaData = {
      val result = new MetaData()
      result.setName(name)
      result.setOnline(online)
      result.setProduction(production)
      result.setCustomJson(customJson)
      result.setOutputNamespace(namespace)
      result.setTeam(Option(team).getOrElse("chronon"))
      result.setHistoricalBackfill(historicalBackill)
      if (dependencies != null)
        result.setDependencies(dependencies.toSeq.toJava)
      if (samplePercent > 0)
        result.setSamplePercent(samplePercent)
      if (consistencySamplePercent > 0)
        result.setConsistencySamplePercent(consistencySamplePercent)
      if (tableProperties.nonEmpty)
        result.setTableProperties(tableProperties.toJava)
      if (deprecationDate != null)
        result.setDeprecationDate(deprecationDate)
      result
    }
  }

  object StagingQuery {
    def apply(
        query: String = null,
        metaData: MetaData = null,
        startPartition: String = null,
        setups: Seq[String] = null
    ): StagingQuery = {
      val stagingQuery = new StagingQuery()
      stagingQuery.setQuery(query)
      stagingQuery.setMetaData(metaData)
      stagingQuery.setStartPartition(startPartition)
      if (setups != null) stagingQuery.setSetups(setups.toJava)
      stagingQuery
    }
  }

  object BootstrapPart {
    def apply(
        query: Query = null,
        table: String = null,
        keyColumns: Seq[String] = null,
        metaData: MetaData = null
    ): BootstrapPart = {
      val bootstrapPart = new BootstrapPart()
      bootstrapPart.setQuery(query)
      bootstrapPart.setTable(table)
      Option(keyColumns)
        .map(_.toSeq.toJava)
        .foreach(bootstrapPart.setKeyColumns)
      bootstrapPart.setMetaData(metaData)
      bootstrapPart
    }
  }

  object Derivation {
    def apply(
        name: String = null,
        expression: String = null
    ): Derivation = {
      val derivation = new Derivation()
      if (name != null) {
        derivation.setName(name)
      }
      if (derivation != null) {
        derivation.setExpression(expression)
      }
      derivation
    }
  }

}
