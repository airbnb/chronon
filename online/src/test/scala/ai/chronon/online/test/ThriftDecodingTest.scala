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

package ai.chronon.online.test

import ai.chronon.api.{Derivation, Join, StructType}
import ai.chronon.online.{CatalystUtil, SerializableFunction, SparkConversions, TBaseDecoderFactory}
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util

class ThriftDecodingTest {

  @Test
  def testJitneyDecoding(): Unit = {
    val tokens = new util.HashSet[String]()
    Seq("left", "source", "events", "derivations", "name", "expression")
      .foreach(tokens.add)
    val decoder = new TBaseDecoderFactory("ai.chronon.api.Join",
                                          tokens,
                                          new SerializableFunction[String, String] {
                                            //camel case to snake case
                                            override def apply(t: String): String =
                                              "[A-Z]".r.replaceAllIn(t, { m => "_" + m.group(0).toLowerCase() })
                                          }).build()
    val dks = buildJoin()
    val schema = decoder.dataType
    assertEquals(
      schema.toString,
      "StructType(StructField(left,StructType(StructField(events,StructType()))), StructField(derivations,ListType(StructType(StructField(name,StringType), StructField(expression,StringType)))))"
    )
    val gson = new Gson()
    // decoder will pull out data from the nested fields
    val json = gson.toJson(decoder.apply(dks))
    assertEquals(json,
                 """[null,[["derived_col1","group_by_aggregate_col"],["derived_col2","group_by_aggregate_col_345"]]]""")

    val (der_name, der_expr) =
      "derivations_avg_expr_length" ->
        ("aggregate(derivations, named_struct('sum', 0, 'count', 0)" +
          ", (acc, x) -> named_struct('sum', char_length(x.expression) + acc.sum, 'count', 1 + acc.count)" +
          ", acc -> acc.sum / acc.count)")

    // Test code to be stepped through in a debugger  - to figure out how spark catalyst is converting
    // sql query into transformation logic that we reverse engineer to create catalyst util
    //    val data_ = decoder.apply(dks)
    //    val schema_ = decoder.dataType.asInstanceOf[StructType]
    //    val spark = CatalystUtil.session
    //    val rdd: RDD[Row] =
    //      spark.sparkContext.parallelize(Seq(SparkConversions.toSparkRow(data_, schema).asInstanceOf[GenericRow]))
    //    val df = spark.createDataFrame(rdd, SparkConversions.fromChrononSchema(schema_))
    //    val computed = df.selectExpr(s"$der_expr as $der_name")
    //    computed.explain()
    //    val rows = computed.collect()(0).toSeq.toArray
    //    gson.toJson(rows)

    // apply sql on this
    val cu = new CatalystUtil(schema.asInstanceOf[StructType], collection.Seq(der_name -> der_expr))
    val result = cu.performSql(decoder.apply(dks).asInstanceOf[Array[Any]]).orNull
    val resultJson = gson.toJson(result)
    assertEquals(resultJson, "[24.0]")
  }

  // using join to
  def buildJoin(): Join = {
    val join = new Join();
    val derivations = new util.ArrayList[Derivation]()
    val derivation1 = new Derivation()
    derivation1.setName("derived_col1")
    derivation1.setExpression("group_by_aggregate_col")
    val derivation2 = new Derivation()
    derivation2.setName("derived_col2")
    derivation2.setExpression("group_by_aggregate_col_345")
    derivations.add(derivation1)
    derivations.add(derivation2)
    join.setDerivations(derivations)
  }

}
