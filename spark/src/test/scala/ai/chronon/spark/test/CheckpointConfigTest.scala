/*
 *    Copyright (C) 2026 The Chronon Authors.
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

package ai.chronon.spark.test

import ai.chronon.api.Builders
import ai.chronon.spark.SparkSessionBuilder
import ai.chronon.spark.streaming.CheckpointConfig
import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.Test

class CheckpointConfigTest {

  val spark: SparkSession = SparkSessionBuilder.build("CheckpointConfigTest", local = true)

  private val groupByConf =
    Builders.GroupBy(metaData = Builders.MetaData(name = "unit_test.checkpoint_config_gb"))

  @Test
  def locationIsUnsetWhenBaseDirIsNotConfigured(): Unit = {
    spark.conf.unset("spark.chronon.stream.checkpoint_base_dir")
    assertEquals(None, CheckpointConfig.location(groupByConf, spark))
  }

  @Test
  def locationIsDerivedFromBaseDirAndGroupByNameWhenConfigured(): Unit = {
    spark.conf.set("spark.chronon.stream.checkpoint_base_dir", "s3://bucket/checkpoints")
    assertEquals(Some("s3://bucket/checkpoints/unit_test/checkpoint_config_gb/"),
                 CheckpointConfig.location(groupByConf, spark))
    spark.conf.unset("spark.chronon.stream.checkpoint_base_dir")
  }
}
