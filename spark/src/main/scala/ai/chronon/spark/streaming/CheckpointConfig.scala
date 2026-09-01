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

package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import org.apache.spark.sql.SparkSession

object CheckpointConfig {

  // Opt-in: only set a stable, GroupBy-derived checkpoint location when the base dir is explicitly
  // configured - restarts (manual or retried) then resume from it with no setup. Left unset otherwise,
  // to avoid changing behavior for callers that haven't opted in.
  def location(groupByConf: api.GroupBy, session: SparkSession): Option[String] =
    session.conf.getOption("spark.chronon.stream.checkpoint_base_dir").map { baseDir =>
      s"$baseDir/${groupByConf.metaData.nameToFilePath}/${session.sparkContext.applicationId}/"
    }
}
