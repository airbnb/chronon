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

package ai.chronon.spark.streaming

import ai.chronon.api
import ai.chronon.api.Extensions.MetadataOps
import org.apache.spark.sql.SparkSession

object PushModeConfig {
  def resolveNotificationTopic(groupByConf: api.GroupBy, session: SparkSession): Option[String] = {
    val enabled = groupByConf.getMetaData.customJsonLookUp("enable_write_notifications") match {
      case b: java.lang.Boolean => b.booleanValue()
      case _                    => false
    }
    if (enabled) {
      val topic = Option(groupByConf.getMetaData.customJsonLookUp("notification_topic_override"))
        .map(_.toString)
        .orElse(Option(session.conf.get("spark.chronon.stream.push.default_notification_topic", null)))
        .getOrElse(
          throw new IllegalArgumentException(
            s"Push mode is enabled for GroupBy ${groupByConf.getMetaData.getName} but no notification topic is configured. " +
              "Set 'notification_topic_override' in customJson or Spark config 'spark.chronon.stream.push.default_notification_topic'."
          )
        )
      Some(topic)
    } else None
  }
}
