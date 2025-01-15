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

package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing.TsUtils
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.{Row, Window}

class NaiveAggregator(aggregator: RowAggregator,
                      windows: Array[Window],
                      tailHops: Array[Long],
                      headRoundingMillis: Long = 1)
    extends Serializable {

  def aggregate(inputRows: Seq[Row], queries: Seq[Long]): Array[Array[Any]] = {

    // initialize the result - convention is to append the timestamp in the end
    val results: Array[Array[Any]] = Array.fill(queries.length)(Array.fill(aggregator.length)(null))
    if (inputRows == null) return results
    for (inputRow <- inputRows) {
      for (endTimeIndex <- queries.indices) {
        val queryTime = queries(endTimeIndex)
        for (col <- aggregator.indices) {
          val windowStart = TsUtils.round(queryTime - windows(col).millis, tailHops(col))
          if (windowStart <= inputRow.ts && inputRow.ts < TsUtils.round(queryTime, headRoundingMillis)) {
            aggregator.columnAggregators(col).update(results(endTimeIndex), inputRow)
          }
        }
      }
    }
    results
  }
}
