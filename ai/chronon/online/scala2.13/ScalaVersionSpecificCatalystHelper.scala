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

package ai.chronon.online

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Predicate}

import scala.collection.Seq

object ScalaVersionSpecificCatalystHelper {

  def evalFilterExec(row: InternalRow, condition: Expression, attributes: Seq[Attribute]): Boolean = {
    val predicate = Predicate.create(condition, attributes.toSeq)
    predicate.initialize(0)
    val r = predicate.eval(row)
    r
  }
}
