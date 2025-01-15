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

package ai.chronon.spark

import ai.chronon.api.ThriftJsonCodec
import org.apache.thrift.TBase
import org.rogach.scallop._

import scala.reflect.ClassTag

class Args(args: Seq[String]) extends ScallopConf(args) {
  val confPath: ScallopOption[String] = opt[String](required = true)
  val endDate: ScallopOption[String] = opt[String](required = false)
  val stepDays: ScallopOption[Int] = opt[Int](required = false) // doesn't apply to uploads
  val skipEqualCheck: ScallopOption[Boolean] =
    opt[Boolean](required = false, default = Some(false)) // only applies to join job for versioning
  def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
    ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)

  override def toString(): String = {
    s"""
       |confPath = $confPath
       |endDate = $endDate
       |stepDays = $stepDays
       |skipEqualCheck = $skipEqualCheck""".stripMargin
  }
}
