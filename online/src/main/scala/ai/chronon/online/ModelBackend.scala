/*
 *    Copyright (C) 2025 The Chronon Authors.
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

import ai.chronon.api.{Join, Model}

import scala.collection.Seq
import scala.concurrent.Future

case class RunModelInferenceRequest(model: Model, inputs: Seq[Map[String, AnyRef]])
case class RunModelInferenceResponse(request: RunModelInferenceRequest, outputs: Seq[Map[String, AnyRef]])

trait ModelBackend {

  def runModelInference(runModelInferenceRequest: RunModelInferenceRequest): Future[RunModelInferenceResponse]
  // Run online model inference which returns the model output directly.
  // Will be used in fetcher.fetchJoin
}
