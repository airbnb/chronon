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

class FetchException(requestName: String, message: String)
    extends IllegalArgumentException(s"Failed to fetch $requestName: $message") {
  def getRequestName: String = requestName
}

case class KeyMissingException(requestName: String, missingKeys: Seq[String], query: Map[String, Any])
    extends FetchException(requestName, s"Missing keys $missingKeys required by $requestName in query: $query")

case class InvalidEntityException(requestName: String)
    extends FetchException(requestName, s"Invalid entity $requestName")

case class EncodeKeyException(requestName: String, message: String)
    extends IllegalArgumentException(s"Failed to encode key for $requestName: $message")
