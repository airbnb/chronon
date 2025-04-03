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

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object FlexibleExecutionContext {
  def buildExecutor(maxPoolSize: Int = 1000): ThreadPoolExecutor =
    new ThreadPoolExecutor(20, // corePoolSize
                           maxPoolSize, // maxPoolSize
                           600, // keepAliveTime
                           TimeUnit.SECONDS, // keep alive time units
                           new ArrayBlockingQueue[Runnable](1000))
  def buildExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(buildExecutor())

  def buildCustomExecutionContext(maxPoolSize: Int): ExecutionContextExecutor =
    ExecutionContext.fromExecutor(buildExecutor(maxPoolSize))
}
