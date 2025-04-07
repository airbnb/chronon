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

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object FlexibleExecutionContext {
  // users can also provide a custom execution context override in [MetadataStore]
  def buildExecutor(corePoolSize: Int = 20,
                    maxPoolSize: Int = 1000,
                    keepAliveTime: Int = 600,
                    keepAliveTimeUnit: TimeUnit = TimeUnit.SECONDS,
                    workQueue: BlockingQueue[Runnable] = new ArrayBlockingQueue[Runnable](1000)): ThreadPoolExecutor =
    new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, keepAliveTimeUnit, workQueue)

  // a helper method to tune execution context
  def buildExecutionContext(
      corePoolSize: Int = 20,
      maxPoolSize: Int = 1000,
      keepAliveTime: Int = 600,
      keepAliveTimeUnit: TimeUnit = TimeUnit.SECONDS,
      workQueue: BlockingQueue[Runnable] = new ArrayBlockingQueue[Runnable](1000)): ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      buildExecutor(corePoolSize = corePoolSize,
                    maxPoolSize = maxPoolSize,
                    keepAliveTime = keepAliveTime,
                    keepAliveTimeUnit = keepAliveTimeUnit,
                    workQueue = workQueue))
}
