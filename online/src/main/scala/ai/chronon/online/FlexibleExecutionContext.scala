package ai.chronon.online

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object FlexibleExecutionContext {
  def buildExecutor: ThreadPoolExecutor =
    new ThreadPoolExecutor(20, // corePoolSize
                           1000, // maxPoolSize
                           600, // keepAliveTime
                           TimeUnit.SECONDS, // keep alive time units
                           new ArrayBlockingQueue[Runnable](1000))
  def buildExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(buildExecutor)
}
