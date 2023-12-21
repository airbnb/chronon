package ai.chronon.online

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{
  ArrayBlockingQueue,
  CancellationException,
  ExecutionException,
  Executors,
  Future,
  ScheduledExecutorService,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.function.LongSupplier
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

// ThreadFactory which creates named threads, copied from Twitter's
// https://twitter.github.io/util/docs/com/twitter/concurrent/NamedPoolThreadFactory.html
class NamedPoolThreadFactory(name: String, makeDaemons: Boolean) extends ThreadFactory {
  def this(name: String) = this(name, false)

  val group: ThreadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup, name)
  val threadNumber: AtomicInteger = new AtomicInteger(1)

  def newThread(r: Runnable): Thread = {
    val thread = new Thread(group, r, name + "-" + threadNumber.getAndIncrement())
    thread.setDaemon(makeDaemons)
    if (thread.getPriority != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY)
    }
    thread
  }
}

object FlexibleExecutionContext {

  private val longSupplier: LongSupplier = new LongSupplier {
    override def getAsLong: Long = System.currentTimeMillis()
  }

  def buildExecutor: ThreadPoolExecutor =
    new ThreadPoolExecutor(20, // corePoolSize
                           1000, // maxPoolSize
                           600, // keepAliveTime
                           TimeUnit.SECONDS, // keep alive time units
                           new ArrayBlockingQueue[Runnable](1000))

  def buildExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(buildExecutor)

  def buildInstrumentedExecutionContext(name: String,
                                        metrics: Metrics.Context,
                                        corePoolSize: Int = 20,
                                        maxPoolSize: Int = 1000,
                                        keepAliveTime: Int = 600,
                                        keepAliveTimeUnit: TimeUnit = TimeUnit.SECONDS,
                                        queue: ArrayBlockingQueue[Runnable] = new ArrayBlockingQueue[Runnable](1000)
                                       ): ExecutionContextExecutor = {
    val instrumentedThreadPoolExecutor = new InstrumentedThreadPoolExecutor(name,
      metrics, corePoolSize, maxPoolSize, keepAliveTime, keepAliveTimeUnit, queue, new NamedPoolThreadFactory(name, true), longSupplier)
    ExecutionContext.fromExecutor(instrumentedThreadPoolExecutor)
  }
}

/**
 * Thread pool executor used for emitting metrics about the performance and utilization of threads
 * @param name String used to name threads for easier debugging
 * @param metrics Chronon Metric.Context used for emitting metrics
 * @param corePoolSize Initial size of the thread pool
 * @param maxPoolSize Maximum number of threads in the thread pool
 * @param keepAliveTime Maximum time that threads can remain idle before being terminated
 * @param keepAliveTimeUnit TimeUnit for the keep alive time
 * @param queue Queue to use for holding tasks before they are executed
 * @param threadFactory Factory to use when creating new threads, we use NamedPoolThreadFactory
 * @param clock LongSupplier interface used for measuring task durations
 */
class InstrumentedThreadPoolExecutor(
  name: String,
  metrics: Metrics.Context,
  corePoolSize: Int,
  maxPoolSize: Int,
  keepAliveTime: Int,
  keepAliveTimeUnit: TimeUnit,
  queue: ArrayBlockingQueue[Runnable],
  threadFactory: ThreadFactory,
  clock: LongSupplier

) extends ThreadPoolExecutor(
    corePoolSize,
    maxPoolSize,
    keepAliveTime,
    keepAliveTimeUnit,
    queue,
    threadFactory
  ) {

  println(s"Setting up thread pool executor for: $name in chronon")

  // scheduler for reporting gauge values at intervals
  private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  scheduler.scheduleAtFixedRate(() => {
    reportGauges()
  }, 0, 10, TimeUnit.SECONDS)

  override def execute(command: Runnable): Unit =
    super.execute(new TrackingRunnable(clock, command))

  override protected def beforeExecute(t: Thread, r: Runnable): Unit =
    super.beforeExecute(t, r)

  override protected def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)
    val tr = r.asInstanceOf[TrackingRunnable]
    report(tr, extractThrowable(tr.delegate, t))
  }

  override protected def shutdown(): Unit = {
    scheduler.shutdown()
    super.shutdown()
  }

  private def reportGauges(): Unit = {
    metrics.gauge(s"$name.thread_pool_executor.active_count", getActiveCount.doubleValue())
    metrics.gauge(s"$name.thread_pool_executor.queue_size", getQueue.size().doubleValue())
    metrics.gauge(s"$name.thread_pool_executor.pool_size", getPoolSize.doubleValue())
  }

  private def report(r: TrackingRunnable, t: Throwable): Unit = {
    val result = if (t == null) "ok" else "error"

    // add queue times
    val queueTime = (r.getStartTime - r.creationTime).doubleValue()
    metrics.histogram(s"$name.thread_pool_executor.timer",
      queueTime, s"result:$result,event:queue_time_ms,outcome:complete")

    // add run times
    val runTime = (r.getCompleteTime - r.getStartTime).doubleValue()
    metrics.histogram(s"$name.thread_pool_executor.timer",
      runTime, s"result:$result,event:run_time_ms,outcome:complete")
  }

  private def extractThrowable(r: Runnable, t: Throwable): Throwable = {
    if (t != null || !r.isInstanceOf[Future[_]]) { // non-FutureTasks are accurately reported
      return t
    }
    // The afterExecute javadoc calls out that calls to the submit API may wrap track the exception
    // internally so we have to extract it from the future. We know that this Future would have
    // completed already since we are in the scope of the afterExecute method.
    try {
      r.asInstanceOf[Future[_]].get
      null
    } catch {
      case ce: CancellationException =>
        ce
      case ee: ExecutionException =>
        ee.getCause
      case _: InterruptedException =>
        // This will ignore/reset interrupted state, which means the code in afterExecute code will
        // not get run (and we won't report anything)
        Thread.currentThread.interrupt()
        null
    }
  }

  private class TrackingRunnable(val clock: LongSupplier, val delegate: Runnable) extends Runnable {
    private var startTime = 0L
    private var completeTime = 0L
    val creationTime: Long = clock.getAsLong

    override def run(): Unit = {
      startTime = clock.getAsLong
      try delegate.run()
      finally completeTime = clock.getAsLong
    }

    def getStartTime: Long = startTime

    def getCompleteTime: Long = completeTime
  }

  }
