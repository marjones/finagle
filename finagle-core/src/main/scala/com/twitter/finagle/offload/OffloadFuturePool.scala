package com.twitter.finagle.offload

import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Duration
import com.twitter.util.ExecutorServiceFuturePool
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Local
import java.util.concurrent.ExecutorService

/**
 * Custom FuturePool for offloading
 *
 * Instrumented to improve debuggability, and to work with offload admission control
 */
final class OffloadFuturePool(executor: ExecutorService, stats: StatsReceiver)
    extends ExecutorServiceFuturePool(executor) {
  // Reference held so GC doesn't clean these up automatically.
  private val gauges = Seq(
    stats.addGauge("pool_size") { poolSize },
    stats.addGauge("active_tasks") { numActiveTasks },
    stats.addGauge("completed_tasks") { numCompletedTasks },
    stats.addGauge("queue_depth") { numPendingTasks }
  )

  private[offload] val admissionControl: Option[OffloadFilterAdmissionControl] =
    OffloadFilterAdmissionControl(this, stats.scope("admission_control"))

  val hasAdmissionControl: Boolean = admissionControl.isDefined
}

final class PriorityOffloadFuturePool(
  normalPriorityPool: FuturePool,
  lowPriorityFuturePool: FuturePool)
    extends FuturePool {
  override def apply[T](f: => T): Future[T] = {
    if (OffloadFuturePool.lowPriorityLocal().isEmpty) {
      normalPriorityPool.apply(f)
    } else {
      lowPriorityFuturePool.apply(f)
    }
  }
}

object OffloadFuturePool {

  /**
   * A central `FuturePool` to use for your application work.
   *
   * If configured, this `FuturePool` is used by `OffloadFilter` to shift your application work
   * from the I/O threads to this pool. This has the benefit of dramatically increasing the
   * responsiveness of I/O work and also acts as a safety against long running or blocking work
   * that may be unknowingly scheduled on the I/O threads.
   *
   * This pool should be used only for non-blocking application work and preferably tasks
   * that are not expected to take a very long time to compute.
   */
  lazy val configuredPool: Option[FuturePool] = {
    val workers =
      numWorkers.get.orElse(if (auto()) Some(com.twitter.jvm.numProcs().ceil.toInt) else None)
    val maxQueueLen = maxQueueLength()

    val normalPriorityPool = workers.map { threads =>
      val stats = FinagleStatsReceiver.scope("offload_pool")
      createPool(OffloadThreadPool(threads, maxQueueLen, stats), stats)
    }

    val lowPriorityPool = lowPriorityNumWorkers.get.map { threads =>
      val stats = FinagleStatsReceiver.scope("low_priority_offload_pool")
      createPool(
        new DefaultThreadPoolExecutor(threads, maxQueueLen, stats, "finagle/low-priority-offload"),
        stats,
      )
    }

    normalPriorityPool.map { normalPool =>
      lowPriorityPool match {
        case Some(lowPriorityPool) => new PriorityOffloadFuturePool(normalPool, lowPriorityPool)
        case None => normalPool
      }
    }
  }

  private[this] def createPool(
    executor: ExecutorService,
    stats: StatsReceiver
  ): FuturePool = {
    val pool = new OffloadFuturePool(executor, stats)

    // Start sampling the offload delay if the interval isn't Duration.Top.
    if (statsSampleInterval().isFinite && statsSampleInterval() > Duration.Zero) {
      val sampleStats = new SampleQueueStats(pool, stats, DefaultTimer)
      sampleStats()
    }

    pool
  }

  /**
   * Get the configured offload pool if available or default to the unbounded [[FuturePool]].
   *
   * @note that the unbounded `FuturePool` can grow indefinitely, both in queue size and in terms
   *       of thread count and should be used with caution.
   */
  def getPool: FuturePool = configuredPool match {
    case Some(pool) => pool
    case None => FuturePool.unboundedPool
  }

  val lowPriorityLocal = new Local[Unit]

  def withLowPriorityOffloads[T](fn: => T): T = {
    lowPriorityLocal.let(())(fn)
  }
}
