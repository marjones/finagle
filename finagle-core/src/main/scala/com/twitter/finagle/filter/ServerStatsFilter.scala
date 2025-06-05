package com.twitter.finagle.filter

import com.twitter.finagle.context.Deadline
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.param
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.context.BackupRequest
import com.twitter.finagle.context.Requeues
import com.twitter.finagle.context.RetryContext
import com.twitter.util.Time
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import java.util.concurrent.TimeUnit

private[finagle] object ServerStatsFilter {
  val role = Stack.Role("ServerStats")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.filter.ServerStatsFilter]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Stats, ServiceFactory[Req, Rep]] {
      val role = ServerStatsFilter.role
      val description =
        "Record elapsed execution time, transit latency, deadline budget, of underlying service"
      def make(_stats: param.Stats, next: ServiceFactory[Req, Rep]) = {
        val param.Stats(statsReceiver) = _stats
        if (statsReceiver.isNull) next
        else new ServerStatsFilter(statsReceiver).andThen(next)
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that records the elapsed execution
 * times of the underlying [[com.twitter.finagle.Service]], in addition to classifying requests
 * as a retry, requeue, or backup.
 *
 * @note the stat does not include the time that it takes to satisfy
 *       the returned `Future`, only how long it takes for the `Service`
 *       to return the `Future`.
 */
private[finagle] class ServerStatsFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  nowNanos: () => Long)
    extends SimpleFilter[Req, Rep] {
  def this(statsReceiver: StatsReceiver) = this(statsReceiver, Stopwatch.systemNanos)

  private[this] val handletime = statsReceiver.stat("handletime_us")
  private[this] val transitTimeStat = statsReceiver.stat("transit_latency_ms")

  // We use a new scope here for clarity. We record "total" for two reasons:
  // 1. This filter is separate from the StatsFilter, which records /requests after requests have
  //    completed --  meaning /total and /requests can be different.
  // 2. /requests *excludes* requests that have been superceded by a backup or have their response
  //    flagged with FailureFlags.Ignorable (for accounting with success/failures). Here, we want to
  //    have an exact measure of requests so we can see the true proportion of retries/requeues/backups.
  private[this] val requestClassificationScope = statsReceiver.scope("request_classification")
  private[this] val backupCounter = requestClassificationScope.counter("backup")
  private[this] val requeueCounter = requestClassificationScope.counter("requeue")
  private[this] val retryCounter = requestClassificationScope.counter("retry")
  private[this] val totalCounter = requestClassificationScope.counter("total")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    totalCounter.incr()
    if (BackupRequest.wasInitiated) {
      backupCounter.incr()
    }

    if (Requeues.isRequeue) {
      requeueCounter.incr()
    }

    if (RetryContext.isRetry) {
      retryCounter.incr()
    }

    val startAt = nowNanos()

    Deadline.current match {
      case Some(deadline) =>
        val now = Time.now
        transitTimeStat.add((now - deadline.timestamp).max(Duration.Zero).inMillis)
      case None =>
    }

    try service(request)
    finally {
      val elapsedNs = nowNanos() - startAt
      handletime.add(TimeUnit.MICROSECONDS.convert(elapsedNs, TimeUnit.NANOSECONDS))
    }
  }
}
