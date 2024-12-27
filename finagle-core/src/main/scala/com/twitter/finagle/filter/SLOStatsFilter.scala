package com.twitter.finagle.filter

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.param
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Throw
import com.twitter.util.Try

private[twitter] object SLOStatsFilter {
  val role = Stack.Role("SLOStats")

  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  object Param {
    case class Configured(
      latency: Duration)
        extends Param

    case object Disabled extends Param

    implicit val param: Stack.Param[SLOStatsFilter.Param] = Stack.Param(Disabled)
  }

  val Disabled: Param = Param.Disabled

  def configured(latency: Duration): Param = {
    Param.Configured(latency)
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[param.Stats, param.ResponseClassifier, Param, ServiceFactory[Req, Rep]] {
      val role = SLOStatsFilter.role
      val description =
        "Record number of SLO violations of underlying service"
      override def make(
        _stats: param.Stats,
        _responseClassifier: param.ResponseClassifier,
        params: Param,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        params match {
          case Param.Disabled => next
          case Param.Configured(latency) =>
            val param.Stats(statsReceiver) = _stats
            val param.ResponseClassifier(responseClassifier) = _responseClassifier
            new SLOStatsFilter(
              statsReceiver.scope("slo"),
              latency.inNanoseconds,
              responseClassifier).andThen(next)
        }
      }
    }
}

/**
 * A [[com.twitter.finagle.Filter]] that records the number of slo violations from the underlying
 * service. A request is classified as violating the slo if any of the following occur:
 * - The response returns after `latency` duration has elapsed
 * - The response is classified as a failure according to the ResponseClassifier (but is not
 *   ignorable or interrupted)
 */
private[finagle] class SLOStatsFilter[Req, Rep](
  statsReceiver: StatsReceiver,
  latencyNanos: Long,
  responseClassifier: ResponseClassifier,
  nowNanos: () => Long = Stopwatch.systemNanos)
    extends SimpleFilter[Req, Rep] {

  private[this] val violationsScope = statsReceiver.scope("violations")
  private[this] val violationsTotalCounter = violationsScope.counter("total")
  private[this] val violationsFailuresCounter = violationsScope.counter("failures")
  private[this] val violationsLatencyCounter = violationsScope.counter("latency")

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val start = nowNanos()
    service(request).respond { response =>
      if (!isIgnorable(response)) {
        var violated = false
        if (nowNanos() - start > latencyNanos) {
          violated = true
          violationsLatencyCounter.incr()
        }

        if (isFailure(request, response)) {
          violated = true
          violationsFailuresCounter.incr()
        }

        if (violated) {
          violationsTotalCounter.incr()
        }
      }
    }
  }

  private[this] def isFailure(request: Req, response: Try[Rep]): Boolean = {
    responseClassifier
      .applyOrElse(ReqRep(request, response), ResponseClassifier.Default) match {
      case ResponseClass.Failed(_) if !isInterrupted(response) => true
      case _ => false
    }
  }

  private[this] def isIgnorable(response: Try[Rep]): Boolean = response match {
    case Throw(f: FailureFlags[_]) => f.isFlagged(FailureFlags.Ignorable)
    case _ => false
  }

  private[this] def isInterrupted(response: Try[Rep]): Boolean = response match {
    case Throw(f: FailureFlags[_]) => f.isFlagged(FailureFlags.Interrupted)
    case _ => false
  }
}
