package com.twitter.finagle.filter

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Filter
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats.StatsReceiver
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
      requestToSLODefinition: PartialFunction[Any, SLODefinition])
        extends Param

    case object Disabled extends Param

    implicit val param: Stack.Param[SLOStatsFilter.Param] = Stack.Param(Disabled)
  }

  val Disabled: Param = Param.Disabled

  def configured(
    requestToSLODefinition: PartialFunction[Any, SLODefinition],
  ): Param = {
    Param.Configured(requestToSLODefinition)
  }

  def configured(SLODefinition: SLODefinition): Param = {
    Param.Configured({
      case _ => SLODefinition
    })
  }

  def typeAgnostic(
    statsReceiver: StatsReceiver,
    requestToSLODefinition: PartialFunction[Any, SLODefinition],
    responseClassifier: ResponseClassifier,
    nowNanos: () => Long = Stopwatch.systemNanos
  ): TypeAgnostic = new TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
      new SLOStatsFilter[Req, Rep](
        requestToSLODefinition,
        responseClassifier,
        statsReceiver,
        nowNanos)
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module3[
      com.twitter.finagle.param.Stats,
      com.twitter.finagle.param.ResponseClassifier,
      Param,
      ServiceFactory[Req, Rep]
    ] {
      val role = SLOStatsFilter.role
      val description =
        "Record number of SLO violations of underlying service"
      override def make(
        _stats: com.twitter.finagle.param.Stats,
        _responseClassifier: com.twitter.finagle.param.ResponseClassifier,
        params: Param,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        params match {
          case Param.Disabled => next
          case Param.Configured(requestToSLODefinition) =>
            val com.twitter.finagle.param.Stats(statsReceiver) = _stats
            val com.twitter.finagle.param.ResponseClassifier(responseClassifier) =
              _responseClassifier

            new SLOStatsFilter(
              requestToSLODefinition,
              responseClassifier,
              statsReceiver.scope("slo")).andThen(next)
        }
      }
    }
}

case class SLODefinition(scope: String, latency: Duration)

/**
 * A [[com.twitter.finagle.Filter]] that records the number of slo violations (as determined from
 * `requestToSLODefinition`) from the underlying service. A request is classified as violating the
 * slo if any of the following occur:
 * - The response returns after `latency` duration has elapsed
 * - The response is classified as a failure according to the ResponseClassifier (but is not
 *   ignorable or interrupted)
 */
class SLOStatsFilter[Req, Rep](
  requestToSLODefinition: PartialFunction[Any, SLODefinition],
  responseClassifier: ResponseClassifier,
  statsReceiver: StatsReceiver,
  nowNanos: () => Long = Stopwatch.systemNanos)
    extends SimpleFilter[Req, Rep] {

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val start = nowNanos()
    service(request).respond { response =>
      if (!isIgnorable(response)) {
        if (requestToSLODefinition.isDefinedAt(request)) {
          val sloDefinition = requestToSLODefinition(request)
          var violated = false
          if (nowNanos() - start > sloDefinition.latency.inNanoseconds) {
            violated = true
            statsReceiver.counter(sloDefinition.scope, "violations", "latency").incr()
          }

          if (isFailure(request, response)) {
            violated = true
            statsReceiver.counter(sloDefinition.scope, "violations", "failures").incr()
          }

          if (violated) {
            statsReceiver.counter(sloDefinition.scope, "violations", "total").incr()
          }

          statsReceiver.counter(sloDefinition.scope, "total").incr()
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
