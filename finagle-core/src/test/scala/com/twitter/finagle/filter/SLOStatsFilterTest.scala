package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.ResponseClassifier.named
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Return
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class SLOStatsFilterTest extends AnyFunSuite {

  private[this] val unitSvc: Service[Unit, Unit] = Service.mk { _ =>
    Future.Unit
  }

  private[this] def mkService(
    statsReceiver: StatsReceiver,
    params: Stack.Params,
    underlying: Service[Unit, Unit] = unitSvc
  ): Service[Unit, Unit] = {
    val factory = SLOStatsFilter.module
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(underlying)))
      .make(params + Stats(statsReceiver))
    Await.result(factory())
  }

  test("Does not create the filter if not configured") {
    val statsReceiver = new InMemoryStatsReceiver
    val service = mkService(statsReceiver, Stack.Params.empty)
    Await.result(service(()))
    assert(!statsReceiver.counters.contains(Seq("slo", "violations", "total")))
  }

  test("Does not create the filter if disabled") {
    val statsReceiver = new InMemoryStatsReceiver
    val service = mkService(statsReceiver, Stack.Params.empty + SLOStatsFilter.Disabled)
    Await.result(service(()))

    assert(!statsReceiver.counters.contains(Seq("slo", "violations", "total")))
  }

  test("Creates the filter if configured") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      mkService(statsReceiver, Stack.Params.empty + SLOStatsFilter.configured(5.seconds))
    Await.result(service(()))

    assert(statsReceiver.counters.contains(Seq("slo", "violations", "total")))
  }

  test("Records latency violation if latency is violated") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val filter = new SLOStatsFilter[Unit, Unit](
        statsReceiver = statsReceiver,
        latencyNanos = latency.inNanoseconds,
        responseClassifier = ResponseClassifier.Default,
        nowNanos = () => Time.now.inNanoseconds
      )

      val service: Service[Unit, Unit] = Service.mk { _ =>
        Future.Unit.delayed(latency + 1.second)(timer)
      }

      val res = filter((), service)

      tc.advance(latency + 1.second)
      timer.tick()

      Await.result(res)

      assert(statsReceiver.counters(Seq("violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("violations", "latency")) == 1)
      assert(statsReceiver.counters(Seq("violations", "failures")) == 0)
    }
  }

  test("Records failure violation if response is a failure according to classifier") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    val responseClassifier = named("SuccessIsFailure") {
      case ReqRep(_, Return(_)) => ResponseClass.NonRetryableFailure
    }

    val filter = new SLOStatsFilter[Unit, Unit](
      statsReceiver = statsReceiver,
      latencyNanos = latency.inNanoseconds,
      responseClassifier = responseClassifier,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.Done
    }

    Await.result(filter((), service))

    assert(statsReceiver.counters(Seq("violations", "total")) == 1)
    assert(statsReceiver.counters(Seq("violations", "latency")) == 0)
    assert(statsReceiver.counters(Seq("violations", "failures")) == 1)
  }

  test(
    "Records latency and failure violation if violated, does not double-count total violations") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val responseClassifier = named("SuccessIsFailure") {
        case ReqRep(_, Return(_)) => ResponseClass.NonRetryableFailure
      }
      val filter = new SLOStatsFilter[Unit, Unit](
        statsReceiver = statsReceiver,
        latencyNanos = latency.inNanoseconds,
        responseClassifier = responseClassifier,
        nowNanos = () => Time.now.inNanoseconds
      )

      val service: Service[Unit, Unit] = Service.mk { _ =>
        Future.Unit.delayed(latency + 1.second)(timer)
      }

      val res = filter((), service)

      tc.advance(latency + 1.second)
      timer.tick()

      Await.result(res)

      assert(statsReceiver.counters(Seq("violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("violations", "latency")) == 1)
      assert(statsReceiver.counters(Seq("violations", "failures")) == 1)
    }
  }

  test("Does not record violation if response is ignorable failure") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    val responseClassifier = named("SuccessIsFailure") {
      case ReqRep(_, Return(_)) => ResponseClass.NonRetryableFailure
    }

    val filter = new SLOStatsFilter[Unit, Unit](
      statsReceiver = statsReceiver,
      latencyNanos = latency.inNanoseconds,
      responseClassifier = responseClassifier,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.exception(new Failure("boom!", flags = FailureFlags.Ignorable))
    }

    intercept[Exception](Await.result(filter((), service)))
    assert(statsReceiver.counters(Seq("violations", "total")) == 0)
  }

  test("Does not record violation if response is interrupted failure") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver

    val filter = new SLOStatsFilter[Unit, Unit](
      statsReceiver = statsReceiver,
      latencyNanos = latency.inNanoseconds,
      responseClassifier = ResponseClassifier.Default,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.exception(new Failure("boom!", flags = FailureFlags.Interrupted))
    }

    intercept[Exception](Await.result(filter((), service)))
    assert(statsReceiver.counters(Seq("violations", "total")) == 0)
  }
}
