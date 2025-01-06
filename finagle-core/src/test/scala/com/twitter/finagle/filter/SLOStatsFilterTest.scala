package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.finagle.param
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
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class SLOStatsFilterTest extends AnyFunSuite {

  private[this] val unitSvc: Service[Unit, Unit] = Service.mk { _ =>
    Future.Unit
  }

  private[this] val alwaysFailClassifier = named("AlwaysFail") {
    case ReqRep(_, _) => ResponseClass.NonRetryableFailure
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
    val service =
      mkService(statsReceiver, Stack.Params.empty + param.ResponseClassifier(alwaysFailClassifier))
    Await.result(service(()))
    assert(!statsReceiver.counters.contains(Seq("slo", "violations", "total")))
  }

  test("Does not create the filter if disabled") {
    val statsReceiver = new InMemoryStatsReceiver
    val service = mkService(
      statsReceiver,
      Stack.Params.empty + param.ResponseClassifier(alwaysFailClassifier) + SLOStatsFilter.Disabled)
    Await.result(service(()))

    assert(!statsReceiver.counters.contains(Seq("slo", "violations", "total")))
  }

  test("Creates the filter if configured") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      mkService(
        statsReceiver,
        Stack.Params.empty + param.ResponseClassifier(alwaysFailClassifier) + SLOStatsFilter
          .configured(SLODefinition("scope", 5.seconds)))
    Await.result(service(()))

    assert(statsReceiver.counters.contains(Seq("slo", "scope", "violations", "total")))
  }

  test("Records latency violation if latency is violated") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case _ => SLODefinition("scope", latency)
    }
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val filter = new SLOStatsFilter[Unit, Unit](
        requestToSLODefinition,
        responseClassifier = ResponseClassifier.Default,
        statsReceiver = statsReceiver,
        nowNanos = () => Time.now.inNanoseconds
      )

      val service: Service[Unit, Unit] = Service.mk { _ =>
        Future.Unit.delayed(latency + 1.second)(timer)
      }

      val res = filter((), service)

      tc.advance(latency + 1.second)
      timer.tick()

      Await.result(res)

      assert(statsReceiver.counters(Seq("scope", "violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("scope", "violations", "latency")) == 1)
      assert(!statsReceiver.counters.contains(Seq("scope", "violations", "failures")))
      assert(statsReceiver.counters(Seq("scope", "total")) == 1)
    }
  }

  test("Records failure violation if response is a failure according to classifier") {
    val latency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case _ => SLODefinition("scope", latency)
    }
    val filter = new SLOStatsFilter[Unit, Unit](
      requestToSLODefinition,
      responseClassifier = alwaysFailClassifier,
      statsReceiver = statsReceiver,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.Done
    }

    Await.result(filter((), service))

    assert(statsReceiver.counters(Seq("scope", "violations", "total")) == 1)
    assert(!statsReceiver.counters.contains(Seq("scope", "violations", "latency")))
    assert(statsReceiver.counters(Seq("scope", "violations", "failures")) == 1)
    assert(statsReceiver.counters(Seq("scope", "total")) == 1)
  }

  test(
    "Records latency and failure violation if violated, does not double-count total violations") {
    val latency = 5.seconds
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case _ => SLODefinition("scope", latency)
    }
    val statsReceiver = new InMemoryStatsReceiver
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val filter = new SLOStatsFilter[Unit, Unit](
        requestToSLODefinition,
        statsReceiver = statsReceiver,
        responseClassifier = alwaysFailClassifier,
        nowNanos = () => Time.now.inNanoseconds
      )

      val service: Service[Unit, Unit] = Service.mk { _ =>
        Future.Unit.delayed(latency + 1.second)(timer)
      }

      val res = filter((), service)

      tc.advance(latency + 1.second)
      timer.tick()

      Await.result(res)

      assert(statsReceiver.counters(Seq("scope", "violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("scope", "violations", "latency")) == 1)
      assert(statsReceiver.counters(Seq("scope", "violations", "failures")) == 1)
      assert(statsReceiver.counters(Seq("scope", "total")) == 1)
    }
  }

  test("Does not record violation if response is ignorable failure") {
    val latency = 5.seconds
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case _ => SLODefinition("scope", latency)
    }
    val statsReceiver = new InMemoryStatsReceiver

    val filter = new SLOStatsFilter[Unit, Unit](
      requestToSLODefinition,
      statsReceiver = statsReceiver,
      responseClassifier = alwaysFailClassifier,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.exception(new Failure("boom!", flags = FailureFlags.Ignorable))
    }

    intercept[Exception](Await.result(filter((), service)))
    assert(!statsReceiver.counters.contains(Seq("scope", "violations", "total")))
    assert(!statsReceiver.counters.contains(Seq("scope", "total")))
  }

  test("Does not record violation if response is interrupted failure") {
    val latency = 5.seconds
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case _ => SLODefinition("scope", latency)
    }
    val statsReceiver = new InMemoryStatsReceiver

    val filter = new SLOStatsFilter[Unit, Unit](
      requestToSLODefinition,
      statsReceiver = statsReceiver,
      responseClassifier = ResponseClassifier.Default,
      nowNanos = () => Time.now.inNanoseconds
    )

    val service: Service[Unit, Unit] = Service.mk { _ =>
      Future.exception(new Failure("boom!", flags = FailureFlags.Interrupted))
    }

    intercept[Exception](Await.result(filter((), service)))
    assert(!statsReceiver.counters.contains(Seq("scope", "violations", "total")))
    assert(statsReceiver.counters(Seq("scope", "total")) == 1)
  }

  test("Records latency violations according to requestToSLODefinition") {
    val fastLatency = 1.seconds
    val slowLatency = 5.seconds
    val statsReceiver = new InMemoryStatsReceiver
    val requestToSLODefinition: PartialFunction[Any, SLODefinition] = {
      case "fast" => SLODefinition("fast", fastLatency)
      case "slow" => SLODefinition("slow", slowLatency)
    }

    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val filter = new SLOStatsFilter[String, Unit](
        requestToSLODefinition,
        responseClassifier = ResponseClassifier.Default,
        statsReceiver = statsReceiver,
        nowNanos = () => Time.now.inNanoseconds
      )

      var delay = fastLatency + 1.second

      val service: Service[String, Unit] = Service.mk { _ =>
        Future.Unit.delayed(delay)(timer)
      }

      val res = filter("fast", service)

      tc.advance(delay)
      timer.tick()

      Await.result(res)

      assert(statsReceiver.counters(Seq("fast", "violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("fast", "violations", "latency")) == 1)
      assert(!statsReceiver.counters.contains(Seq("fast", "violations", "failures")))
      assert(statsReceiver.counters(Seq("fast", "total")) == 1)

      // Don't change the delay, we should not get any violations
      val res2 = filter("slow", service)

      tc.advance(delay)
      timer.tick()

      Await.result(res2)

      assert(!statsReceiver.counters.contains(Seq("slow", "violations", "total")))
      assert(!statsReceiver.counters.contains(Seq("slow", "violations", "latency")))
      assert(!statsReceiver.counters.contains(Seq("slow", "violations", "failures")))
      assert(statsReceiver.counters(Seq("slow", "total")) == 1)

      delay = slowLatency + 1.second
      val res3 = filter("slow", service)

      tc.advance(delay)
      timer.tick()

      Await.result(res3)

      assert(statsReceiver.counters(Seq("slow", "violations", "total")) == 1)
      assert(statsReceiver.counters(Seq("slow", "violations", "latency")) == 1)
      assert(!statsReceiver.counters.contains(Seq("slow", "violations", "failures")))
      assert(statsReceiver.counters(Seq("slow", "total")) == 2)

      // Make sure nothing explodes if there is no slo definition for this request.
      val res4 = filter("other", service)

      tc.advance(delay)
      timer.tick()

      Await.result(res4)
    }
  }
}
