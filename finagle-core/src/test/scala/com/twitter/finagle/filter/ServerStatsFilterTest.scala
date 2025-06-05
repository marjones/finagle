package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Time
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context
import com.twitter.finagle.context.BackupRequest
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RetryContext
import org.scalatest.funsuite.AnyFunSuite

class ServerStatsFilterTest extends AnyFunSuite {
  test("Records handletime for a service") {
    Time.withCurrentTimeFrozen { ctl =>
      val inMemory = new InMemoryStatsReceiver
      val svc = Service.mk[Unit, Unit] { unit =>
        ctl.advance(5.microseconds)
        Future.never
      }
      val filter = new ServerStatsFilter[Unit, Unit](inMemory, Stopwatch.timeNanos)
      filter.andThen(svc)(())
      val expected = 5
      val actual = inMemory.stats(Seq("handletime_us"))(0)
      assert(actual == expected)
    }
  }

  test("Record retry stat when request is retry") {
    val stats = new InMemoryStatsReceiver
    val svc = Service.mk[Unit, Unit] { _ =>
      Future.Done
    }
    val filter = new ServerStatsFilter[Unit, Unit](stats)
    filter.andThen(svc)(())

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)

    RetryContext.withRetry {
      filter.andThen(svc)(())
    }

    assert(stats.counters(Seq("request_classification", "retry")) == 1)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)
    assert(stats.counters(Seq("request_classification", "total")) == 2)
  }

  test("Record requeue stat when request is requeue") {
    val stats = new InMemoryStatsReceiver
    val svc = Service.mk[Unit, Unit] { _ =>
      Future.Done
    }
    val filter = new ServerStatsFilter[Unit, Unit](stats)
    filter.andThen(svc)(())

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)

    Contexts.broadcast.let(context.Requeues, context.Requeues(0)) {
      filter.andThen(svc)(())
    }

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)

    Contexts.broadcast.let(context.Requeues, context.Requeues(2)) {
      filter.andThen(svc)(())
    }

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 1)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)
    assert(stats.counters(Seq("request_classification", "total")) == 3)
  }

  test("Record retry stat when request is backup") {
    val stats = new InMemoryStatsReceiver
    val svc = Service.mk[Unit, Unit] { _ =>
      Future.Done
    }
    val filter = new ServerStatsFilter[Unit, Unit](stats)
    filter.andThen(svc)(())

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)

    BackupRequest.let {
      filter.andThen(svc)(())
    }

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 1)
    assert(stats.counters(Seq("request_classification", "total")) == 2)
  }

  test("Record retry+requeue+backup stat when request is retry+requeue+backup") {
    val stats = new InMemoryStatsReceiver
    val svc = Service.mk[Unit, Unit] { _ =>
      Future.Done
    }
    val filter = new ServerStatsFilter[Unit, Unit](stats)
    filter.andThen(svc)(())

    assert(stats.counters(Seq("request_classification", "retry")) == 0)
    assert(stats.counters(Seq("request_classification", "requeue")) == 0)
    assert(stats.counters(Seq("request_classification", "backup")) == 0)

    Contexts.broadcast.let(context.Requeues, context.Requeues(1)) {
      BackupRequest.let {
        RetryContext.withRetry {
          filter.andThen(svc)(())
        }
      }
    }

    assert(stats.counters(Seq("request_classification", "retry")) == 1)
    assert(stats.counters(Seq("request_classification", "requeue")) == 1)
    assert(stats.counters(Seq("request_classification", "backup")) == 1)
    assert(stats.counters(Seq("request_classification", "total")) == 2)
  }
}
