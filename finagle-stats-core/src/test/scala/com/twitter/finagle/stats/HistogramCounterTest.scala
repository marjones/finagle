package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.util.MockTimer
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class HistogramCounterTest extends AnyFunSuite {

  test("Records stat at given frequency") {
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer()
      val statsReceiver = new InMemoryStatsReceiver
      val histogramCounterFactory = new HistogramCounterFactory(timer, () => Time.now.inMillis)
      val histogramCounter =
        histogramCounterFactory(
          Seq("foo", "bar"),
          StatsFrequency.HundredMilliSecondly,
          statsReceiver
        )
      histogramCounter.incr(5)
      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")).isEmpty)
      histogramCounter.incr()

      tc.advance(100.millis)
      timer.tick()

      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(6))

      tc.advance(100.millis)
      timer.tick()

      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(6, 0))

      histogramCounter.incr(2)

      tc.advance(50.millis)
      timer.tick()

      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(6, 0))
      histogramCounter.incr(3)

      tc.advance(50.millis)
      timer.tick()

      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(6, 0, 5))
    }
  }

  test("Normalizes recorded stat to the elapsed time since last recording") {
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer()
      val statsReceiver = new InMemoryStatsReceiver
      val histogramCounterFactory = new HistogramCounterFactory(timer, () => Time.now.inMillis)
      val histogramCounter =
        histogramCounterFactory(
          Seq("foo", "bar"),
          StatsFrequency.HundredMilliSecondly,
          statsReceiver
        )
      histogramCounter.incr(5)
      histogramCounter.incr(10)
      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")).isEmpty)

      // Our task was slow to execute on the timer :(
      tc.advance(150.millis)
      timer.tick()

      // We have 15 requests in 1.5 windows, so 10 requests in 1 window.
      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(10))

      histogramCounter.incr(12)

      tc.advance(300.millis)
      timer.tick()

      // We have 12 requests in 3 windows, so 4 requests in 1 window.
      assert(statsReceiver.stats(Seq("foo", "bar", "hundredMilliSecondly")) == Seq(10, 4))
    }
  }

  test("Returns the same histogramCounter object for equivalent names") {
    val timer = new MockTimer()
    val metrics = Metrics.createDetached()
    val statsReceiver = new MetricsStatsReceiver(metrics)

    val histogramCounterFactory = new HistogramCounterFactory(timer, () => Time.now.inMillis)

    val histogramCounter1 =
      histogramCounterFactory(Seq("foo", "bar"), StatsFrequency.HundredMilliSecondly, statsReceiver)
    val histogramCounter2 =
      histogramCounterFactory(Seq("foo", "bar"), StatsFrequency.HundredMilliSecondly, statsReceiver)
    val histogramCounter3 =
      histogramCounterFactory(Seq("foo/bar"), StatsFrequency.HundredMilliSecondly, statsReceiver)

    val scopedStatsReceiver = statsReceiver.scope("foo")

    val histogramCounter4 =
      histogramCounterFactory(Seq("bar"), StatsFrequency.HundredMilliSecondly, scopedStatsReceiver)

    assert(histogramCounter1 eq histogramCounter2)
    assert(histogramCounter1 eq histogramCounter3)
    assert(histogramCounter1 eq histogramCounter4)
  }

  test("Doesn't schedule recording of stats after close is called") {
    Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer()
      assert(timer.tasks.size == 0)
      val histogramCounterFactory = new HistogramCounterFactory(timer, () => Time.now.inMillis)
      assert(timer.tasks.size == 1)
      histogramCounterFactory.close()
      tc.advance(100.millis)
      timer.tick()
      assert(timer.tasks.size == 0)
    }
  }
}
