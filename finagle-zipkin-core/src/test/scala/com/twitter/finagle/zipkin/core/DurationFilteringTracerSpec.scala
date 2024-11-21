package com.twitter.finagle.zipkin.core
package unit

import org.scalatest.FunSuite
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Futures
import com.twitter.util.Promise
import com.twitter.util.Return
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import scala.io.Source
import zipkin2.codec.SpanBytesDecoder

class DurationFilteringTracerSpec extends FunSuite with Eventually {

  implicit val config: PatienceConfig = PatienceConfig(timeout = scaled(Span(5, Seconds)))
  private val futurePool = FuturePool(Executors.newFixedThreadPool(100))
  private implicit val timer = DefaultTimer

  test("Test only persists spans with duration greater than 100 milliseconds") {
    val tempFile = File.createTempFile("traces", ".json")
    val tracer = new DurationFilteringTracer(100.millis, 1F, tempFile.getPath)

    Trace.letTracers(Seq(tracer)) {
      val future1 = Trace.traceLocalFuture("test") {
        taskWithDuration(10.millis)
      }

      val future2 = Trace.traceLocalFuture("test2") {
        taskWithDuration(100.millis)
      }

      Await.ready(Futures.join(future1, future2))
    }

    eventually {
      val parsedSpans = parseSpansFromFile(tempFile.getPath)
      assert(parsedSpans.size == 1)
      assert(parsedSpans.head.name() == "test2")
    }
  }

  test("Test persists all child spans where parent has duration greater than 100 milliseconds") {
    val tempFile = File.createTempFile("traces", ".json")
    val tracer = new DurationFilteringTracer(100.millis, 1F, tempFile.getPath)

    Trace.letTracers(Seq(tracer)) {
      val future1 = Trace.traceLocalFuture("test") {
        taskWithDuration(10.millis)
      }

      val future2 = Trace.traceLocalFuture("test2") {
        taskWithDuration(
          100.millis,
          childTask = Futures
            .join(
              Trace.traceLocalFuture("test3") {
                taskWithDuration(10.millis)
              },
              Trace.traceLocalFuture("test4") {
                taskWithDuration(10.millis)
              }).unit
        )
      }

      Await.ready(Futures.join(future1, future2))
    }

    eventually {
      val parsedSpans = parseSpansFromFile(tempFile.getPath)
      assert(parsedSpans.size == 3)
      assert(parsedSpans.exists(_.name() == "test2"))
      assert(parsedSpans.exists(_.name() == "test3"))
      assert(parsedSpans.exists(_.name() == "test4"))
    }
  }

  test("Keeps limited number of spans in memory") {
    val tempFile = File.createTempFile("traces", ".json")
    val tracer =
      new DurationFilteringTracer(100.millis, 1F, tempFile.getPath, maxInFlightTraces = 99)

    val latch = new CountDownLatch(100)
    val barrier = Promise[Unit]()

    val tasks = Trace.letTracers(Seq(tracer)) {
      for (i <- 0.until(100)) yield {
        Trace.traceLocalFuture(s"task$i")(futurePool { // top level tasks
          Trace.traceLocalFuture(s"subtask$i")(Future()) // bottom level tasks
          latch.countDown()
          Await.ready(barrier)
        })
      }
    }

    latch.await()
    Thread.sleep(100) // ensures tasks exceed 100ms threshold
    barrier.update(Return())
    Await.ready(Future.join(tasks))

    eventually {
      val parsedSpans = parseSpansFromFile(tempFile.getPath)
      assert(parsedSpans.size < 200) // all 100 top level spans at most 99 bottom level spans
      assert(parsedSpans.count(_.name().contains("subtask")) < 100)
    }
  }

  test("Only collects limited number of spans based on sample rate") {
    val tempFile = File.createTempFile("traces", ".json")

    val tracer = new DurationFilteringTracer(0.millis, 0.1F, tempFile.getPath)

    Trace.letTracers(Seq(tracer)) {
      for (i <- 0 until 100) {
        Trace.traceLocalFuture(s"task$i")(Future())
      }
    }

    eventually {
      val parsedSpans = parseSpansFromFile(tempFile.getPath)
      assert(parsedSpans.size < 30)
    }
  }

  private[this] def taskWithDuration(
    duration: Duration,
    childTask: => Future[Unit] = Future()
  ): Future[Unit] = {
    Future.Unit.delayed(duration).flatMap(_ => childTask)
  }

  private[this] def parseSpansFromFile(filename: String): Seq[zipkin2.Span] = {
    Source
      .fromFile(filename)
      .getLines()
      .map(line => SpanBytesDecoder.JSON_V2.decodeOne(line.getBytes()))
  }.toSeq
}
