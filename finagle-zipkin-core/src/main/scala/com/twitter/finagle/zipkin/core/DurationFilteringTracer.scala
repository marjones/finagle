package com.twitter.finagle.zipkin.core

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalCause
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.zipkin.core.DurationFilteringTracer.BitMask
import com.twitter.finagle.zipkin.core.DurationFilteringTracer.Multiplier
import com.twitter.finagle.zipkin.core.DurationFilteringTracer.SomeFalse
import com.twitter.finagle.zipkin.core.DurationFilteringTracer.SomeTrue
import com.twitter.finagle.zipkin.core.DurationFilteringTracer.salt
import com.twitter.util.Duration
import com.twitter.util.Future
import java.io.FileOutputStream
import java.lang.ThreadLocal
import java.util.concurrent.ConcurrentMap
import org.apache.thrift.TSerializer
import scala.util.Random
import zipkin.internal.ApplyTimestampAndDuration
import zipkin.Codec
import zipkin2.codec.SpanBytesDecoder
import java.lang.{Long => JLong}
import scala.util.Using
import zipkin2.codec.SpanBytesEncoder

object DurationFilteringTracer {
  // Use same sampling params here as in com.twitter.finagle.zipkin.core.Sampler
  private val Multiplier = (1 << 24).toFloat
  private val BitMask = (Multiplier - 1).toInt
  private val salt = new Random().nextInt()

  private val SomeTrue = Some(true)
  private val SomeFalse = Some(false)
}

class DurationFilteringTracer(
  duration: Duration,
  samplingRate: Float,
  outputPath: String,
  maxInFlightTraces: Int = 2000,
  statsReceiver: StatsReceiver = NullStatsReceiver)
    extends RawZipkinTracer {

  if (samplingRate < 0 || samplingRate > 1) {
    throw new IllegalArgumentException(
      "Sample rate not within the valid range of 0-1, was " + samplingRate
    )
  }

  private[this] val persistedSpansCounter = statsReceiver.counter("persistedSpans")
  private[this] val evictions = statsReceiver.counter("evictions")

  private[this] val thriftSerialiser = ThreadLocal.withInitial(() => new TSerializer())

  // map from TraceID -> spans within that trace
  private[this] val spanRoots: ConcurrentMap[Long, List[zipkin.Span]] = Caffeine
    .newBuilder()
    .asInstanceOf[Caffeine[Long, List[zipkin.Span]]]
    .maximumSize(maxInFlightTraces)
    .evictionListener((_: Long, v: Seq[zipkin.Span], _: RemovalCause) => {
      evictions.incr()
    })
    .build[Long, List[zipkin.Span]].asMap()

  // sentinel value that will get set for a trace ID when we've seen at least one span in that trace
  // with duration >= threshold
  private[this] val durationThresholdMetSentinel = List[zipkin.Span]()

  val cacheSizeGauge = statsReceiver.addGauge("cacheSize")(spanRoots.size().floatValue())

  override def record(record: Record): Unit = {
    if (sampleTrace(record.traceId).contains(true)) {
      super.record(record)
    }
  }

  override def sampleTrace(traceId: TraceId): Option[Boolean] = {
    // Same as in com.twitter.finagle.zipkin.core.Sampler, except here we don't check if
    // the traceId has already had Some(false) set, since we want to consider all traceIds
    if (((JLong.hashCode(traceId.traceId.toLong) ^ salt) & BitMask) < samplingRate * Multiplier)
      SomeTrue
    else
      SomeFalse
  }

  override def getSampleRate: Float = samplingRate

  override def sendSpans(spans: Seq[Span]): Future[Unit] = {
    spans.map(convertToZipkinSpan).foreach { span =>
      if (span.duration >= duration.inMicroseconds) {
        val existingSpansForTrace = spanRoots.put(span.traceId, durationThresholdMetSentinel)
        persistSpans(span, existingSpansForTrace)
      } else {
        val existingSpansForTrace = spanRoots.compute(
          span.traceId,
          {
            case (_, null) => List(span) // this is the first span for the trace
            case (_, v) if v.eq(durationThresholdMetSentinel) =>
              durationThresholdMetSentinel // duration threshold has already been met
            case (_, v) =>
              v.+:(span) // there are existing spans, but duration threshold not yet met
          }
        )

        if (existingSpansForTrace.eq(durationThresholdMetSentinel)) {
          persistSpans(span, List.empty)
        }
      }
    }

    Future.Done
  }

  override def isActivelyTracing(traceId: TraceId): Boolean = sampleTrace(traceId).contains(true)

  private[this] def convertToZipkinSpan(span: Span): zipkin.Span = {
    val serialisedBytes = thriftSerialiser.get().serialize(span.toThrift)
    val zipkinV1ThriftSpan = zipkin.Codec.THRIFT.readSpan(serialisedBytes)
    ApplyTimestampAndDuration.apply(zipkinV1ThriftSpan)
  }

  private[this] def persistSpans(parent: zipkin.Span, children: Seq[zipkin.Span]): Unit = {
    val spansToPersist = if (children != null) children :+ parent else Seq(parent)
    persistedSpansCounter.incr(spansToPersist.size)
    Using(new FileOutputStream(outputPath, true)) { fileOutputStream =>
      spansToPersist.foreach { span =>
        val converted = convertV1SpanToV2(span)
        fileOutputStream.write(
          SpanBytesEncoder.JSON_V2
            .encode(converted))
        fileOutputStream.write('\n')
      }
      fileOutputStream.flush()
    }
  }

  private[this] def convertV1SpanToV2(span: zipkin.Span): zipkin2.Span = {
    val spanBytesV1 = Codec.THRIFT.writeSpan(span)
    SpanBytesDecoder.THRIFT.decodeOne(spanBytesV1)
  }
}
