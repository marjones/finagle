package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Request
import com.twitter.finagle.stats.MetricBuilder.CounterType
import com.twitter.finagle.stats.MetricBuilder.GaugeType
import com.twitter.finagle.stats.MetricsView.CounterSnapshot
import com.twitter.finagle.stats.MetricsView.GaugeSnapshot
import com.twitter.finagle.stats.MetricsView.HistogramSnapshot
import com.twitter.util.Await
import org.scalatest.funsuite.AnyFunSuite

class PrometheusExporterTest extends AnyFunSuite {

  import SampleSnapshots._

  private[this] def writeMetrics(
    counters: Iterable[CounterSnapshot],
    gauges: Iterable[GaugeSnapshot],
    histograms: Iterable[HistogramSnapshot],
    exportMetadata: Boolean,
    exportEmptyQuantiles: Boolean = true,
    verbosityPattern: Option[String => Boolean] = None
  ): String = {
    val view = new TestMetricsView(counters, gauges, histograms)
    new PrometheusExporter(
      exportMetadata = exportMetadata,
      exportEmptyQuantiles = exportEmptyQuantiles,
      verbosityPattern = verbosityPattern)
      .writeMetricsString(view)
  }

  test("Write a counter without any labels") {
    val result =
      writeMetrics(counters = Seq(NoLabelCounter), gauges = Seq(), histograms = Seq(), true)
    assert(
      result ==
        """# TYPE requests counter
          |# UNIT requests Requests
          |requests 1
          |""".stripMargin)
  }

  test("write counter with +/- infinite value") {
    val posInfCounter = CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = CounterType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Long.MaxValue
    )

    val negInfCounter = CounterSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = CounterType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Long.MinValue
    )

    val result = writeMetrics(
      counters = Seq(posInfCounter, negInfCounter),
      gauges = Seq(),
      histograms = Seq(),
      true)
    val expected =
      """# TYPE requests_pos counter
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg counter
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(result == expected)
  }

  test("write long gauge with +/- infinite value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Long.MaxValue
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Long.MinValue
    )

    val result = writeMetrics(
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq(),
      true)
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(result == expected)
  }

  test("write float gauge with +/- Float.MaxValue value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Float.MaxValue
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Float.MinValue
    )

    val result = writeMetrics(
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq(),
      true)
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(result == expected)
  }

  test("write float gauge with +/- Infinity value") {
    val posInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_pos"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Float.PositiveInfinity
    )

    val negInfCounter = GaugeSnapshot(
      hierarchicalName = "requests",
      builder = MetricBuilder(
        name = Seq("requests_neg"),
        metricType = GaugeType,
        units = Requests
      ).withLabels(Map("role" -> "foo")).withDimensionalSupport,
      value = Float.NegativeInfinity
    )

    val result = writeMetrics(
      counters = Seq(),
      gauges = Seq(posInfCounter, negInfCounter),
      histograms = Seq(),
      true)
    val expected =
      """# TYPE requests_pos gauge
        |# UNIT requests_pos Requests
        |requests_pos{role="foo"} +Inf
        |# TYPE requests_neg gauge
        |# UNIT requests_neg Requests
        |requests_neg{role="foo"} -Inf
        |""".stripMargin
    assert(result == expected)
  }

  test("end-to-end fetching stats works") {
    val registry: MetricsView = new TestMetricsView(
      Seq(RequestsCounter, ClntExceptionsCounter),
      Seq(PoolSizeFloatGauge, poolSizeLongGauge),
      Seq(HistoSample))

    val exporter = new PrometheusExporterHandler(registry)

    val request = Request("/admin/prometheus.txt")
    val response = Await.result(exporter.apply(request), 1.seconds)
    assert(Some("text/plain; version=0.0.4;charset=utf-8") == response.contentType)
    val expected =
      """requests{role="foo",job="baz-service",env="staging",zone="dc1"} 1
        |failures{side="clnt",exception="com.twitter.finagle.ChannelClosedException",method_name="get",type="logical",client_label="baz-service"} 2
        |pool_size_float{pool="future_pool",rpc="finagle"} 3.0
        |pool_size_long{pool="future_pool",rpc="finagle"} 3
        |foo_ping_ms{biz="bar",quantile="0.5"} 2
        |foo_ping_ms{biz="bar",quantile="0.95"} 3
        |foo_ping_ms_count{biz="bar"} 3
        |foo_ping_ms_sum{biz="bar"} 6
        |""".stripMargin
    assert(response.contentString == expected)
  }

  test("Filter out debug metrics by default") {
    val result = writeMetrics(Seq(DebugCounter), Seq.empty, Seq.empty, true)
    assert(result.isEmpty)
  }

  test("Allow debug metrics that match a verbose pattern") {
    def allow(input: String): Boolean = input.contains("requests")
    val result =
      writeMetrics(Seq(DebugCounter), Seq.empty, Seq.empty, true, verbosityPattern = Some(allow(_)))
    assert(result.contains("debug_requests"))
  }

  test("can omit empty percentiles") {
    val result = writeMetrics(
      Seq.empty,
      Seq.empty,
      Seq(EmptyHistoSample),
      exportMetadata = true,
      exportEmptyQuantiles = false)

    assert(
      result ==
        """# TYPE foo_ping_ms summary
        |# UNIT foo_ping_ms Milliseconds
        |foo_ping_ms_count{biz="bar"} 0
        |foo_ping_ms_sum{biz="bar"} 0
        |""".stripMargin
    )
  }

  test("will still emit non-empty percentiles if emitEmptyPercentiles = false") {
    val result = writeMetrics(
      Seq.empty,
      Seq.empty,
      Seq(HistoSample),
      exportMetadata = true,
      exportEmptyQuantiles = false)

    assert(
      result ==
        """# TYPE foo_ping_ms summary
          |# UNIT foo_ping_ms Milliseconds
          |foo_ping_ms{biz="bar",quantile="0.5"} 2
          |foo_ping_ms{biz="bar",quantile="0.95"} 3
          |foo_ping_ms_count{biz="bar"} 3
          |foo_ping_ms_sum{biz="bar"} 6
          |""".stripMargin)
  }

  test("translate illegal Prometheus characters to underscores") {
    val counter = CounterSnapshot(
      hierarchicalName = "metric",
      builder = MetricBuilder(
        name = Seq("invalid-metric.seconds"),
        metricType = CounterType,
        units = Seconds
      ).withLabels(Map("invalid.label" -> "foo")).withDimensionalSupport,
      value = 500L
    )
    val result = writeMetrics(Seq(counter), Seq.empty, Seq.empty, exportMetadata = true)

    assert(
      result ==
        """# TYPE invalid_metric_seconds counter
          |# UNIT invalid_metric_seconds Seconds
          |invalid_metric_seconds{invalid_label="foo"} 500
          |""".stripMargin)
  }
}
