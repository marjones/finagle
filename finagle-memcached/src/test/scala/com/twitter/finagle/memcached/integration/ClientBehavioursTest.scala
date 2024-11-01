package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.ExternalMemcached
import com.twitter.finagle.memcached.integration.external.MemcachedServer
import com.twitter.finagle.memcached.protocol.Command
import com.twitter.finagle.memcached.protocol.Error
import com.twitter.finagle.memcached.protocol.Get
import com.twitter.finagle.memcached.protocol.Quit
import com.twitter.finagle.memcached.protocol.Response
import com.twitter.finagle.memcached.protocol.Set
import com.twitter.finagle.memcached.protocol.Value
import com.twitter.finagle.memcached.protocol.Values
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.{param => ctfparam}
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.util._
import com.twitter.util.registry.Entry
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.registry.SimpleRegistry
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalatest.BeforeAndAfter
import org.scalatest.Outcome
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.Milliseconds
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import scala.util.Random
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import scala.collection.JavaConverters._

class ClientBehavioursTest
    extends AnyFunSuite
    with BeforeAndAfter
    with Eventually
    with PatienceConfiguration {

  protected def createClient(dest: Name, clientName: String): Client = {
    Memcached.client.newRichClient(dest, clientName)
  }

  private[this] val NumServers = 5
  private[this] val NumConnections = 4
  private[this] val Timeout: Duration = 15.seconds
  private[this] var servers: Seq[MemcachedServer] = Seq.empty
  private[this] var client: Client = _
  private[this] val clientName = "test_client"

  private[this] val redistributesKey: Seq[String] =
    Seq("test_client", "partitioner", "redistributes")
  private[this] val leavesKey: Seq[String] = Seq(clientName, "partitioner", "leaves")
  private[this] val revivalsKey: Seq[String] = Seq(clientName, "partitioner", "revivals")
  private[this] val ejectionsKey: Seq[String] = Seq(clientName, "partitioner", "ejections")

  before {
    servers = for (_ <- 1 to NumServers) yield MemcachedServer.start()

    val dest = Name.bound(servers.map { s => Address(s.address) }: _*)
    client = createClient(dest, clientName)
  }

  after {
    servers.foreach(_.stop())
    client.close()
  }

  override def withFixture(test: NoArgTest): Outcome = {
    if (servers.length == NumServers) {
      test()
    } else {
      info("Cannot start memcached. Skipping test...")
      cancel()
    }
  }

  private[this] def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, Timeout)

  test("re-hash when a bad host is ejected") {
    val sr = new InMemoryStatsReceiver
    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(param.EjectFailedHost(true))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(ctfparam.Stats(sr))
      .newRichClient(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)

    val max = 200
    // set values
    awaitResult(
      Future.collect(
        (0 to max).map { i => client.set(s"foo$i", Buf.Utf8(s"bar$i")) }
      )
    )

    // We can't control the Distributor to make sure that for the set of servers, there is at least
    // one client in the partition talking to it. Therefore, we rely on the fact that for 5
    // backends, it's very unlikely all clients will be talking to the same server, and as such,
    // shutting down all backends but one will trigger cache misses.
    servers.tail.foreach(_.stop())

    // trigger ejection
    for (i <- 0 to max) {
      Await.ready(client.get(s"foo$i"), Timeout)
    }
    // wait a little longer than default to prevent test flappiness
    val timeout = PatienceConfiguration.Timeout(Span(10, Seconds))
    val interval = PatienceConfiguration.Interval(Span(100, Milliseconds))
    eventually(timeout, interval) {
      assert(sr.counters.getOrElse(ejectionsKey, 0L) > 2)
    }

    // previously set values have cache misses
    var cacheMisses = 0
    for (i <- 0 to max) {
      if (awaitResult(client.get(s"foo$i")).isEmpty) cacheMisses = cacheMisses + 1
    }
    assert(cacheMisses > 0)

    client.close()
  }

  test("host comes back into ring after being ejected") {
    class MockedMemcacheServer extends Service[Command, Response] {
      def apply(command: Command): Future[Response with Product with Serializable] = command match {
        case Get(_) => Future.value(Values(List(Value(Buf.Utf8("foo"), Buf.Utf8("bar")))))
        case Set(_, _, _, _) => Future.value(Error(new Exception))
        case x => Future.exception(new MatchError(x))
      }
    }

    val cacheServer: ListeningServer = Memcached.serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new MockedMemcacheServer
    )

    val timer = new MockTimer
    val statsReceiver = new InMemoryStatsReceiver

    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(param.EjectFailedHost(true))
      .configured(ctfparam.Timer(timer))
      .configured(ctfparam.Stats(statsReceiver))
      .newRichClient(
        Name.bound(Address(cacheServer.boundAddress.asInstanceOf[InetSocketAddress])),
        clientName)

    Time.withCurrentTimeFrozen { timeControl =>
      // Send a bad request
      intercept[Exception] {
        awaitResult(client.set("foo", Buf.Utf8("bar")))
      }

      // Node should have been ejected
      val timeout = PatienceConfiguration.Timeout(Span(1, Seconds))
      eventually(timeout) {
        assert(statsReceiver.counters.get(ejectionsKey).contains(1))
      }

      // Node should have been marked dead, and still be dead after 5 minutes
      timeControl.advance(5.minutes)

      // Shard should be unavailable
      intercept[ShardNotAvailableException] {
        awaitResult(client.get(s"foo"))
      }

      timeControl.advance(5.minutes)
      timer.tick()

      // 10 minutes (markDeadFor duration) have passed, so the request should go through
      assert(statsReceiver.counters.get(revivalsKey) == Some(1))
      assert(awaitResult(client.get(s"foo")).get == Buf.Utf8("bar"))
    }
    client.close()
  }

  test("Add and remove nodes") {
    val addrs = servers.map { s => Address(s.address) }

    // Start with 3 backends
    val mutableAddrs: ReadWriteVar[Addr] = new ReadWriteVar(Addr.Bound(addrs.toSet.drop(2)))

    val sr = new InMemoryStatsReceiver

    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(FailureAccrualFactory.Param(1, () => 10.minutes))
      .configured(param.EjectFailedHost(true))
      .connectionsPerEndpoint(NumConnections)
      .withStatsReceiver(sr)
      .newRichClient(Name.Bound.singleton(mutableAddrs), clientName)

    assert(sr.counters(redistributesKey) == 1)
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 3)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Add 2 nodes to the backends, for a total of 5 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet))

    assert(sr.counters(redistributesKey) == 2)
    // Need to rebuild each of the 5 nodes with `numConnections`
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == 0)

    // Remove 1 node from the backends, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(redistributesKey) == 3)
    // Don't need to rebuild or update any existing nodes
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)

    assert(sr.counters(leavesKey) == 1)

    // Node is removed, closing `numConnections` in the LoadBalancer
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)

    // Update the backends with the same list, for a total of 4 backends
    mutableAddrs.update(Addr.Bound(addrs.toSet.drop(1)))

    assert(sr.counters(redistributesKey) == 4)
    // Ensure we don't do anything in the LoadBalancer because the set of nodes is the same
    assert(sr.counters(Seq("test_client", "loadbalancer", "rebuilds")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "updates")) == 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "adds")) == NumConnections * 5)
    assert(sr.counters(Seq("test_client", "loadbalancer", "removes")) == NumConnections)

    client.close()
  }

  test("FailureAccrualFactoryException has remote address") {
    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .configured(TimeoutFilter.Param(10000.milliseconds))
      .configured(FailureAccrualFactory.Param(1, 10.minutes))
      .configured(param.EjectFailedHost(false))
      .connectionsPerEndpoint(1)
      .newRichClient(Name.bound(Address("localhost", 1234)), clientName)

    // Trigger transition to "Dead" state
    intercept[Exception] {
      awaitResult(client.delete("foo"))
    }

    // Client has not been ejected, so the same client gets a re-application of the connection,
    // triggering the 'failureAccrualEx' in KetamaFailureAccrualFactory
    val failureAccrualEx = intercept[HasRemoteInfo] {
      awaitResult(client.delete("foo"))
    }

    assert(failureAccrualEx.getMessage().contains("Endpoint is marked dead by failureAccrual"))
    assert(failureAccrualEx.getMessage().contains("Downstream Address: localhost/127.0.0.1:1234"))
    client.close()
  }

  test("traces fanout requests") {
    // we use an eventually block to retry the request if we didn't get partitioned to different shards
    eventually {
      val tracer = new BufferingTracer()

      // the servers created in `before` have inconsistent addresses, which means sharding
      // will be inconsistent across runs. To combat this, we'll start our own servers and rerun the
      // tests if we partition to the same shard.
      val servers =
        for (_ <- 1 to NumServers) yield MemcachedServer.start()

      val client = Memcached.client
        .configured(param.KeyHasher(KeyHasher.FNV1_32))
        .connectionsPerEndpoint(1)
        .withTracer(tracer)
        .newRichClient(Name.bound(servers.map { s => Address(s.address) }: _*), clientName)

      awaitResult(client.set("foo", Buf.Utf8("bar")))
      awaitResult(client.set("baz", Buf.Utf8("boing")))
      awaitResult(
        client.gets(Seq("foo", "baz"))
      ).flatMap {
        case (key, (Buf.Utf8(value1), Buf.Utf8(value2))) =>
          Map((key, (value1, value2)))
      }

      client.close()
      servers.foreach(_.stop())

      val gets: Seq[TraceId] = tracer.iterator.toList collect {
        case Record(id, _, Annotation.Rpc("Gets"), _) => id
      }

      // Moving the MemcachedTracingFilter means that partitioned requests should result in two gets spans
      assert(gets.length == 2)
      // However the FanoutProxy should ensure that the requests are stored in peers, not the same tid.
      gets.tail.foreach { get =>
        assert(get._parentId == gets.head._parentId)
        assert(get.spanId != gets.head.spanId)
      }
    }
  }

  test("set and get without partioning") {
    val c = Memcached.client
      .newLoadBalancedTwemcacheClient(
        Name.bound(servers.map { s => Address(s.address) }.head),
        clientName)
    awaitResult(c.set("xyz", Buf.Utf8("value1")))
    assert(awaitResult(c.get("xyz")).get == Buf.Utf8("value1"))
  }

  test("Push client uses Netty4PushTransporter") {
    val simple = new SimpleRegistry()
    val address = servers.head.address
    GlobalRegistry.withRegistry(simple) {
      val client =
        Memcached.client.newService(Name.bound(com.twitter.finagle.Address(address)), "memcache")
      client(Quit())
      val entries = simple.toSet
      assert(
        entries.contains(
          Entry(Seq("client", "memcached", "memcache", "Transporter"), "Netty4PushTransporter")
        )
      )
    }
  }

  test("annotates the total number of hits and misses") {
    withExpectedTraces(
      c => {
        awaitResult(c.set("foo", Buf.Utf8("bar")))
        awaitResult(c.set("bar", Buf.Utf8("baz")))
        // add a missing key
        awaitResult(c.getResult(Seq("foo", "bar", "themissingkey")))
      },
      Seq(
        Annotation.BinaryAnnotation("clnt/memcached.hits", 2),
        Annotation.BinaryAnnotation("clnt/memcached.misses", 1)
      )
    )
  }

  test("annotates with the endpoint") {
    withExpectedTraces(
      c => {
        awaitResult(c.get("foo"))
      },
      Seq(
        Annotation.BinaryAnnotation("clnt/namer.name", s"Set(Inet(${servers.head.address},Map()))")
      )
    )
  }

  // This works with our internal memcached because when the server is shutdown, we get an immediate
  // "connection refused" when trying to send a request. With external memcached, the connection
  // establishment instead hangs. To make this test pass with external memcached, we could add
  // `withSession.acquisitionTimeout` to the client, but this makes the test a) slow and b) can
  // make the other tests flakey, so don't bother.
  if (!ExternalMemcached.use()) {
    test("partial success") {
      val ValueSuffix = ":" + Time.now.inSeconds

      // creating multiple random strings so that we get a uniform distribution of keys the
      // ketama ring and thus the Memcached shards
      val keys = 1 to 1000 map { _ => Random.alphanumeric.take(20).mkString }
      val writes = keys map { key => client.set(key, Buf.Utf8(s"$key$ValueSuffix")) }
      awaitResult(Future.join(writes))

      val readValues: Map[String, Buf] = awaitResult {
        client.get(keys)
      }
      assert(readValues.size == keys.length)
      assert(readValues.keySet.toSeq.sorted == keys.sorted)
      readValues.keys foreach { key =>
        val Buf.Utf8(readValue) = readValues(key)
        assert(readValue == s"$key$ValueSuffix")
      }

      val initialResult = awaitResult {
        client.getResult(keys)
      }
      assert(initialResult.failures.isEmpty)
      assert(initialResult.misses.isEmpty)
      assert(initialResult.values.size == keys.size)

      // now kill one server
      servers.head.stop()

      // test partial success with getResult()
      val getResult = awaitResult {
        client.getResult(keys)
      }
      // assert the failures are set to the exception received from the failing partition
      assert(getResult.failures.nonEmpty)
      getResult.failures.foreach {
        case (_, e) =>
          assert(e.isInstanceOf[Exception])
      }
      // there should be no misses as all keys are known
      assert(getResult.misses.isEmpty)

      // assert that the values are what we expect them to be. We are not checking for exact
      // number of failures and successes here because we don't know how many keys will fall into
      // the failed partition. The accuracy of the responses are tested in other tests anyways.
      assert(getResult.values.nonEmpty)
      assert(getResult.values.size < keys.size)
      getResult.values.foreach {
        case (keyStr, valueBuf) =>
          val Buf.Utf8(valStr) = valueBuf
          assert(valStr == s"$keyStr$ValueSuffix")
      }
    }
  }

  private[this] def withExpectedTraces(f: Client => Unit, expected: Seq[Annotation]): Unit = {
    val tracer = spy(new NullTracer)
    when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)
    when(tracer.isNull).thenReturn(false)
    val captor: ArgumentCaptor[Record] = ArgumentCaptor.forClass(classOf[Record])

    // Bind to only one server so all requests go to it
    val client = Memcached.client
      .configured(param.KeyHasher(KeyHasher.FNV1_32))
      .connectionsPerEndpoint(1)
      .withTracer(tracer)
      .newRichClient(Name.bound(Seq(Address(servers.head.address)): _*), clientName)

    f(client)
    verify(tracer, atLeastOnce()).record(captor.capture())
    val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }
    assert(expected.filterNot(annotations.contains(_)).isEmpty)
  }
}
