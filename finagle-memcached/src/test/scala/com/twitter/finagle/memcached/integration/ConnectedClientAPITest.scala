package com.twitter.finagle.memcached.integration
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.ExternalMemcached
import com.twitter.finagle.memcached.protocol.ClientError

class ConnectedClientAPITest extends ClientAPITest {

  override protected def mkClient(dest: Name): Client = {
    val service = Memcached.client
      .connectionsPerEndpoint(1)
      .newService(dest, "memcache")
    Client(service)
  }

  // The twemcache client doesn't check this so move out of the common tests. Nulls are not
  // scala-idiomatic so don't change the twemcache client, but keep the current checking in
  // the base ConnectedClient in case it's being relied on.
  test("Null Keys") {
    intercept[ClientError] {
      awaitResult(client.get(null: Seq[String]))
    }

    intercept[ClientError] {
      awaitResult(client.gets(null: Seq[String]))
    }
  }

  // The twemcache client doesn't support stats
  if (ExternalMemcached.use()) {
    test("stats") {
      val stats = awaitResult(client.stats())
      assert(stats != null)
      assert(!stats.isEmpty)
      stats.foreach { stat => assert(stat.startsWith("STAT")) }
    }
  }
}
