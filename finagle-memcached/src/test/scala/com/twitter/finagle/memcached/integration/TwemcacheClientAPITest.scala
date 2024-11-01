package com.twitter.finagle.memcached.integration
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.GetResult
import com.twitter.finagle.memcached.GetsResult

class TwemcacheClientAPITest extends ClientAPITest {

  override protected def mkClient(dest: Name): Client = {
    Memcached.client
      .connectionsPerEndpoint(1)
      .newRichClient(dest, "foo")
  }

  test("empty key sequence") {
    assert(awaitResult(client.get(Seq.empty)).isEmpty)
    assert(awaitResult(client.gets(Seq.empty)).isEmpty)
    assert(awaitResult(client.getWithFlag(Seq.empty)).isEmpty)
    assert(awaitResult(client.getsWithFlag(Seq.empty)).isEmpty)
    assert(awaitResult(client.getResult(Seq.empty)) == GetResult.Empty)
    assert(awaitResult(client.getsResult(Seq.empty)) == GetsResult(GetResult.Empty))
  }
}
