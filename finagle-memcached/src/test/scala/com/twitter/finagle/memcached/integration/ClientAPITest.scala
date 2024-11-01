package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.Name
import com.twitter.finagle.memcached.Client
import com.twitter.finagle.memcached.integration.external.ExternalMemcached
import com.twitter.finagle.memcached.integration.external.MemcachedServer
import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Awaitable
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

// These tests run against either a test Memcached server or a real MemcachedServer,
// depending on the value of `ExternalMemcached.use`
abstract class ClientAPITest extends AnyFunSuite with BeforeAndAfter {

  protected var client: Client = _
  private var testServer: MemcachedServer = _

  protected def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 5.seconds)

  protected def mkClient(dest: Name): Client

  before {
    testServer = MemcachedServer.start()
    client = mkClient(Name.bound(Seq(Address(testServer.address)): _*))
  }

  after {
    testServer.stop()
  }

  test("set & get") {
    awaitResult(client.delete("foo"))
    assert(awaitResult(client.get("foo")) == None)
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    assert(awaitResult(client.get("foo")).get == Buf.Utf8("bar"))
  }

  test("set & get data containing newlines") {
    awaitResult(client.delete("bob"))
    assert(awaitResult(client.get("bob")) == None)
    awaitResult(client.set("bob", Buf.Utf8("hello there \r\n nice to meet \r\n you")))
    assert(
      awaitResult(client.get("bob")).get == Buf.Utf8("hello there \r\n nice to meet \r\n you"),
      3.seconds
    )
  }

  test("get") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.get(Seq("foo", "baz", "notthere")))
      .map { case (key, Buf.Utf8(value)) => (key, value) }
    assert(
      result == Map(
        "foo" -> "bar",
        "baz" -> "boing"
      )
    )
  }

  test("getWithFlag") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.set("baz", Buf.Utf8("boing")))
    val result = awaitResult(client.getWithFlag(Seq("foo", "baz", "notthere")))
      .map { case (key, ((Buf.Utf8(value), Buf.Utf8(flag)))) => (key, (value, flag)) }
    assert(
      result == Map(
        "foo" -> (("bar", "0")),
        "baz" -> (("boing", "0"))
      )
    )
  }

  test("append & prepend") {
    awaitResult(client.set("foo", Buf.Utf8("bar")))
    awaitResult(client.append("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res) = awaitResult(client.get("foo")).get
    assert(res == "barrab")
    awaitResult(client.prepend("foo", Buf.Utf8("rab")))
    val Buf.Utf8(res2) = awaitResult(client.get("foo")).get
    assert(res2 == "rabbarrab")
  }

  test("incr & decr") {
    // As of memcached 1.4.8 (issue 221), empty values are no longer treated as integers
    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    assert(awaitResult(client.incr("foo", 2)) == Some(3L))
    assert(awaitResult(client.decr("foo")) == Some(2L))

    awaitResult(client.set("foo", Buf.Utf8("0")))
    assert(awaitResult(client.incr("foo")) == Some(1L))
    val l = 1L << 50
    assert(awaitResult(client.incr("foo", l)) == Some(l + 1L))
    assert(awaitResult(client.decr("foo")) == Some(l))
    assert(awaitResult(client.decr("foo", l)) == Some(0L))
  }

  test("send malformed keys") {
    // test key validation trait
    intercept[ClientError] {
      awaitResult(client.get("fo o"))
    }
    intercept[ClientError] {
      awaitResult(client.set("", Buf.Utf8("bar")))
    }
    intercept[ClientError] {
      awaitResult(client.get("    foo"))
    }
    intercept[ClientError] {
      awaitResult(client.get("foo   "))
    }
    intercept[ClientError] {
      awaitResult(client.get("    foo"))
    }
    intercept[ClientError] {
      awaitResult(client.get(null: String))
    }
    intercept[ClientError] {
      awaitResult(client.set(null: String, Buf.Utf8("bar")))
    }
    intercept[ClientError] {
      awaitResult(client.set("    ", Buf.Utf8("bar")))
    }

    try {
      awaitResult(client.set("\t", Buf.Utf8("bar")))
    } catch {
      case _: ClientError => fail("\t is allowed")
    }

    intercept[ClientError] {
      awaitResult(client.set("\r", Buf.Utf8("bar")))
    }
    intercept[ClientError] {
      awaitResult(client.set("\n", Buf.Utf8("bar")))
    }
    intercept[ClientError] {
      awaitResult(client.set("\u0000", Buf.Utf8("bar")))
    }

    val veryLongKey =
      "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
    intercept[ClientError] {
      awaitResult(client.get(veryLongKey))
    }
    intercept[ClientError] {
      awaitResult(client.set(veryLongKey, Buf.Utf8("bar")))
    }

    // test other keyed command validation
    intercept[ClientError] {
      awaitResult(client.gets(Seq(null)))
    }
    intercept[ClientError] {
      awaitResult(client.gets(Seq("")))
    }
    intercept[ClientError] {
      awaitResult(client.gets(Seq("foos", "bad key", "somethingelse")))
    }
    intercept[ClientError] {
      awaitResult(client.append("bad key", Buf.Utf8("rab")))
    }
    intercept[ClientError] {
      awaitResult(client.prepend("bad key", Buf.Utf8("rab")))
    }
    intercept[ClientError] {
      awaitResult(client.replace("bad key", Buf.Utf8("bar")))
    }
    intercept[ClientError] {
      awaitResult(client.add("bad key", Buf.Utf8("2")))
    }
    intercept[ClientError] {
      awaitResult(client.checkAndSet("bad key", Buf.Utf8("z"), Buf.Utf8("2")))
    }
    intercept[ClientError] {
      awaitResult(client.incr("bad key"))
    }
    intercept[ClientError] {
      awaitResult(client.decr("bad key"))
    }
    intercept[ClientError] {
      awaitResult(client.delete("bad key"))
    }
  }

  // Only run these if we have a real memcached server to use
  if (ExternalMemcached.use()) {
    test("gets") {
      assert(awaitResult(client.gets("foos")).isEmpty)
      awaitResult(client.set("foos", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs", Buf.Utf8("zyx")))
      val result = awaitResult(client.gets(Seq("foos", "bazs", "somethingelse")))
        .map {
          case (key, (Buf.Utf8(value), Buf.Utf8(casUnique))) =>
            (key, (value, casUnique))
        }

      assert(
        result == Map(
          "foos" -> (
            (
              "xyz",
              "2"
            )
          ), // the "cas unique" values are predictable from a fresh memcached
          "bazs" -> (("zyx", "4"))
        )
      )
    }

    test("getsWithFlag") {
      awaitResult(client.set("foos1", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs1", Buf.Utf8("xyz")))
      awaitResult(client.set("bazs1", Buf.Utf8("zyx")))
      val result =
        awaitResult(client.getsWithFlag(Seq("foos1", "bazs1", "somethingelse")))
          .map {
            case (key, (Buf.Utf8(value), Buf.Utf8(flag), Buf.Utf8(casUnique))) =>
              (key, (value, flag, casUnique))
          }

      // the "cas unique" values are predictable from a fresh memcached
      assert(
        result == Map(
          "foos1" -> (("xyz", "0", "2")),
          "bazs1" -> (("zyx", "0", "4"))
        )
      )
    }

    test("cas") {
      awaitResult(client.set("x", Buf.Utf8("y")))
      val Some((value, casUnique)) = awaitResult(client.gets("x"))
      assert(value == Buf.Utf8("y"))
      assert(casUnique == Buf.Utf8("2"))

      assert(!awaitResult(client.checkAndSet("x", Buf.Utf8("z"), Buf.Utf8("1")).map(_.replaced)))
      assert(
        awaitResult(client.checkAndSet("x", Buf.Utf8("z"), casUnique).map(_.replaced)).booleanValue
      )
      val res = awaitResult(client.get("x"))
      assert(res.isDefined)
      assert(res.get == Buf.Utf8("z"))
    }
  }
}
