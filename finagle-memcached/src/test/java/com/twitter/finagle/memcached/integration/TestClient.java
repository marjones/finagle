package com.twitter.finagle.memcached.integration;

import scala.Option;

import org.junit.Before;
import org.junit.Test;

import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
import com.twitter.finagle.Memcached;
import com.twitter.finagle.Names;
import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.JavaClient;
import com.twitter.finagle.memcached.JavaClientBase;
import com.twitter.finagle.memcached.integration.external.MemcachedServer;
import com.twitter.finagle.memcached.integration.external.MemcachedServer$;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.io.Bufs;
import com.twitter.util.Await;

import static org.junit.Assert.assertEquals;

public class TestClient {
  private MemcachedServer server;

  @Before
  public void setUp() {
    server = MemcachedServer$.MODULE$.start();
  }

  /**
   * Tests Get/Set commands.
   */
  @Test
  public void testGetAndSet() throws Exception {
    Address addr = Addresses.newInetAddress(server.address());

    Service<Command, Response> service = Memcached.client()
          .connectionsPerEndpoint(1)
          .newService(Names.bound(addr), "memcached");

    JavaClient client = JavaClientBase.newInstance(service);
    Await.ready(client.set("foo", "bar"));

    Option<String> res = Bufs.UTF_8.unapply(Await.result(client.get("foo")));
    assertEquals("bar", res.get());
  }
}
