package com.twitter.finagle.memcached.integration.external

import com.twitter.conversions.DurationOps._
import com.twitter.util.Duration
import com.twitter.util.RandomSocket
import com.twitter.util.Stopwatch
import java.net.BindException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.collection._

object MemcachedServer {

  def start(): MemcachedServer = {
    if (ExternalMemcached.use()) {
      ExternalMemcached.start()
    } else {
      InternalMemcached.start()
    }
  }
}

trait MemcachedServer {
  val address: InetSocketAddress
  def stop(): Unit
}

private[memcached] object InternalMemcached {
  def start(): MemcachedServer = {
    val server = new InProcessMemcached(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    )
    new MemcachedServer {
      val address = server.start().boundAddress.asInstanceOf[InetSocketAddress]
      def stop(): Unit = { server.stop(true) }
    }
  }
}

private[memcached] object ExternalMemcached {
  private[this] var processes: List[Process] = List()
  private[this] val forbiddenPorts = 11000.until(11900)
  private[this] var takenPorts: Set[Int] = Set[Int]()
  // prevent us from taking a port that is anything close to a real memcached port.

  def use(): Boolean = {
    externalMemcachedPath().nonEmpty
  }

  private[this] def externalMemcachedPath(): Option[String] = {
    Option(System.getProperty("EXTERNAL_MEMCACHED_PATH"))
  }

  private[this] def findAddress(): InetSocketAddress = {
    var address: Option[InetSocketAddress] = None
    var tries = 100
    while (address == None && tries >= 0) {
      address = Some(RandomSocket.nextAddress())
      if (forbiddenPorts.contains(address.get.getPort) ||
        takenPorts.contains(address.get.getPort)) {
        address = None
        tries -= 1
        Thread.sleep(5)
      }
    }
    if (address == None) throw new Exception("Couldn't get an address for the external memcached")

    takenPorts += address.get.getPort
    address.get
  }

  def start(): MemcachedServer = {
    def exec(address: InetSocketAddress): Process = {
      val cmd =
        List(externalMemcachedPath().get, "-l", address.getHostName, "-p", address.getPort.toString)
      val builder = new ProcessBuilder(cmd: _*)
      builder.start()
    }

    val addr = findAddress()
    val proc = exec(addr)
    processes :+= proc

    if (waitForPort(addr.getPort)) {
      new MemcachedServer {
        val address = addr

        def stop(): Unit = {
          proc.destroy()
          proc.waitFor()
        }
      }
    } else {
      throw new Exception("Timed out waiting for external memcached to start")
    }
  }

  def waitForPort(port: Int, timeout: Duration = 5.seconds): Boolean = {
    val elapsed = Stopwatch.start()
    def loop(): Boolean = {
      if (!isPortAvailable(port))
        true
      else if (timeout < elapsed())
        false
      else {
        Thread.sleep(100)
        loop()
      }
    }
    loop()
  }

  def isPortAvailable(port: Int): Boolean = {
    var ss: ServerSocket = null
    var result = false
    try {
      ss = new ServerSocket(port)
      ss.setReuseAddress(true)
      result = true
    } catch {
      case ex: BindException =>
        result = !ex.getMessage.contains("Address already in use")
    } finally {
      if (ss != null)
        ss.close()
    }

    result
  }

  // Make sure the process is always killed eventually
  Runtime
    .getRuntime()
    .addShutdownHook(new Thread {
      override def run(): Unit = {
        processes foreach { p =>
          p.destroy()
          p.waitFor()
        }
      }
    })
}
