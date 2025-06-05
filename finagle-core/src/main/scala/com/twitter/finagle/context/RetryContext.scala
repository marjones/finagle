package com.twitter.finagle.context

import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Try

/**
 * Value-less context indicating that this request is a Retry. We do not include the retry # because
 * it can be misleading. Suppose that we have services A -> B -> C. A makes a request to B, which makes
 * a request to C, which fails and is retried 3 times. All 3 retries fail, so A retries its request
 * to B. When C receives this request, it's really the 4th retry of the request, but because we cannot
 * pass context information back up the stack, we are unable to correctly set the # retries in the
 * context -- to A, it's the first retry, and to B, it's the first attempt.
 */
private[finagle] object RetryContext {

  final class Retry

  private val retry = new Retry
  val returnRetry = Return(retry)

  private final class Context extends Contexts.broadcast.Key[Retry]("c.t.f.Retry") {
    def marshal(value: Retry): Buf = Buf.Empty

    def tryUnmarshal(buf: Buf): Try[Retry] = {
      returnRetry
    }
  }

  val Ctx: Contexts.broadcast.Key[Retry] = new Context

  def isRetry: Boolean = {
    Contexts.broadcast.get(Ctx).nonEmpty
  }

  def withRetry[T](f: => T): T = {
    Contexts.broadcast.let(Ctx, retry) {
      f
    }
  }
}
