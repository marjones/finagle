package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RetryContext
import com.twitter.finagle.context.RetryContext.Retry
import com.twitter.util.Try

private object HttpRetry extends HttpContext {

  type ContextKeyType = Retry
  val key: Contexts.broadcast.Key[Retry] = RetryContext.Ctx

  def toHeader(retry: Retry): String = {
    "1"
  }

  def fromHeader(header: String): Try[Retry] = {
    RetryContext.returnRetry
  }
}
