package com.twitter.finagle.context

import org.scalatest.funsuite.AnyFunSuite

class RetryContextTest extends AnyFunSuite {

  test("Get/Set") {
    assert(!RetryContext.isRetry)
    RetryContext.withRetry {
      assert(RetryContext.isRetry)
    }
  }
}
