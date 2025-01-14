package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object lowPriorityNumWorkers
    extends GlobalFlag[Int](
      """Experimental flag. Enables the low priority offload pool using a thread pool with the specified number of threads.
    | When this flag is greater that zero, the execution of low priority tasks happen in an isolated pool.
    |""".stripMargin)
