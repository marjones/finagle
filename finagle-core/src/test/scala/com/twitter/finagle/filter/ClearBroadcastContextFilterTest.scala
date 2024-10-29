package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.context.Requeues
import com.twitter.finagle.stack.nilStack
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class ClearBroadcastContextFilterTest extends AnyFunSuite {

  test("clears context except for the configured keys") {
    val clearContextFilter =
      ClearBroadcastContextFilter.module[Unit, Set[String]](retainKeys = Set(Deadline))

    val svcModule = new Stack.Module0[ServiceFactory[Unit, Set[String]]] {
      val role = Stack.Role("svcModule")
      val description = ""

      def make(next: ServiceFactory[Unit, Set[String]]) =
        ServiceFactory.const(
          Service.mk[Unit, Set[String]](_ =>
            Future.value(Contexts.broadcast
              .marshal().map {
                case (k, v) =>
                  Buf.Utf8.unapply(k).get
              }.toSet)))
    }

    val factory = new StackBuilder[ServiceFactory[Unit, Set[String]]](nilStack[Unit, Set[String]])
      .push(svcModule)
      .push(clearContextFilter)
      .make(Stack.Params.empty)

    val svc: Service[Unit, Set[String]] = Await.result(factory(), 1.second)
    Contexts.broadcast.let(Requeues, Requeues(5), Deadline, Deadline.ofTimeout(5.seconds)) {
      assert(Await.result(svc(()), 1.second) == Set(Deadline.id))
    }
  }
}
