package com.twitter.finagle.filter

import com.twitter.finagle.context.Contexts
import com.twitter.finagle._
import com.twitter.util.Future

/**
 * ClearBroadcastContextFilter clears the broadcast context of all context keys except those specified.
 */
private[twitter] object ClearBroadcastContextFilter {
  val role = Stack.Role("ClearBroadcastContextFilter")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.Filter]] that clears the
   * broadcast context of all context keys except those specified.
   */
  def module[Req, Rep](
    retainKeys: Set[Contexts.broadcast.Key[_]]
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = ClearBroadcastContextFilter.role
      val description =
        s"Clears the broadcast context of all keys except: ${retainKeys.mkString(",")}"
      val lookupIds = retainKeys.map(_.lookupId)
      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val filter = new SimpleFilter[Req, Rep] {
          def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
            Contexts.broadcast.retainIds(lookupIds) {
              service(request)
            }
          }
        }
        filter.andThen(next)
      }
    }
}
