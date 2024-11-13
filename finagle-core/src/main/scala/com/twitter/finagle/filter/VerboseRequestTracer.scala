package com.twitter.finagle.filter

import com.twitter.app.GlobalFlag
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.ServiceProxy
import com.twitter.finagle.Stack
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future

object verboseRequestTracing
    extends GlobalFlag[Boolean](
      """Experimental flag. Enables verbose request tracing, which includes tracing though the finagle stack""".stripMargin
    )

private[twitter] object VerboseRequestTracer {

  sealed trait Param {
    def mk(): (Param, Stack.Param[Param]) = (this, Param.param)
  }

  private[finagle] object Param {
    case object Disabled extends Param
    case object Enabled extends Param

    implicit val param: Stack.Param[Param] = new Stack.Param[Param] {
      lazy val default: Param = {
        verboseRequestTracing.get match {
          case Some(value) if value => Enabled
          case _ => Disabled
        }
      }
    }
  }

  /**
   * Enables the [[VerboseRequestTracer]].
   */
  val Enabled: Param = Param.Enabled

  /**
   * Disables the [[VerboseRequestTracer]] (disabled by default).
   */
  val Disabled: Param = Param.Disabled

  private[finagle] val stackTransformer: Stack.Transformer =
    new Stack.Transformer {
      def apply[Req, Rep](stack: Stack[ServiceFactory[Req, Rep]]): Stack[ServiceFactory[Req, Rep]] =
        stack.map((hd, sf) => withRequestTracing(hd.role, sf))
    }

  private[this] def withRequestTracing[Req, Rep](
    role: Stack.Role,
    svcFac: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    new ServiceFactoryProxy[Req, Rep](svcFac) {
      override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
        super.apply(conn).map { svc =>
          new ServiceProxy[Req, Rep](svc) {
            override def apply(request: Req): Future[Rep] = {
              if (!Trace.isActivelyTracing) {
                super.apply(request)
              } else {
                Trace.traceLocalFuture(role.name + "_async") {
                  Trace.traceLocal(role.name + "_sync") {
                    super.apply(request)
                  }
                }
              }
            }
          }
        }
      }
    }
}
