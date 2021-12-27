package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryAndRemoveBreakpointsHandler.QueryAndRemoveBreakpoints
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Paused

object QueryAndRemoveBreakpointsHandler {

  final case class QueryAndRemoveBreakpoints(ids: Array[String]) extends ControlCommand[Array[LocalBreakpoint]]
}

trait QueryAndRemoveBreakpointsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QueryAndRemoveBreakpoints, sender) =>
    stateManager.assertState(Paused)
    val ret = breakpointManager.getBreakpoints(msg.ids)
    breakpointManager.removeBreakpoints(msg.ids)
    ret
  }

}
