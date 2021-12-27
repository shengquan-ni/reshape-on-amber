package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{EndMarker, EndOfAllMarker}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Ready, Running, WorkerState}
import edu.uci.ics.amber.error.WorkflowRuntimeError

object StartHandler {
  final case class StartWorker() extends ControlCommand[WorkerState]
}

trait StartHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorker, sender) =>
    stateManager.assertState(Ready)
    if (operator.isInstanceOf[ISourceOperatorExecutor]) {
      dataProcessor.appendElement(EndMarker)
      dataProcessor.appendElement(EndOfAllMarker)
      stateManager.transitTo(Running)
      stateManager.getCurrentState
    } else {
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          "unexpected Start message for non-source operator!",
          selfID.toString,
          Map.empty
        )
      )
    }
  }
}
