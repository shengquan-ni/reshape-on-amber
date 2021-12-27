package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{ReportCurrentProcessingTuple, WorkflowPaused, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Completed, Paused, Running}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object PauseHandler {

  final case class PauseWorkflow() extends ControlCommand[CommandCompleted]
}

/** pause the entire workflow
  *
  * possible sender: client, controller
  */
trait PauseHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: PauseWorkflow, sender) =>
    {
      Future
        .collect(workflow.getAllOperators.map { operator =>
          // create a buffer for the current input tuple
          // since we need to show them on the frontend
          val buffer = mutable.ArrayBuffer[(ITuple, ActorVirtualIdentity)]()
          Future
            .collect(
              operator.getAllWorkers
                // send pause to all workers
                // pause message has no effect on completed or paused workers
                .map { worker =>
                  // send a pause message
                  send(PauseWorker(), worker).map { ret =>
                    operator.getWorker(worker).state = ret
                    send(QueryStatistics(), worker)
                      .join(send(QueryCurrentInputTuple(), worker))
                      // get the stats and current input tuple from the worker
                      .map {
                        case (stats, tuple) =>
                          operator.getWorker(worker).stats = stats
                          buffer.append((tuple, worker))
                      }
                  }
                }.toSeq
            )
            .map { ret =>
              // for each paused operator, send the input tuple
              if (eventListener.reportCurrentTuplesListener != null) {
                eventListener.reportCurrentTuplesListener
                  .apply(ReportCurrentProcessingTuple(operator.id.operator, buffer.toArray))
              }
            }
        }.toSeq)
        .map { ret =>
          // update frontend workflow status
          updateFrontendWorkflowStatus()
          // send paused to frontend
          if (eventListener.workflowPausedListener != null) {
            eventListener.workflowPausedListener.apply(WorkflowPaused())
          }
          disableStatusUpdate() // to be enabled in resume
          actorContext.parent ! ControllerState.Paused // for testing
          CommandCompleted()
        }
    }
  }

}
