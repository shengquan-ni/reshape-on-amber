package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running

object ResumeHandler {
  final case class ResumeWorkflow() extends ControlCommand[CommandCompleted]
}

/** resume the entire workflow
  *
  * possible sender: controller, client
  */
trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorkflow, sender) =>
    {

      // send all workers resume
      // resume message has no effect on non-paused workers
      Future
        .collect(workflow.getAllWorkers.map { worker =>
          send(ResumeWorker(), worker).map { ret =>
            workflow.getWorkerInfo(worker).state = ret
          }
        }.toSeq)
        .map { ret =>
          // update frontend status
          updateFrontendWorkflowStatus()
          enableStatusUpdate() //re-enabled it since it is disabled in pause
          actorContext.parent ! ControllerState.Running //for testing
          CommandCompleted()
        }
    }
  }
}
