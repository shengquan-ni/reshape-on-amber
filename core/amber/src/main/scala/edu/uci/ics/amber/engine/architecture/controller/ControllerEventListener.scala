package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  BreakpointTriggered,
  ErrorOccurred,
  ModifyLogicCompleted,
  ReportCurrentProcessingTuple,
  SkipTupleResponse,
  WorkflowCompleted,
  WorkflowPaused,
  WorkflowStatusUpdate
}

case class ControllerEventListener(
    workflowCompletedListener: WorkflowCompleted => Unit = null,
    workflowStatusUpdateListener: WorkflowStatusUpdate => Unit = null,
    modifyLogicCompletedListener: ModifyLogicCompleted => Unit = null,
    breakpointTriggeredListener: BreakpointTriggered => Unit = null,
    workflowPausedListener: WorkflowPaused => Unit = null,
    skipTupleResponseListener: SkipTupleResponse => Unit = null,
    reportCurrentTuplesListener: ReportCurrentProcessingTuple => Unit = null,
    recoveryStartedListener: Unit => Unit = null,
    workflowExecutionErrorListener: ErrorOccurred => Unit = null
)
