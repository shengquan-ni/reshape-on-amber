package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalBreakpointTriggeredHandler.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class BreakpointManager(asyncRPCClient: AsyncRPCClient) {
  private var breakpoints = new Array[LocalBreakpoint](0)

  def registerOrReplaceBreakpoint(breakpoint: LocalBreakpoint): Unit = {
    var i = 0
    Breaks.breakable {
      while (i < breakpoints.length) {
        if (breakpoints(i).id == breakpoint.id) {
          breakpoints(i) = breakpoint
          Breaks.break()
        }
        i += 1
      }
      breakpoints = breakpoints :+ breakpoint
    }
  }

  def getBreakpoints(ids: Array[String]): Array[LocalBreakpoint] = {
    ids.map(id => {
      val idx = breakpoints.indexWhere(_.id == id)
      breakpoints(idx)
    })
  }

  def removeBreakpoint(breakpointID: String): Unit = {
    val idx = breakpoints.indexWhere(_.id == breakpointID)
    if (idx != -1) {
      breakpoints = breakpoints.take(idx)
    }
  }

  def removeBreakpoints(breakpointIDs: Array[String]): Unit = {
    breakpoints = breakpoints.filter(x => !breakpointIDs.contains(x.id))
  }

  def evaluateTuple(tuple: ITuple): Boolean = {
    var isTriggered = false
    var triggeredBreakpoints: ArrayBuffer[(String, Long)] = null
    breakpoints.indices.foreach { i =>
      if (breakpoints(i).checkCondition(tuple)) {
        isTriggered = true
        if (triggeredBreakpoints == null) {
          triggeredBreakpoints = ArrayBuffer[(String, Long)]()
        } else {
          triggeredBreakpoints.append((breakpoints(i).id, breakpoints(i).version))
        }
      }
    }
    if (isTriggered) {
      asyncRPCClient.send(
        LocalBreakpointTriggered(triggeredBreakpoints.toArray),
        ActorVirtualIdentity.Controller
      )
    }
    isTriggered
  }

}
