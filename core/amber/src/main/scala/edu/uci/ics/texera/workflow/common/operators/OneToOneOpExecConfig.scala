package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig

class OneToOneOpExecConfig(
    override val id: OperatorIdentity,
    val opExec: Int => OperatorExecutor
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(id, "main"),
          opExec,
          Constants.defaultNumWorkers,
          FollowPrevious(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    // TODO: take worker states into account
    topology.layers(0).identifiers
  }
}
