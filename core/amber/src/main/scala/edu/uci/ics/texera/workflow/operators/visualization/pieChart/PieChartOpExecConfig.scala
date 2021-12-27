package edu.uci.ics.texera.workflow.operators.visualization.pieChart

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{FollowPrevious, UseAll}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.HashBasedShuffle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class PieChartOpExecConfig(
    tag: OperatorIdentity,
    val numWorkers: Int,
    val nameColumn: String,
    val dataColumn: String,
    val pruneRatio: Double
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    val partialLayer = new WorkerLayer(
      LayerIdentity(tag, "localPieChartProcessor"),
      _ => new PieChartOpPartialExec(nameColumn, dataColumn),
      numWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    val finalLayer = new WorkerLayer(
      LayerIdentity(tag, "globalPieChartProcessor"),
      _ => new PieChartOpFinalExec(pruneRatio),
      1,
      FollowPrevious(),
      RoundRobinDeployment()
    )
    new Topology(
      Array(
        partialLayer,
        finalLayer
      ),
      Array(
        new HashBasedShuffle(
          partialLayer,
          finalLayer,
          Constants.defaultBatchSize,
          x => x.asInstanceOf[Tuple].hashCode(),
          x => x.asInstanceOf[Tuple].get(0).toString()
        )
      )
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }

}
