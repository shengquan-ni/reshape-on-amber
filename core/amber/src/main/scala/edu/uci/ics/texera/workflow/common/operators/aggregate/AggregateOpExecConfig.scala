package edu.uci.ics.texera.workflow.common.operators.aggregate

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{FollowPrevious, ForceLocal, UseAll}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{RandomDeployment, RoundRobinDeployment}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.{AllToOne, HashBasedShuffle}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class AggregateOpExecConfig[P <: AnyRef](
    id: OperatorIdentity,
    val aggFunc: DistributedAggregation[P]
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {

    if (aggFunc.groupByFunc == null) {
      val partialLayer = new WorkerLayer(
        LayerIdentity(id, "localAgg"),
        _ => new PartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new WorkerLayer(
        LayerIdentity(id, "globalAgg"),
        _ => new FinalAggregateOpExec(aggFunc),
        1,
        ForceLocal(),
        RandomDeployment()
      )
      new Topology(
        Array(
          partialLayer,
          finalLayer
        ),
        Array(
          new AllToOne(partialLayer, finalLayer, Constants.defaultBatchSize)
        )
      )
    } else {
      val partialLayer = new WorkerLayer(
        LayerIdentity(id, "localAgg"),
        _ => new PartialAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
        UseAll(),
        RoundRobinDeployment()
      )
      val finalLayer = new WorkerLayer(
        LayerIdentity(id, "globalAgg"),
        _ => new FinalAggregateOpExec(aggFunc),
        Constants.defaultNumWorkers,
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
            x => {
              val tuple = x.asInstanceOf[Tuple]
              aggFunc.groupByFunc(tuple).hashCode()
            },
            x => {
              val tuple = x.asInstanceOf[Tuple]
              aggFunc.groupByFunc(tuple).get(0).toString()
            }
          )
        )
      )
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }

}
