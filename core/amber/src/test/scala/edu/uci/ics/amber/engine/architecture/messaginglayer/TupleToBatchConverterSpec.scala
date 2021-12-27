package edu.uci.ics.amber.engine.architecture.messaginglayer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.OneToOnePolicy
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity

class TupleToBatchConverterSpec extends AnyFlatSpec with MockFactory {
  private val mockDataOutputPort = mock[DataOutputPort]
  private val identifier = WorkerActorVirtualIdentity("batch producer mock")

  "TupleToBatchConverter" should "aggregate tuples and output" in {
    val batchProducer = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    val fakeID = WorkerActorVirtualIdentity("testReceiver")
    inSequence {
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(0, 10)))
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(10, 20)))
      (mockDataOutputPort.sendTo _).expects(fakeID, DataFrame(tuples.slice(20, 21)))
      (mockDataOutputPort.sendTo _).expects(fakeID, EndOfUpstream())
    }
    val fakeLink = LinkIdentity(LayerIdentity("", "", ""), LayerIdentity("", "", ""))
    val fakeReceiver = Array[ActorVirtualIdentity](fakeID)

    batchProducer.addPolicy(new OneToOnePolicy(fakeLink, 10, fakeReceiver))
    tuples.foreach { t =>
      batchProducer.passTupleToDownstream(t)
    }
    batchProducer.emitEndOfUpstream()
  }

  "TupleToBatchConverter" should "not output tuples when there is no policy" in {
    val tupleToBatchConverter = wire[TupleToBatchConverter]
    val tuples = Array.fill(21)(ITuple(1, 2, 3, 4, "5", 9.8))
    (mockDataOutputPort.sendTo _).expects(*, *).never()
    tuples.foreach { t =>
      tupleToBatchConverter.passTupleToDownstream(t)
    }
    tupleToBatchConverter.emitEndOfUpstream()
  }

}
