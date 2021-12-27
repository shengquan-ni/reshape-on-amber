package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.{InputExhausted, WorkflowLogger}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import org.apache.avro.SchemaBuilder
import java.util

import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinOpExec[K](
    val buildTable: LinkIdentity,
    val buildAttributeName: String,
    val probeAttributeName: String
) extends OperatorExecutor {

  var isBuildTableFinished: Boolean = false
  var buildTableHashMap: mutable.HashMap[K, ArrayBuffer[Tuple]] = _
  var outputProbeSchema: Schema = _

  var currentEntry: Iterator[Tuple] = _
  var currentTuple: Tuple = _

  def getBuildHashTable(): ArrayBuffer[mutable.HashMap[K, ArrayBuffer[Tuple]]] = {
    val sendingMap = new ArrayBuffer[mutable.HashMap[K, ArrayBuffer[Tuple]]]
    var count = 1
    var curr = new mutable.HashMap[K, ArrayBuffer[Tuple]]
    for ((key, tuples) <- buildTableHashMap) {
      curr.put(key, tuples)
      if (count % 4000 == 0) {
        sendingMap.append(curr)
        curr = new mutable.HashMap[K, ArrayBuffer[Tuple]]
      }
      count += 1
    }
    if (!curr.isEmpty) sendingMap.append(curr)
    sendingMap
  }

  def addToHashTable(additionalTable: mutable.HashMap[K, ArrayBuffer[Tuple]]): Unit = {
    for ((key, tuples) <- additionalTable) {
      val existingTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
      existingTuples.appendAll(tuples)
      buildTableHashMap(key) = existingTuples
    }
  }

  // probe attribute removed in the output schema
  private def createOutputProbeSchema(buildTuple: Tuple, probeTuple: Tuple) = {
    val buildSchema = buildTuple.getSchema()
    val probeSchema = probeTuple.getSchema()
    var builder = Schema.newBuilder()
    probeSchema
      .getAttributes()
      .forEach(attr => {
        if (attr.getName() != probeAttributeName) {
          if (buildSchema.containsAttribute(attr.getName())) {
            builder.add(new Attribute(s"${attr.getName()}#@1", attr.getType()))
          } else {
            builder.add(attr)
          }
        }
      })
    builder.build()
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        // The operatorInfo() in HashJoinOpDesc has a inputPorts list. In that the
        // small input port comes first. So, it is assigned the inputNum 0. Similarly
        // the large input is assigned the inputNum 1.
        if (input == buildTable) {
          val key = t.getField(buildAttributeName).asInstanceOf[K]
          var storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
          storedTuples += t
          buildTableHashMap.put(key, storedTuples)
          Iterator()
        } else {
          if (!isBuildTableFinished) {
            val err = WorkflowRuntimeError(
              "Probe table came before build table ended",
              "HashJoinOpExec",
              Map("stacktrace" -> Thread.currentThread().getStackTrace().mkString("\n"))
            )
            throw new WorkflowRuntimeException(err)
          } else {
            val key = t.getField(probeAttributeName).asInstanceOf[K]
            val storedTuples = buildTableHashMap.getOrElse(key, new ArrayBuffer[Tuple]())
            var tuplesToOutput: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
            if (storedTuples.isEmpty) {
              return Iterator()
            }
            if (outputProbeSchema == null) {
              outputProbeSchema = createOutputProbeSchema(storedTuples(0), t)
            }

            storedTuples.foreach(buildTuple => {
              val builder = Tuple
                .newBuilder()
                .add(buildTuple)

              var newProbeIdx = 0
              // outputProbeSchema doesnt have "probeAttribute" but t does. The following code
              //  takes that into consideration while creating a tuple.
              for (i <- 0 until t.getFields.size()) {
                if (!t.getSchema().getAttributeNames().get(i).equals(probeAttributeName)) {
                  builder.add(
                    outputProbeSchema.getAttributes().get(newProbeIdx),
                    t.getFields().get(i)
                  )
                  newProbeIdx += 1
                }
              }

              tuplesToOutput += builder.build()
            })
            tuplesToOutput.iterator
          }
        }
      case Right(_) =>
        if (input == buildTable) {
          isBuildTableFinished = true
          if (buildTableHashMap.keySet.size < 13) {
            println(
              s"\tKeys in build table are: ${buildTableHashMap.keySet.mkString(", ")}"
            )
          }
        }
        Iterator()

    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ArrayBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}
