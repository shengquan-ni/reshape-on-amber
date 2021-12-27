package edu.uci.ics.texera.workflow.operators.union

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class UnionOpDesc extends OperatorDescriptor {

  override def operatorExecutor: OpExecConfig = {
    new OneToOneOpExecConfig(this.operatorIdentifier, _ => new UnionOpExec())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "unions the output rows from multiple input operators",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort(allowMultiInputs = true)),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
