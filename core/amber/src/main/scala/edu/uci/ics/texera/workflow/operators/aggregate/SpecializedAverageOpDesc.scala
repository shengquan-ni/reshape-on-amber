package edu.uci.ics.texera.workflow.operators.aggregate

import java.io.Serializable
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpDesc, AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAverageOpDesc extends AggregateOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Aggregation Function")
  @JsonPropertyDescription("sum, count, average, min, or max")
  var aggFunction: AggregationFunction = _

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to calculate average value")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "result attribute", required = true)
  @JsonPropertyDescription("column name of average result")
  var resultAttribute: String = _

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  var groupByKeys: List[String] = _

  override def operatorExecutor: AggregateOpExecConfig[_] = {
    if (aggFunction.equals(AggregationFunction.AVERAGE)) {
      averageAgg()
    } else if (aggFunction.equals(AggregationFunction.COUNT)) {
      countAgg()
    } else if (aggFunction.equals(AggregationFunction.SUM)) {
      sumAgg()
    } else if (aggFunction.equals(AggregationFunction.MIN)) {
      minAgg()
    } else if (aggFunction.equals(AggregationFunction.MAX)) {
      maxAgg()
    } else {
      throw new UnsupportedOperationException("unknown aggregation function: " + this.aggFunction)
    }
  }

  def groupByFunc(): Tuple => Tuple = {
    if (this.groupByKeys == null) null
    else
      tuple => {
        val builder = Tuple.newBuilder()
        groupByKeys.foreach(key => builder.add(tuple.getSchema.getAttribute(key), tuple.getField(key)))
        builder.build()
      }
  }

  def sumAgg(): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => 0,
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        partial + value
      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, partial).build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation
    )
  }

  def countAgg(): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        partial + 1
      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple.newBuilder.add(resultAttribute, AttributeType.INTEGER, partial).build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[Integer](
      operatorIdentifier,
      aggregation
    )
  }

  def minAgg(): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MaxValue,
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        if (value < partial) value else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MaxValue) null
        else
          Tuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, partial).build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation
    )
  }

  def maxAgg(): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MinValue,
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        if (value > partial) value else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MinValue) null
        else
          Tuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, partial).build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation
    )
  }

  def averageAgg(): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[AveragePartialObj](
      () => AveragePartialObj(0, 0),
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        AveragePartialObj(partial.sum + value, partial.count + 1)
      },
      (partial1, partial2) => AveragePartialObj(partial1.sum + partial2.sum, partial1.count + partial2.count),
      partial => {
        val value = if (partial.count == 0) null else partial.sum / partial.count
        Tuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, value).build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[AveragePartialObj](
      operatorIdentifier,
      aggregation
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "calculate different types of aggregation values",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty) {
      return null
    }
    if (groupByKeys == null) {
      groupByKeys = List()
    }
    if (this.aggFunction.equals(AggregationFunction.COUNT)) {
      Schema
        .newBuilder()
        .add(
          groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*
        )
        .add(resultAttribute, AttributeType.INTEGER)
        .build()
    } else {
      Schema
        .newBuilder()
        .add(
          groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*
        )
        .add(resultAttribute, AttributeType.DOUBLE)
        .build()
    }
  }

}
