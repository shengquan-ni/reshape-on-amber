package edu.uci.ics.texera.workflow.operators.visualization.pieChart;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.curator.shaded.com.google.common.collect.Iterators;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.*;

/**
 * Simply extract relevant fields and do partial sorting.
 * @author Mingji Han, Xiaozhen Liu
 */
public class PieChartOpPartialExec implements OperatorExecutor {
    private final String nameColumn;
    private final String dataColumn;
    private List<Tuple> result;

    public PieChartOpPartialExec(String nameColumn, String dataColumn) {
        this.nameColumn = nameColumn;
        this.dataColumn = dataColumn;
    }

    @Override
    public void open() {
        result = new ArrayList<>();
    }

    @Override
    public void close() {
    }

    @Override
    public String getParam(String query) {
        return null;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            Tuple inputTuple = tuple.left().get();
            String name = inputTuple.getField(nameColumn);
            Double data;
            if (inputTuple.getSchema().getAttribute(dataColumn).getType() == AttributeType.STRING) {
                data = Double.parseDouble(inputTuple.getField(dataColumn));
            } else {
                data = inputTuple.getField(dataColumn);
            }
            Schema oldSchema = tuple.left().get().getSchema();
            Attribute dataAttribute = new Attribute(oldSchema.getAttribute(dataColumn).getName(), AttributeType.DOUBLE);
            Schema newSchema = new Schema(Arrays.asList(oldSchema.getAttribute(nameColumn), dataAttribute));
            result.add(Tuple.newBuilder().add(newSchema, Arrays.asList(name, data)).build());
            return JavaConverters.asScalaIterator(Iterators.emptyIterator());
        }
        else {
            result.sort((left, right) -> {
                double leftValue = left.getDouble(1);
                double rightValue = right.getDouble(1);
                return Double.compare(rightValue, leftValue);
            });
            return JavaConverters.asScalaIterator(result.iterator());
        }
    }
}
