package edu.uci.ics.texera.workflow.operators.localscan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.io.Files;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.operators.OpExecConfig;
import edu.uci.ics.texera.workflow.common.metadata.InputPort;
import edu.uci.ics.texera.workflow.common.metadata.OperatorGroupConstants;
import edu.uci.ics.texera.workflow.common.metadata.OperatorInfo;
import edu.uci.ics.texera.workflow.common.metadata.OutputPort;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.apache.avro.SchemaBuilder;
import scala.collection.immutable.List;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static scala.collection.JavaConverters.asScalaBuffer;


public class LocalCsvFileScanOpDesc extends SourceOperatorDescriptor {

    @JsonProperty(value = "file path", required = true)
    @JsonPropertyDescription("local file path")
    public String filePath;

    @JsonProperty(value = "delimiter", defaultValue = ",")
    @JsonPropertyDescription("delimiter to separate each line into fields")
    public String delimiter;

    @JsonProperty(value = "header", defaultValue = "true")
    @JsonPropertyDescription("whether the CSV file contains a header line")
    public Boolean header;

    @Override
    public OpExecConfig operatorExecutor() {
        // fill in default values
        if (this.delimiter == null) {
            this.delimiter = ",";
        }
        if (this.header == null) {
            this.header = true;
        }
        try {
            String headerLine = Files.asCharSource(new File(filePath), Charset.defaultCharset()).readFirstLine();
            return new LocalCsvFileScanOpExecConfig(this.operatorIdentifier(), Constants.defaultNumWorkers(),
                    filePath, delimiter.charAt(0), this.inferSchema(headerLine), header != null && header);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public OperatorInfo operatorInfo() {
        return new OperatorInfo(
                "CSV File Scan",
                "Scan data from a local CSV file",
                OperatorGroupConstants.SOURCE_GROUP(),
                List.empty(),
                asScalaBuffer(singletonList(new OutputPort(""))).toList());
    }

    @Override
    public Schema sourceSchema() {
        if (this.filePath == null) {
            return null;
        }
        try {
            String headerLine = Files.asCharSource(new File(filePath), Charset.defaultCharset()).readFirstLine();
            if (header == null) {
                return null;
            }
            return inferSchema(headerLine);
        } catch (IOException e) {
            return null;
        }
    }

    private Schema inferSchema(String headerLine) {
        if (delimiter == null) {
            return null;
        }
        if (header != null && header) {
            return Schema.newBuilder().add(Arrays.stream(headerLine.split(delimiter)).map(c -> c.trim())
                    .map(c -> new Attribute(c, AttributeType.STRING)).collect(Collectors.toList())).build();
        } else {
            if(!Constants.sortExperiment()) {
                return Schema.newBuilder().add(IntStream.range(0, headerLine.split(delimiter).length).
                        mapToObj(i -> new Attribute("column" + i, AttributeType.STRING))
                        .collect(Collectors.toList())).build();
            } else {
                String tempDelimiter = "\\|";
                Schema.Builder builder = Schema.newBuilder();
                String[] fields = headerLine.split(tempDelimiter);
                int idx = 0;
                for(String fieldVal: fields) {
                    try
                    {
                        Float.parseFloat(fieldVal);
                        builder.add(new Attribute("column" + idx, AttributeType.FLOAT));
                    }
                    catch(NumberFormatException e)
                    {
                        builder.add(new Attribute("column" + idx, AttributeType.STRING));
                    }
                    idx++;
                }
                return builder.build();
            }
        }
    }

}
