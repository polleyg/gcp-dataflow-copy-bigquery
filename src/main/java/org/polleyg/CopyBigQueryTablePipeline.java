package org.polleyg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

/**
 * Copies a BigQuery table from anywhere to anywhere, even across regions.
 */
public class CopyBigQueryTablePipeline {

    public static void main(String[] args) throws Exception {
        new CopyBigQueryTablePipeline().execute(args);
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("day").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("wikimedia_project").setType("STRING"));
        fields.add(new TableFieldSchema().setName("language").setType("STRING"));
        fields.add(new TableFieldSchema().setName("title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("views").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    private void execute(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        List<BigQueryTableCopy> copies = mapper.readValue(
                new File(getClass().getClassLoader().getResource("tables.yaml").getFile()),
                new TypeReference<List<BigQueryTableCopy>>() {
                });

        for (BigQueryTableCopy copy : copies) {
            PipelineOptionsFactory.register(DataflowPipelineOptions.class);
            DataflowPipelineOptions options = PipelineOptionsFactory
                    .fromArgs(args)
                    .withValidation()
                    .as(DataflowPipelineOptions.class);
            Pipeline pipeline = Pipeline.create(options);
            pipeline.apply("Read_from_US", BigQueryIO.readTableRows().from(copy.src))
                    .apply("Transform", ParDo.of(new TableRowCopyParDo()))
                    .apply("Write_to_EU", BigQueryIO.writeTableRows()
                            .to(copy.des)
                            .withCreateDisposition(CREATE_IF_NEEDED)
                            .withWriteDisposition(WRITE_TRUNCATE)
                            .withSchema(getTableSchema()));
            pipeline.run();
        }
    }

    public static class TableRowCopyParDo extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element());
        }
    }

    public static class BigQueryTableCopy {
        @JsonProperty
        public String src;
        @JsonProperty
        public String des;
    }
}
