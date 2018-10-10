package org.polleyg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.storage.*;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
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
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        List<BigQueryTableCopy> config = mapper.readValue(
                new File(getClass().getClassLoader().getResource("bq_tables.yaml").getFile()),
                new TypeReference<List<BigQueryTableCopy>>() {
                });

        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        Storage storage = StorageOptions.getDefaultInstance().getService();

        for (BigQueryTableCopy copy : config) {
            String srcDatasetLocation = getDatasetLocation(copy.src, bigQuery);
            String desDatasetLocation = getDatasetLocation(copy.des, bigQuery);

            String gcsExtract = format("%s_df_bqcopy_extract_%s", options.getProject(), srcDatasetLocation);
            maybeCreateBucket(gcsExtract, srcDatasetLocation, storage);

            String gcsLoad = format("%s_df_bqcopy_load_%s", options.getProject(), desDatasetLocation);
            maybeCreateBucket(gcsLoad, desDatasetLocation, storage);

            options.setTempLocation(format("gs://%s/tmp", gcsExtract));
            options.setStagingLocation(format("gs://%s/jars", gcsExtract));
            options.setJobName(format("bq-table-copy-%s-to-%s-%d", srcDatasetLocation, desDatasetLocation, System.currentTimeMillis()));

            Pipeline pipeline = Pipeline.create(options);
            pipeline.apply(format("Read: %s", copy.src), BigQueryIO.readTableRows().from(copy.src))
                    .apply("Transform", ParDo.of(new TableRowCopyParDo()))
                    .apply(format("Write: %s", copy.des), BigQueryIO.writeTableRows()
                            .to(copy.des)
                            .withCreateDisposition(CREATE_IF_NEEDED)
                            .withWriteDisposition(WRITE_TRUNCATE)
                            .withSchema(getTableSchema())
                            .withCustomGcsTempLocation(StaticValueProvider.of((format("gs://%s", gcsLoad)))));
            pipeline.run();
        }
    }

    private String getDatasetLocation(final String table, final BigQuery bigQuery) {
        String datasetName = BigQueryHelpers.parseTableSpec(table).getDatasetId();
        return bigQuery.getDataset(DatasetId.of(datasetName)).getLocation().toLowerCase();
    }

    private void maybeCreateBucket(final String name, final String location, final Storage storage) {
        try {
            storage.create(
                    BucketInfo.newBuilder(name)
                            .setStorageClass(StorageClass.MULTI_REGIONAL)
                            .setLocation(location)
                            .build()
            );
        } catch (StorageException e) {
            if (e.getCode() != 409) throw new RuntimeException(e);
        }
    }

    private static class TableRowCopyParDo extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element());
        }
    }

    private static class BigQueryTableCopy {
        @JsonProperty
        public String src;
        @JsonProperty
        public String des;
    }
}
