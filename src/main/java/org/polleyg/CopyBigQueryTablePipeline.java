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
import com.google.common.collect.ImmutableMap;
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
import java.util.Map;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

/**
 * Copies a BigQuery table from anywhere to anywhere, even across regions.
 */
public class CopyBigQueryTablePipeline {
    private static final BigQuery BIGQUERY = BigQueryOptions.getDefaultInstance().getService();
    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
    private static final Map<String, StorageClass> BQ_REGION_TO_GCS_BUCKET_TYPE = ImmutableMap.of(
            "us", StorageClass.MULTI_REGIONAL,
            "eu", StorageClass.MULTI_REGIONAL,
            "asia-northeast1", StorageClass.REGIONAL,
            "europe-west2", StorageClass.REGIONAL);

    public static void main(String[] args) throws Exception {
        new CopyBigQueryTablePipeline().execute(args);
    }

    private static TableSchema getTableSchema(Map<String, String> schema) {
        List<TableFieldSchema> fields = new ArrayList<>();
        schema.forEach((key, val) -> fields.add(new TableFieldSchema().setName(key).setType(val)));
        return new TableSchema().setFields(fields);
    }

    private void execute(String[] args) throws Exception {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        List<Config> config = mapper.readValue(
                new File(getClass().getClassLoader().getResource("config.yaml").getFile()),
                new TypeReference<List<Config>>() {
                });
        config.forEach(configuration -> {
            TableSchema schema = getTableSchema(configuration.schema);
            configuration.tables.forEach(copy -> runDataflowCopyPipeline(
                    options,
                    schema,
                    copy.get("source"),
                    copy.get("destination")));
        });
    }

    private void runDataflowCopyPipeline(DataflowPipelineOptions options, TableSchema schema, String source, String destination) {
        String srcDatasetLocation = getDatasetLocation(source);
        String desDatasetLocation = getDatasetLocation(destination);

        String gcsExtract = format("%s_df_bqcopy_extract_%s", options.getProject(), srcDatasetLocation);
        maybeCreateBucket(gcsExtract, srcDatasetLocation);

        String gcsLoad = format("%s_df_bqcopy_load_%s", options.getProject(), desDatasetLocation);
        maybeCreateBucket(gcsLoad, desDatasetLocation);

        options.setTempLocation(format("gs://%s/tmp", gcsExtract));
        options.setStagingLocation(format("gs://%s/jars", gcsExtract));
        options.setJobName(format("bq-table-copy-%s-to-%s-%d", srcDatasetLocation, desDatasetLocation, currentTimeMillis()));

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(format("Read: %s", source), BigQueryIO.readTableRows().from(source))
                .apply("Transform", ParDo.of(new TableRowCopyParDo()))
                .apply(format("Write: %s", destination), BigQueryIO.writeTableRows()
                        .to(destination)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withSchema(schema)
                        .withCustomGcsTempLocation(StaticValueProvider.of((format("gs://%s", gcsLoad)))));
        pipeline.run();
    }

    private String getDatasetLocation(String table) {
        String datasetName = BigQueryHelpers.parseTableSpec(table).getDatasetId();
        return BIGQUERY.getDataset(DatasetId.of(datasetName)).getLocation().toLowerCase();
    }

    private void maybeCreateBucket(String name, String location) {
        try {
            STORAGE.create(
                    BucketInfo.newBuilder(name)
                            .setStorageClass(BQ_REGION_TO_GCS_BUCKET_TYPE.get(location))
                            .setLocation(location)
                            .build()
            );
        } catch (StorageException e) {
            if (e.getCode() != 409) throw new RuntimeException(e); //409 == bucket already exists
        }
    }

    private static class TableRowCopyParDo extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element()); //1:1 copy
        }
    }

    //POJO for YAML config
    private static class Config {
        @JsonProperty
        public List<Map<String, String>> tables;
        @JsonProperty
        public Map<String, String> schema;
    }
}
