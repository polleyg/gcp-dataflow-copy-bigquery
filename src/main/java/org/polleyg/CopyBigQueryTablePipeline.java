package org.polleyg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

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
            "australia-southeast1", StorageClass.REGIONAL,
            "europe-west2", StorageClass.REGIONAL);

    public static void main(String[] args) throws Exception {
        new CopyBigQueryTablePipeline().execute(args);
    }

    private static TableSchema getTableSchema(final String sourceTable) {
        TableReference ref = BigQueryHelpers.parseTableSpec(sourceTable);
        TableId tableId = TableId.of(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
        List<TableFieldSchema> fields = new ArrayList<>();
        Schema realSchema = BIGQUERY.getTable(tableId).getDefinition().getSchema();
        realSchema.getFields().forEach(f -> fields.add(
                new TableFieldSchema().setName(f.getName()).setType(f.getType().name()))
        );
        return new TableSchema().setFields(fields);
    }

    private void execute(final String[] args) throws Exception {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Config config = mapper.readValue(
                new File(getClass().getClassLoader().getResource("config.yaml").getFile()),
                new TypeReference<Config>() {

                });

        config.tables.forEach(table -> {
                    String src = table.get("src");
                    String des = table.get("des");
                    runDataflowCopyPipeline(
                            options,
                            getTableSchema(src),
                            src,
                            des);
                }
        );
    }

    private void runDataflowCopyPipeline(final DataflowPipelineOptions options, final TableSchema schema, final String src, final String des) {
        final String srcLocation = getDatasetLocation(src);
        final String desLocation = getDatasetLocation(des);

        final String gcsExport = format("%s_df_bqcopy_extract_%s", options.getProject(), srcLocation);
        final String gcsImport = format("%s_df_bqcopy_load_%s", options.getProject(), desLocation);

        maybeCreateBucket(gcsExport, srcLocation);
        maybeCreateBucket(gcsImport, desLocation);

        options.setTempLocation(format("gs://%s/tmp", gcsExport));
        options.setStagingLocation(format("gs://%s/jars", gcsExport));
        options.setJobName(format("bq-table-copy-%s-to-%s-%d", srcLocation, desLocation, currentTimeMillis()));

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(format("Read: %s", src), BigQueryIO.readTableRows().from(src))
                .apply(format("Write: %s", des), BigQueryIO.writeTableRows()
                        .to(des)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withSchema(schema)
                        .withCustomGcsTempLocation(StaticValueProvider.of((format("gs://%s", gcsImport)))));
        pipeline.run();
    }

    private String getDatasetLocation(final String tableSpec) {
        TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
        DatasetId datasetId = DatasetId.of(tableReference.getProjectId(), tableReference.getDatasetId());
        return BIGQUERY.getDataset(datasetId).getLocation().toLowerCase();
    }

    private void maybeCreateBucket(final String name, final String location) {
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

    //POJO for YAML config
    private static class Config {
        @JsonProperty
        public List<Map<String, String>> tables;
    }
}
