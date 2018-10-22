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
import org.apache.http.HttpStatus;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

        if(Objects.isNull(config.tables) || config.tables.isEmpty()){
            throw new RuntimeException("No BQ tables parsed correctly");
        }

        config.tables.forEach(table -> {
                    String src = table.get("source");
                    String des = table.get("destination");
                    String desRegion = table.getOrDefault("desRegion", null);
                    runDataflowCopyPipeline(
                            options,
                            getTableSchema(src),
                            src,
                            des, //table reference
                            desRegion); //region if destination data set doesn't exist
                }
        );
    }

    private void runDataflowCopyPipeline(final DataflowPipelineOptions options, final TableSchema schema, final String src, final String des, final String desRegion) {
        final String srcLocation = getDataset(src).getLocation().toLowerCase();
        final String desLocation = maybeCreateDataset(des, desRegion).getLocation().toLowerCase();

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

    private Dataset maybeCreateDataset(final String tableName, String desRegion) {
        Dataset ds = getDataset(tableName);

        if(Objects.nonNull(ds)) {
            return ds;
        }

        return createDataSet(tableName, desRegion);
    }

    private Dataset createDataSet(String tableName, String desRegion) {
        // inputs should be in format `Project:Dataset.Table` or `Dataset.Table` Project is optional.
        // split on period or colon
        String[] referenceSplit =tableName.split( "[\\.:]");

        //if our string has weird formatting, or no destination region is defined rethrow
        if( referenceSplit.length < 2 || referenceSplit.length > 3){
            throw new IllegalArgumentException(
                    "Table reference is not in [project_id]:[dataset_id].[table_id] "
                            + "format: "
                            + tableName);
        }

        //if the dataset doesn't exist create it with the parsed name
        // inputs are in format `Project:Dataset.Table` or `Dataset.Table` Project is optional.
        String datasetName = (referenceSplit.length == 3) ? referenceSplit[1] : referenceSplit[2];

        DatasetInfo datasetInfo = DatasetInfo
                .newBuilder(datasetName)
                .setLocation(desRegion.toLowerCase())
                .build();
        return BIGQUERY.create(datasetInfo);
    }


    private Dataset getDataset(final String tableSpec) {
        TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
        DatasetId datasetId = DatasetId.of(tableReference.getProjectId(), tableReference.getDatasetId());
        return BIGQUERY.getDataset(datasetId);
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
            if (e.getCode() != HttpStatus.SC_CONFLICT) throw new RuntimeException(e); //409 == bucket already exists
        }
    }

    //POJO for YAML config
    private static class Config {

        @Override
        public String toString() {
            return tables.toString();
        }

        //package private is fine here
        @JsonProperty
        List<Map<String, String>> tables;
    }
}
