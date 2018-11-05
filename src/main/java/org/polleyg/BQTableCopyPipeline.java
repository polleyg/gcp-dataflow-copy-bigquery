package org.polleyg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.StorageException;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;

/**
 * This application is designed to be used when you need to copy/transfer a BigQuery table(s) between location/region
 * e.g. copying a table from the US to the EU. If you are just copying a table(s) between the same location/region, then
 * you don't need this. Instead just use the `gcloud` CLI tool, WebUI, or the BigQuery API. Refer to the README.md for
 * details/instructions on how to use this application.
 *
 * @author Graham Polley
 */
public class BQTableCopyPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(BQTableCopyPipeline.class);
    private static final String DEFAULT_NUM_WORKERS = "1";
    private static final String DEFAULT_MAX_WORKERS = "3";
    private static final String DEFAULT_TYPE_WORKERS = "n1-standard-1";
    private static final String DEFAULT_ZONE = "australia-southeast1-a";
    private static final String DEFAULT_WRITE_DISPOSITION = "truncate";
    private static final String DEFAULT_DETECT_SCHEMA = "true";

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new BQTableCopyPipeline().copy(args);
    }

    /**
     * Kicks off the copying process by reading the YAML config and creating the initial DataflowPipelineOptions
     * that will be used by all subsequent Dataflow pipelines. Each pipeline will share the same project and runner,
     * but each pipeline can be configured differently, depending on user requirements.
     *
     * @param args
     * @throws Exception
     */
    private void copy(final String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Config config = mapper.readValue(
                new File(getClass().getClassLoader().getResource("config.yaml").getFile()),
                new TypeReference<Config>() {
                });
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .as(DataflowPipelineOptions.class);
        options.setProject(config.project);
        options.setRunner(GCPHelpers.getRunnerClass(config.runner));
        LOG.info("Project set to '{}' and runner set to '{}'", config.project, config.runner);
        config.copies.forEach(copy -> setupAndRunPipeline(options, copy));
    }

    /**
     * Bootstraps the Dataflow pipeline with the provided configuration. For example, sets the number of Dataflow
     * workers, zone, machine type etc. This can be configured per pipeline that does the copy. Finally, it makes
     * a call to run the actual Dataflow pipeline.
     *
     * @param options the options used for creating the actual Dataflow pipeline
     * @param copy    a Map that encapsulates the copy configuration, which is defined in the YAML config
     */
    private void setupAndRunPipeline(final DataflowPipelineOptions options,
                                     final Map<String, String> copy) {
        LOG.info("Running a copy for '{}'", copy);
        String sourceTable = checkNotNull(copy.get("source"), "Source table cannot be null");
        String targetTable = checkNotNull(copy.get("target"), "Target table cannot be null");

        int numWorkers = Integer.valueOf(copy.getOrDefault("numWorkers", DEFAULT_NUM_WORKERS));
        int maxNumWorkers = Integer.valueOf(copy.getOrDefault("maxNumWorkers", DEFAULT_MAX_WORKERS));
        boolean detectSchema = Boolean.valueOf(copy.getOrDefault("detectSchema", DEFAULT_DETECT_SCHEMA));
        String zone = copy.getOrDefault("zone", DEFAULT_ZONE);
        String worker = copy.getOrDefault("workerMachineType", DEFAULT_TYPE_WORKERS);
        WriteDisposition writeDisposition = GCPHelpers.getWriteDisposition(copy.getOrDefault("writeDisposition", DEFAULT_WRITE_DISPOSITION));
        String targetDatasetLocation = copy.getOrDefault("targetDatasetLocation", null);

        options.setNumWorkers(numWorkers);
        options.setMaxNumWorkers(maxNumWorkers);
        options.setZone(zone);
        options.setWorkerMachineType(worker);

        TableSchema schema = null; //no schema is permitted
        if (detectSchema) {
            schema = GCPHelpers.getTableSchema(sourceTable);
        }
        runPipeline(options, schema, sourceTable, targetTable, targetDatasetLocation, writeDisposition);
    }

    /**
     * Works out if GCS buckets need to be created and also the target dataset in BigQuery. Sets the GCS import and
     * export buckets on the DataflowPipelineOptions object, which is specific to this pipeline. Then it executes the
     * Dataflow pipeline to run.
     *
     * @param options               the options used for creating the actual Dataflow pipeline
     * @param schema                the schema of the BigQuery table to use and can be null
     * @param sourceTable           the source BigQuery to copy from
     * @param targetTable           the target BigQuery to copy to
     * @param targetDatasetLocation if configured, the location/region of the target dataset in BigQuery
     * @param writeDisposition
     */
    private void runPipeline(final DataflowPipelineOptions options,
                             final TableSchema schema,
                             final String sourceTable,
                             final String targetTable,
                             final String targetDatasetLocation,
                             final WriteDisposition writeDisposition) {
        String targetLocation = getTargetDatasetLocation(targetTable, targetDatasetLocation);
        String sourceLocation = GCPHelpers.getDatasetLocation(sourceTable);

        String exportBucket = format("%s_df_bqcopy_export_%s", options.getProject(), sourceLocation);
        String importBucket = format("%s_df_bqcopy_import_%s", options.getProject(), targetLocation);

        handleBucketCreation(exportBucket, sourceLocation);
        handleBucketCreation(importBucket, targetLocation);

        options.setTempLocation(format("gs://%s/tmp", exportBucket));
        options.setStagingLocation(format("gs://%s/jars", exportBucket));
        options.setJobName(format("bq-table-copy-%s-to-%s-%d", sourceLocation, targetLocation, currentTimeMillis()));

        LOG.info("Running Dataflow pipeline with options '{}'", options);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<TableRow> rows = pipeline.apply(format("Read: %s", sourceTable), BigQueryIO.readTableRows().from(sourceTable));
        if (schema != null) {
            rows.apply(format("Write: %s", targetTable), BigQueryIO.writeTableRows()
                    .to(targetTable)
                    .withCreateDisposition(CREATE_IF_NEEDED)
                    .withWriteDisposition(writeDisposition)
                    .withSchema(schema)
                    .withCustomGcsTempLocation(StaticValueProvider.of((format("gs://%s", importBucket)))));
        } else {
            rows.apply(format("Write: %s", targetTable), BigQueryIO.writeTableRows()
                    .to(targetTable)
                    .withCreateDisposition(CREATE_NEVER)
                    .withWriteDisposition(writeDisposition)
                    .withCustomGcsTempLocation(StaticValueProvider.of((format("gs://%s", importBucket)))));
        }
        pipeline.run();
    }


    /**
     * Wraps the creation of the GCS bucket in a try/catch block. If the bucket already exists it will swallow up the
     * exception because that's ok. Otherwise, it will rethrow it.
     *
     * @param name     the name of the GCS bucket
     * @param location the location of the GCS bucket e.g. "US"
     */
    private void handleBucketCreation(final String name,
                                      final String location) {
        try {
            GCPHelpers.createGCSBucket(name, location);
        } catch (StorageException e) {
            if (e.getCode() != HttpStatus.SC_CONFLICT) { // 409 == bucket already exists. That's ok.
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Determines the location of the target dataset. If it has not configured in the YAML config, then it is assumed
     * the target dataset exists. If it doesn't, then it will bail out and throw an exception. If it has been
     * configured in the YAML config, then it will attempt to create the dataset in BigQuery using that region. If
     * the dataset already exists, then it will bail out and throw an exception.
     *
     * @param targetTable           the full table spec of the target table in format [PROJECT]:[DATASET].[TABLE]
     * @param targetDatasetLocation the target dataset location and can be null
     * @return
     */
    private String getTargetDatasetLocation(final String targetTable,
                                            final String targetDatasetLocation) {
        String location;
        if (targetDatasetLocation == null) {
            //target dataset/table should already exist in this case
            try {
                location = GCPHelpers.getDatasetLocation(targetTable);
            } catch (RuntimeException e) {
                throw new IllegalStateException("'targetDatasetLocation' wasn't specified in config, but it looks" +
                        " like the target dataset doesn't exist.");
            }
        } else {
            //otherwise, try and create it for the user
            location = targetDatasetLocation;
            try {
                GCPHelpers.createBQDataset(targetTable, targetDatasetLocation);
            } catch (BigQueryException e) {
                if (e.getCode() == HttpStatus.SC_CONFLICT) { // 409 == dataset already exists
                    throw new IllegalStateException(
                            format("'targetDatasetLocation' specified in config, but the dataset '%s' already exists",
                                    targetTable));
                } else {
                    throw new IllegalStateException(e);
                }
            }
        }
        assert location != null;
        return location;
    }

    /**
     * POJO for YAML config
     */
    private static class Config {
        @JsonProperty
        public List<Map<String, String>> copies;
        @JsonProperty
        public String project;
        @JsonProperty
        public String runner;
    }
}
