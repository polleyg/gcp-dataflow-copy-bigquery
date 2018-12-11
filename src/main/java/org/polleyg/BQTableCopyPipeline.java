package org.polleyg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.*;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.polleyg.GCPHelpers.extractConfigPath;
import static org.polleyg.GCPHelpers.getGCSFile;
import static org.polleyg.GCPHelpers.removeConfigPathFromArgs;

/**
 * This application is designed to be used when you need to copy/transfer a BigQuery table(s) between location/region
 * e.g. copying a table from the US to the EU. If you are just copying a table(s) between the same location/region, then
 * you don't need this. Instead just use the `gcloud` CLI tool, WebUI, or the BigQuery API. Refer to the README.md for
 * details/instructions on how to use this application.
 *
 * @author Graham Polley, Matthew Grey
 */
public class BQTableCopyPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(BQTableCopyPipeline.class);
    private static final String DEFAULT_NUM_WORKERS = "1";
    private static final String DEFAULT_MAX_WORKERS = "3";
    private static final String DEFAULT_TYPE_WORKERS = "n1-standard-1";
    private static final String DEFAULT_TARGET_LOCATION = null;
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
        Config config = setConfig(args, mapper);
        DataflowPipelineOptions options = getDataflowPipelineOptions(args, config);

        LOG.info("BigQuery table copy: {}", config);

        List<Map<String, String>> copyTables = new ArrayList<>();
        for (Map<String, String> copy : config.copies) {
            if (GCPHelpers.isDatasetTableSpec(copy.get("source"))) {
                List<TableId> tableIds = GCPHelpers.getTableIds(copy.get("source"));
                tableIds.forEach(id -> copyTables.add(createTableCopyParams(GCPHelpers.getTableIdAsString(id), copy)));
            } else {
                copyTables.add(copy);
            }
            copyTables.forEach(tableCopyParams -> setupAndRunPipeline(options, Arrays.asList(tableCopyParams), config));
        }
    }

    /**
     * Use arguments and Config pojo to prepare the DataflowPipelineOptions POJO
     * @param args application input arguments
     * @param config Table config parsed from application arguments and file contents
     * @return DataflowPipelineOptions object ready to be run
     **/
    private DataflowPipelineOptions getDataflowPipelineOptions(String[] args, Config config) {
        final String[] dataflowArgs = removeConfigPathFromArgs(args);
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(dataflowArgs)
                .as(DataflowPipelineOptions.class);
        if (config.copies == null || config.copies.size() == 0) {
            throw new IllegalStateException("No table or datasets were defined for copying in the config file");
        }
        options.setProject(config.project);
        options.setRunner(GCPHelpers.getRunnerClass(config.runner));
        return options;
    }

    /**
     * Extracts the Config pojo from arguments and file content
     * @param args application input arguments
     * @param mapper application object mapper
     * @return Config pojo for tables to be copied
     **/
    private Config setConfig(String[] args, ObjectMapper mapper) throws java.io.IOException {
        String configPath = extractConfigPath(args);
        LOG.info("Fetching config from: " + configPath);
        //get from gcs if in format: "gs://[BUCKET_NAME]/[OBJECT_NAME]"
        if(configPath.startsWith("gs://")){
            configPath = getGCSFile(configPath);
        }

        return mapper.readValue(
                new File(getClass().getClassLoader().getResource(configPath).getFile()),
                new TypeReference<Config>() {
                });
    }

    /**
     * Bootstraps the Dataflow pipeline with the provided configuration. For example, sets the number of Dataflow
     * workers, zone, machine type etc. This can be configured per pipeline that does the copy. Finally, it makes
     * a call to run the actual Dataflow pipeline.
     *
     * @param options         the options used for creating the actual Dataflow pipeline
     * @param tableCopyParams a list of table copy Maps that encapsulates the copy configuration, which is defined in the YAML config
     * @param config          the YAML config, used for globally-set copy params
     */
    private void setupAndRunPipeline(final DataflowPipelineOptions options,
                                     final List<Map<String, String>> tableCopyParams,
                                     final Config config) {

        Map<String, String> pipeParams = getFullTableCopyParams(tableCopyParams.get(0), config); //use first copy command as base params for pipeline

        String exportBucket = format("%s_df_bqcopy_export_%s", options.getProject(), pipeParams.get("sourceLocation")).toLowerCase();
        String importBucket = format("%s_df_bqcopy_import_%s", options.getProject(), pipeParams.get("targetLocation")).toLowerCase();

        handleBucketCreation(exportBucket, pipeParams.get("sourceLocation"));
        handleBucketCreation(importBucket, pipeParams.get("targetLocation"));

        Pipeline pipeline = setupPipeline(options, pipeParams, exportBucket);

        tableCopyParams.forEach(tableCopy -> {

            tableCopy = getFullTableCopyParams(tableCopy, config);

            handleTargetDatasetCreation(tableCopy.get("target"), tableCopy.get("targetDatasetLocation"));

            TableSchema schema = null; //no schema is permitted
            if (Boolean.valueOf(tableCopy.get("detectSchema"))) {
                schema = GCPHelpers.getTableSchema(tableCopy.get("source"));
            }
            WriteDisposition writeDisposition = GCPHelpers.getWriteDisposition(tableCopy.get("writeDisposition"));
            addCopyToPipeline(pipeline, tableCopy.get("source"), tableCopy.get("target"), importBucket, schema, writeDisposition);
        });
        pipeline.run();
    }

    /**
     * Creates a Dataflow pipeline based on the configuration parameters
     *
     * @param options        the options used for creating the actual Dataflow pipeline
     * @param pipelineParams a Map that encapsulates the copy configuration, which is defined in the YAML config
     * @param exportBucket   the GCS bucket name that is to be used in the exporting process
     * @return the created Dataflow Pipeline
     */
    private Pipeline setupPipeline(final DataflowPipelineOptions options,
                                   final Map<String, String> pipelineParams,
                                   final String exportBucket) {

        LOG.info("Running a copy for '{}'", pipelineParams);
        int numWorkers = Integer.valueOf(pipelineParams.get("numWorkers"));
        int maxNumWorkers = Integer.valueOf(pipelineParams.get("maxNumWorkers"));
        String zone = pipelineParams.get("zone");
        String worker = pipelineParams.get("workerMachineType");

        options.setNumWorkers(numWorkers);
        options.setMaxNumWorkers(maxNumWorkers);
        options.setZone(zone);
        options.setWorkerMachineType(worker);
        options.setTempLocation(format("gs://%s/tmp", exportBucket));
        options.setStagingLocation(format("gs://%s/jars", exportBucket));
        options.setJobName(format("bq-copy-%s-to-%s-%d", pipelineParams.get("sourceLocation"), pipelineParams.get("targetLocation"), currentTimeMillis()));

        LOG.info("Running Dataflow pipeline with options '{}'", options);

        return Pipeline.create(options);
    }

    /**
     * Adds a table-copy job to a Dataflow pipeline
     *
     * @param pipeline         the schema of the BigQuery table to use and can be null
     * @param sourceTable      the source BigQuery to copy from
     * @param targetTable      the target BigQuery to copy to
     * @param importBucket     the GCS bucket name that is to be used in the importing process
     * @param schema           the schema of the table to be copied (null acceptable)
     * @param writeDisposition the write disposition if the table already exists
     */
    private void addCopyToPipeline(final Pipeline pipeline,
                                   final String sourceTable,
                                   final String targetTable,
                                   final String importBucket,
                                   final TableSchema schema,
                                   final WriteDisposition writeDisposition) {

        checkNotNull(sourceTable, "Source table cannot be null");
        checkNotNull(targetTable, "Target table cannot be null");

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
     * configured in the YAML config, then it will use this as the target dataset location. handleTargetDatasetCreation
     * performs an existance check for the dataset in the target location.
     *
     * @param targetTable           the full table spec of the target table in format [PROJECT]:[DATASET].[TABLE]
     * @param targetDatasetLocation the target dataset location and can be null
     * @return the location of the dataset
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
            //otherwise, return target dataset location
            location = targetDatasetLocation;
        }
        assert location != null;
        return location;
    }

    /**
     * Handles the creation of the target dataset in a given location. If the target dataset location is specified
     * in the YAML configuration, then it will attempt to verify that the dataset does not exist. If it does not
     * exist, it will attempt to create the dataset in BigQuery using that region. If the dataset already exists and
     * the target dataset location is specified in the YAML, then it will bail out and throw an exception. If the
     * target dataset location is not set in the YAML, it will verify that the dataset exists within the project.
     * If it does not exist, it will bail out and throw an exception
     *
     * @param targetTable           the full table spec of the target table in format [PROJECT]:[DATASET].[TABLE]
     * @param targetDatasetLocation the target dataset location and can be null
     */
    private void handleTargetDatasetCreation(final String targetTable,
                                             final String targetDatasetLocation) {
        if (targetDatasetLocation == null) {
            //target dataset/table should already exist in this case
            try {
                GCPHelpers.getDatasetLocation(targetTable);
            } catch (RuntimeException e) {
                throw new IllegalStateException("'targetDatasetLocation' wasn't specified in config, but it looks" +
                        " like the target dataset doesn't exist.");
            }
        } else {
            //otherwise, return target dataset location
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
    }

    /**
     * Creates a map of params for a single table copy pipeline, data is combined from dataset copy config & a table that was found inside the dataset
     *
     * @param tableId           the full table spec of the target table in format [PROJECT]:[DATASET].[TABLE]
     * @param datasetCopyParams a configuration map
     * @return a map of copy parameters for one table copy
     */
    private Map<String, String> createTableCopyParams(String tableId, Map<String, String> datasetCopyParams) {
        Map<String, String> params = new HashMap<>(datasetCopyParams);
        params.put("source", tableId);
        String tableName = tableId.split("\\.")[1];
        params.put("target", format("%s.%s", datasetCopyParams.get("target"), tableName));
        return params;
    }

    /**
     * Combines values in the config with values specified in a map, favouring the map values over the config values where they exist
     *
     * @param copyParams a map of copy parameters that is to override values in the config
     * @param config     the config specified in the YAML file
     * @return the combined map of parameters
     */
    private Map<String, String> getFullTableCopyParams(Map<String, String> copyParams, Config config) {
        copyParams.put("detectSchema", copyParams.getOrDefault("detectSchema", config.detectSchema));
        copyParams.put("numWorkers", copyParams.getOrDefault("numWorkers", config.numWorkers));
        copyParams.put("maxNumWorkers", copyParams.getOrDefault("maxNumWorkers", config.maxNumWorkers));
        copyParams.put("zone", copyParams.getOrDefault("zone", config.zone));
        copyParams.put("workerMachineType", copyParams.getOrDefault("workerMachineType", config.workerMachineType));
        copyParams.put("writeDisposition", copyParams.getOrDefault("writeDisposition", config.writeDisposition));
        copyParams.put("targetDatasetLocation", copyParams.getOrDefault("targetDatasetLocation", config.targetDatasetLocation));
        copyParams.put("targetLocation", getTargetDatasetLocation(copyParams.get("target"), copyParams.get("targetDatasetLocation")));
        copyParams.put("sourceLocation", GCPHelpers.getDatasetLocation(copyParams.get("source")));
        return copyParams;
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

        @JsonProperty
        public String workerMachineType = DEFAULT_TYPE_WORKERS;
        @JsonProperty
        public String numWorkers = DEFAULT_NUM_WORKERS;
        @JsonProperty
        public String maxNumWorkers = DEFAULT_MAX_WORKERS;
        @JsonProperty
        public String targetDatasetLocation = DEFAULT_TARGET_LOCATION;
        @JsonProperty
        public String zone = DEFAULT_ZONE;
        @JsonProperty
        public String writeDisposition = DEFAULT_WRITE_DISPOSITION;
        @JsonProperty
        public String detectSchema = DEFAULT_DETECT_SCHEMA;

        @Override
        public String toString() {
            return "Config{" +
                    "copies=" + copies +
                    ", project='" + project + '\'' +
                    ", runner='" + runner + '\'' +
                    ", workerMachineType='" + workerMachineType + '\'' +
                    ", numWorkers='" + numWorkers + '\'' +
                    ", maxNumWorkers='" + maxNumWorkers + '\'' +
                    ", targetDatasetLocation='" + targetDatasetLocation + '\'' +
                    ", zone='" + zone + '\'' +
                    ", writeDisposition='" + writeDisposition + '\'' +
                    ", detectSchema='" + detectSchema + '\'' +
                    '}';
        }
    }
}