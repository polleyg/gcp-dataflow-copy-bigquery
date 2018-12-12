package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.cloud.storage.BucketInfo.newBuilder;
import static java.lang.String.format;

/**
 * Some package-private helper/convenience methods for talking with GCP services. Nothing to see here,
 * move along please.
 *
 * @author Graham Polley
 */
class GCPHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(GCPHelpers.class);
    private static final BigQuery BIGQUERY = BigQueryOptions.getDefaultInstance().getService();
    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
    private static final Map<String, StorageClass> BQ_LOCATION_TO_GCS_STORAGE_CLASS = ImmutableMap.of(
            "us", StorageClass.MULTI_REGIONAL,
            "eu", StorageClass.MULTI_REGIONAL,
            "asia-northeast1", StorageClass.REGIONAL,
            "australia-southeast1", StorageClass.REGIONAL,
            "europe-west2", StorageClass.REGIONAL);

    /**
     * Retrieves the table ids of all tables within a given dataset
     *
     * @param datasetSpec the dataset spec in the format [PROJECT]:[DATASET]
     * @return a list of table ids for the given dataset
     */
    static List<TableId> getTableIds(final String datasetSpec) {
        TableReference tableReference = BigQueryHelpers.parseTableSpec(datasetSpec);
        if (tableReference.getDatasetId() == null) {
            throw new IllegalStateException(String.format("No dataset could be found for %s", datasetSpec));
        }
        LOG.debug("Discovering tables in the dataset: {}", datasetSpec);
        DatasetId datasetId = DatasetId.of(tableReference.getProjectId(), tableReference.getDatasetId());
        return StreamSupport
                .stream(BIGQUERY.listTables(datasetId).iterateAll().spliterator(), false)
                .map(table -> table.getTableId())
                .collect(Collectors.toList());

    }

    /**
     * Returns the TableId as Strings in the format [PROJECT]:[DATASET].[TABLE]
     *
     * @param tableId
     * @return
     */
    static String getTableIdAsString(final TableId tableId) {
        return format("%s:%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    /**
     * Works out the table schema of the provided BigQuery table spec. Note, it currently does not support
     * complex schemas with nested structures.
     *
     * @param tableSpec the full table spec in the format [PROJECT].[DATASET].[TABLE]
     * @return the TableSchema.
     */
    static TableSchema getTableSchema(final String tableSpec) {
        LOG.debug("Fetching schema for '{}'", tableSpec);
        TableReference ref = BigQueryHelpers.parseTableSpec(tableSpec);
        TableId tableId = TableId.of(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
        List<TableFieldSchema> fields = new ArrayList<>();
        Schema realSchema = BIGQUERY.getTable(tableId).getDefinition().getSchema();
        realSchema.getFields().forEach(f -> fields.add(
                new TableFieldSchema().setName(f.getName()).setType(f.getType().name()))
        );
        return new TableSchema().setFields(fields);
    }

    /**
     * Creates a GCS bucket
     *
     * @param bucketName the name of the bucket to create
     * @param location   the location where the bucket should be created
     * @throws StorageException
     */
    static void createGCSBucket(final String bucketName,
                                final String location) throws StorageException {
        LOG.debug("Requested to create bucket '{}' in location '{}'..", bucketName, location);
        StorageClass storageClass = BQ_LOCATION_TO_GCS_STORAGE_CLASS.get(location);
        STORAGE.create(newBuilder(bucketName)
                .setStorageClass(storageClass)
                .setLocation(location)
                .build());
        LOG.info("Successfully created bucket '{}' with storage class '{}' and in location '{}'",
                bucketName, storageClass, location);
    }

    /**
     * Creates a BigQuery dataset
     *
     * @param tableSpec the full table spec in the format [PROJECT].[DATASET].[TABLE]
     * @param location  the location where the dataset should be created
     * @throws BigQueryException
     */
    static void createBQDataset(final String tableSpec,
                                final String location) throws BigQueryException {
        TableReference ref = BigQueryHelpers.parseTableSpec(tableSpec);
        LOG.debug("Requested to create dataset '{}' in location '{}'..", ref.getDatasetId(), location);
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(ref.getProjectId(), ref.getDatasetId())
                .setLocation(location)
                .build();
        BIGQUERY.create(datasetInfo);
        LOG.info("Successfully created dataset '{}' in location '{}'", ref.getDatasetId(), location);
    }

    /**
     * Gets the location of a BigQuery dataset
     *
     * @param tableSpec the full table spec in the format [PROJECT].[DATASET].[TABLE]
     * @return the location of the dataset
     */
    static String getDatasetLocation(final String tableSpec) {
        LOG.debug("Fetching BigQuery dataset location for '{}'", tableSpec);
        TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
        DatasetId datasetId = DatasetId.of(tableReference.getProjectId(), tableReference.getDatasetId());
        return BIGQUERY.getDataset(datasetId).getLocation().toLowerCase();
    }

    /**
     * Takes a String and returns the corresponding Dataflow runner class object. Expects one of either 'dataflow' or
     * 'local'.
     *
     * @param clazz the type of Dataflow runner
     * @return the class object for the Dataflow runner.
     */
    static Class<? extends PipelineRunner<?>> getRunnerClass(final String clazz) {
        Class<? extends PipelineRunner<?>> result;
        switch (clazz) {
            case "dataflow": {
                result = DataflowRunner.class;
                break;
            }
            case "local": {
                result = DirectRunner.class;
                break;
            }
            default:
                throw new IllegalArgumentException(format("I don't know this runner: '%s'." +
                        " Use one of 'Dataflow' or 'Local'", clazz));
        }
        return result;
    }

    /**
     * Takes a String and return the corresponding Write Disposition. Expects one of either 'truncate' or 'append'.
     *
     * @param writeDisposition
     * @return the Write Disposition for the pipeline
     */
    static WriteDisposition getWriteDisposition(final String writeDisposition) {
        WriteDisposition result;
        switch (writeDisposition) {
            case "truncate": {
                result = WriteDisposition.WRITE_TRUNCATE;
                break;
            }
            case "append": {
                result = WriteDisposition.WRITE_APPEND;
                break;
            }
            default:
                throw new IllegalArgumentException(format("I don't know this write disposition: '%s'." +
                        " Use one of 'truncate' or 'append'", writeDisposition));
        }
        return result;
    }

    /**
     * Determines if the table spec ( [PROJECT]:[DATASET].[TABLE] ) is that of an entire dataset or a single table
     *
     * @param spec the table spec to test
     * @return boolean true if spec is for a dataset
     */
    static Boolean isDatasetTableSpec(String spec) {
        return BigQueryHelpers.parseTableSpec(spec).getTableId() == null;
    }

    /**
     * Pull a file from GCS and write it out to /tmp/
     * @param gcsPath location of a file in GCS in "gs://[BUCKET_NAME]/[OBJECT_NAME]" format
     * @return string absolute path of file written out to /tmp/
     **/
    static String getGCSFile(String gcsPath) {
        String path = gcsPath.replace("gs://","");
        Storage gcs = StorageOptions.getDefaultInstance().getService();

        //bucketname should be first element in string with format "gs://[BUCKET_NAME]/[OBJECT_NAME]"
        String[] sections = path.split("/");
        if(sections.length < 2){
            LOG.info(Arrays.toString(sections));
            final String errMsg = "GCS Url must be in format 'gs://[BUCKET_NAME]/[OBJECT_NAME]'. Found: " + gcsPath;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        String bucketName = sections[0]; //get bucketname
        String srcFilePath = path.replaceFirst(bucketName + "/", ""); //remove bucketname and rejoin
        String srcFilename = sections[sections.length - 1];

        LOG.info(String.format("Bucketname: %s, srcPath: %s, srcFile: %s",bucketName,srcFilePath,srcFilename));


        Blob blob = gcs.get(BlobId.of(bucketName, srcFilePath));
        blob.downloadTo(Paths.get(srcFilename));

        return srcFilename;

    }

    /**
     * Extracts value of the configPath from the array of input args
     * @param args application input arguments
     * @return string value of the configPath argument
     **/
    static String extractConfigPath(String[] args) {
        final String configString = "--configPath=";

        // Remove our argument(s) from the set before binding to beam arguments
        for (String arg : args) {
            if (arg.startsWith(configString)) {
                return arg.replaceFirst(configString, "");  // remove config key from argument
            }
        }
        throw new RuntimeException("No configPath found"); // We shouldn't ever have this thrown as its defaulted*/
    }

    /**
     * Removes the string key and value of the configPath from the array of input args
     * @param args application input arguments
     * @return string[] args value with configPath removed
     **/
    static String[] removeConfigPathFromArgs(String[] args){
        final String configString = "--configPath=";

        // Remove our argument(s) from the set before binding to beam arguments
        for (String arg : args) {
            if (arg.startsWith(configString)) {
                return ArrayUtils.removeElement(args, arg);
            }
        }

        return args;
    }
}
