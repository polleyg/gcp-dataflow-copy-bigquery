package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * Works out the table schema of the provided BigQuery table spec. Note, it currently does not support
     * complex schemas with nested structures.
     *
     * @param tableSpec the full table spec in the format [PROJECT].[DATASET].[TABLE]
     * @return the TableSchema.
     */
    static TableSchema getTableSchema(final String tableSpec) {
        LOG.info("Fetching schema for '{}'", tableSpec);
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
        LOG.info("Requested to create bucket '{}' in location '{}'..", bucketName, location);
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
        LOG.info("Requested to create dataset '{}' in location '{}'..", ref.getDatasetId(), location);
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
        LOG.info("Fetching BigQuery dataset location for '{}'", tableSpec);
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
}
