# An application that uses Cloud Dataflow and Cloud Build to copy/transfer BigQuery tables between locations/regions.
https://medium.com/weareservian/how-to-transfer-bigquery-tables-between-locations-with-cloud-dataflow-9582acc6ae1d

This is an Apache 2.0 license. Feel free to fork, change, or basically do whatever you want with this
repo. PRs more than welcome.

## What's the tech?
 - Java (Cloud Dataflow & BigQuery/GCS APIs)
 - Gradle (build)
 - Cloud Build (CI/CD)

## Why did you build it?
Unfortunately, there's no easy/native way to do copy tables between locations/regions directly in BigQuery. For example,
you can't just copy a table from the US to the EU without jumping through a few hoops.

The process is convoluted. For example, to copy a table from EU to Sydney:

 1. Export BigQuery table to a bucket located in the EU
 2. Copy/sync the exported table from the EU bucket to another bucket located in Sydney
 3. Load into BigQuery from Sydney bucket

You can roll your own solution for this (e.g. bash + gcloud), or there's currently 2 patterns available, which do it 
for you:

 1. Use Cloud Composer (Airflow)
 2. Use Cloud Dataflow (Beam)
 
This is the repo for Cloud Dataflow option.

## How does it work?
You basically configure some YAML with name(s) of the table(s) that you copy between regions. Then invoke Cloud Build
to build, deploy and run it. The application will handle everything for you. See `config.yaml` for details on how to
configure the application. It will create the necessary GCS buckets for you, and also in the correct locations. It can
also create the BigQuery target dataset if you can't be bothered manually creating it beforehand.

You can specify for the job to copy a list of individual tables from one region to another, or copy an entire dataset 
from one region to another.

## How do I run it?
 1. Make sure all the relevant APIs are enabled on your GCP project. These include Cloud Dataflow,
BigQuery, GCS, and Cloud Build.
 2. Elevate permissions on the Cloud Build service account that was created for you by Google. It will look something
like `<your_project_number>@cloudbuild.gserviceaccount.com`. You can give it only the required permissions for each
service, or simply give it the `Project Editor` role if you're comfortable with that.
 3. Clone the GitHub repo and make the necessary changes to `config.yaml`
 4. Finally, `gcloud builds submit --config=cloudbuild.yaml <path_to_repo>` 

## Any known limitations?
 Complex schemas are not supported e.g. nested records etc. If you have a complex schema, then create an empty table
 in the target dataset with the schema and set the flag `detectSchema` to `false` in the YAML config for the
 appropriate copy, and the application will skip trying to detect the schema.
 
## Can I contact you if I need some help?
Sure. Email me at `polleyg@gmail.com`

