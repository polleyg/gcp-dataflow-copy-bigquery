# Uses Cloud Build to run a Dataflow pipeline that can copy a BigQuery table(s) from anywhere to anywhere, even across regions my friends.
See `cloudbuild.yaml` for an idea of what this CI/CD pipeline does. Essentially it runs a container for each step
of the build using Cloud Build and deploys each component of the pipeline to GCP.
