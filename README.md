# airflow-sagemaker-tutorial
This repo contains an Astronomer project with multiple example DAGs showing how to use Airflow for ML orchestration with AWS SageMaker. A guide discussing the DAGs and concepts in depth will be published shortly.

## Tutorial Overview
This tutorial has two example DAGs showing how to accomplish the following ML use cases:

 - `sagemaker-run-model`: gets inferences on a dataset from an existing SageMaker model by running a batch transform job and saves the results to Redshift.
 - `sagemaker-pipeline`: orchestrates an end-to-end ML model including obtaining and pre-processing the data, training a model, saving the model from the training artifact, and testing the model with a batch transform job.


## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
