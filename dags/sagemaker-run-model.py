from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


from datetime import datetime, timedelta

"""
This DAG shows an example implementation of executing predictions from a machine learning model using AWS SageMaker.
The DAG assumes that a SageMaker model has already been created, and runs the one-time batch inference job
using SageMaker batch transform. This method is useful if you don't have a hosted model endpoint and want
to run ad-hoc predictions when data becomes available.

The DAG uploads a local dataset from the /include directory to S3, then submits a Batch Transform job to SageMaker
to get model inference on that data. The inference results are saved to the S3 Output Path given in the config.
Finally, the inference results are uploaded to a table in Redshift using the S3 to Redshift transfer operator.

To use the DAG, add Airflow variables for `s3_bucket` (S3 Bucket used with SageMaker instance) then fill in the 
information directly below with the target AWS S3 locations, and model and model name.
"""

# Define variables used in config and Python function
date = '{{ ds_nodash }}'                                                     # Date for transform job name

test_s3_key = 'demo-sagemaker-xgboost-adult-income-prediction/test/test.csv' # Test data S3 key
output_s3_key = 'demo-sagemaker-xgboost-adult-income-prediction/output/'     # Model output data S3 key
sagemaker_model_name = "sagemaker-xgboost-2022-09-12-19-09-15-587"           # SageMaker model name


with DAG(
    'sagemaker_model',
    start_date=datetime(2021, 7, 31),
    max_active_runs=1,
    schedule_interval=None,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'aws_conn_id': 'aws-sagemaker'
    },
    catchup=False
) as dag:

    @task
    def upload_data_to_s3(s3_bucket, test_s3_key):
        """
        Uploads validation data to S3 from /include/data
        """
        s3_hook = S3Hook(aws_conn_id='aws-sagemaker')

        # Take string, upload to S3 using predefined method
        s3_hook.load_file(filename='include/data/test.csv',
                        key=test_s3_key,
                        bucket_name=s3_bucket,
                        replace=True)

    upload_data = upload_data_to_s3("{{ var.value.get('s3_bucket') }}", test_s3_key)

    predict = SageMakerTransformOperator(
        task_id='predict',
        config={
            "DataProcessing": {
                "JoinSource": "Input",
            },
            "TransformJobName": "test-sagemaker-job-{0}".format(date),
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType":"S3Prefix",
                        "S3Uri": "s3://{0}/{1}".format("{{ var.value.get('s3_bucket') }}", test_s3_key)
                    }
                },
                "SplitType": "Line",
                "ContentType": "text/csv",
            },
            "TransformOutput": {
                "S3OutputPath": "s3://{0}/{1}".format("{{ var.value.get('s3_bucket') }}", output_s3_key),
                "Accept": "text/csv",
                "AssembleWith": "Line"
            },
            "TransformResources": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.large"
            },
            "ModelName": sagemaker_model_name
        }
    )

    results_to_redshift = S3ToRedshiftOperator(
            task_id='save_results',
            s3_bucket="{{ var.value.get('s3_bucket') }}",
            s3_key=output_s3_key,
            schema="PUBLIC",
            table="results",
            copy_options=['csv'],
        )

    upload_data >> predict >> results_to_redshift
