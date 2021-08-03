from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

"""
This DAG shows an example implementation of executing predictions from a machine learning model using AWS SageMaker.
The DAG assumes that a SageMaker model has already been created, and runs the one-time batch inference job
using SageMaker batch transform. This method is useful if you don't have a hosted model endpoint and want
to run ad-hoc predictions when data becomes available.

The DAG uploads a local dataset from the /include directory to S3, then submits a Batch Transform job to SageMaker
to get model inference on that data. The inference results are saved to the S3 Output Path given in the config.
Finally, the inference results are uploaded to a table in Snowflake using the S3 to Snowflake transfer operator.
"""

# Get execution date to use for transform job name
date = '{{ ds_nodash }}'

# Define transform config for the SageMakerTransformOperator
transform_config = {
        "TransformJobName": "test-sagemaker-job2-{0}".format(date),
        "TransformInput": { 
            "DataSource": { 
                "S3DataSource": {
                    "S3DataType":"S3Prefix", 
                    "S3Uri": "s3://sagemaker-us-east-2-559345414282/demo-sagemaker-xgboost-adult-income-prediction/data/validation.csv"
                }
            }
        },
        "TransformOutput": { 
            "S3OutputPath": "s3://sagemaker-us-east-2-559345414282/demo-sagemaker-xgboost-adult-income-prediction/output/"
        },
        "TransformResources": { 
            "InstanceCount": 1,
            "InstanceType": "ml.m5.large"
        },
        "ModelName": "sagemaker-xgboost-2021-08-03-23-25-30-873"
    }


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG('sagemaker_model',
         start_date=datetime(2021, 7, 31),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    # @task
    # def upload_data_to_s3():
    #     """
    #     Uploads validation data to S3 from /include/data 
    #     """
    #     s3_hook = S3Hook(aws_conn_id='aws-sagemaker')

    #     # Take string, upload to S3 using predefined method
    #     s3_hook.load_file(filename='include/data/linear_validation.data', 
    #                     key='sagemaker/DEMO-breast-cancer-prediction/validation/linear_validation.data', 
    #                     bucket_name='sagemaker-us-east-2-559345414282', 
    #                     replace=True)

    # upload_data = upload_data_to_s3()

    predict = SageMakerTransformOperator(
        task_id='predict',
        config=transform_config,
        aws_conn_id='aws-sagemaker'
    )

    # results_to_snowflake = 

    # upload_data >> predict #>> results_to_snowflake