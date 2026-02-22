from airflow import DAG
from airflow.providers.amazon.aws.operators.cloud_formation import (
    CloudFormationCreateStackOperator,
    CloudFormationDeleteStackOperator,
)
from airflow.providers.amazon.aws.sensors.cloud_formation import (
    CloudFormationCreateStackSensor,
)
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import boto3

# ---------------- DEFAULT ARGS ----------------
default_args = {
    "owner": "nihal",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AWS_CONN_ID = "aws_default"
STACK_NAME = "uber-emr-stack"

# ---------------- FUNCTION TO EXTRACT CLUSTER ID ----------------
def get_emr_cluster_id(**context):
    cf = boto3.client("cloudformation")

    response = cf.describe_stacks(StackName=STACK_NAME)

    outputs = response["Stacks"][0].get("Outputs", [])

    for output in outputs:
        if output["OutputKey"] == "EMRClusterId":
            return output["OutputValue"]

    raise ValueError("EMRClusterId not found in stack outputs")

# ---------------- DAG ----------------
with DAG(
    dag_id="uber_kafka_emr_pipeline_cf_fixed",
    default_args=default_args,
    description="Kafka → Bronze → Silver → Gold via CloudFormation",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["uber", "emr", "cloudformation"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -------- CREATE STACK --------
    create_stack = CloudFormationCreateStackOperator(
        task_id="create_stack",
        stack_name=STACK_NAME,
        cloudformation_parameters={
            "TemplateURL": "https://s3.us-east-1.amazonaws.com/uber-project1.1/cloud-formation/emr.yaml",
            "Capabilities": ["CAPABILITY_IAM"],
        },
        aws_conn_id=AWS_CONN_ID,
    )

    wait_for_stack = CloudFormationCreateStackSensor(
        task_id="wait_for_stack",
        stack_name=STACK_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    # -------- EXTRACT CLUSTER ID --------
    extract_cluster_id = PythonOperator(
        task_id="extract_cluster_id",
        python_callable=get_emr_cluster_id,
    )

    cluster_id = "{{ ti.xcom_pull(task_ids='extract_cluster_id') }}"

    # -------- BRONZE STEP --------
    bronze_step = EmrAddStepsOperator(
        task_id="bronze_step",
        job_flow_id=cluster_id,
        aws_conn_id=AWS_CONN_ID,
        steps=[{
            "Name": "Bronze Layer",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "--packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                    "s3://uber-project1.1/spark-scripts/spark_bronze.py"
                ],
            },
        }],
    )

    bronze_sensor = EmrStepSensor(
        task_id="bronze_sensor",
        job_flow_id=cluster_id,
        step_id="{{ ti.xcom_pull(task_ids='bronze_step')[0] }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # -------- SILVER STEP --------
    silver_step = EmrAddStepsOperator(
        task_id="silver_step",
        job_flow_id=cluster_id,
        aws_conn_id=AWS_CONN_ID,
        steps=[{
            "Name": "Silver Layer",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "s3://uber-project1.1/spark-scripts/spark_silver.py",
                ],
            },
        }],
    )

    silver_sensor = EmrStepSensor(
        task_id="silver_sensor",
        job_flow_id=cluster_id,
        step_id="{{ ti.xcom_pull(task_ids='silver_step')[0] }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # -------- GOLD STEP --------
    gold_step = EmrAddStepsOperator(
        task_id="gold_step",
        job_flow_id=cluster_id,
        aws_conn_id=AWS_CONN_ID,
        steps=[{
            "Name": "Gold Layer",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "s3://uber-project1.1/spark-scripts/spark_gold.py",
                ],
            },
        }],
    )

    gold_sensor = EmrStepSensor(
        task_id="gold_sensor",
        job_flow_id=cluster_id,
        step_id="{{ ti.xcom_pull(task_ids='gold_step')[0] }}",
        aws_conn_id=AWS_CONN_ID,
    )

    # -------- TERMINATE --------
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=cluster_id,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )

    # -------- DELETE STACK --------
    delete_stack = CloudFormationDeleteStackOperator(
        task_id="delete_stack",
        stack_name=STACK_NAME,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    # -------- FLOW --------
    start >> create_stack >> wait_for_stack >> extract_cluster_id \
          >> bronze_step >> bronze_sensor \
          >> silver_step >> silver_sensor \
          >> gold_step >> gold_sensor \
          >> terminate_cluster >> delete_stack >> end