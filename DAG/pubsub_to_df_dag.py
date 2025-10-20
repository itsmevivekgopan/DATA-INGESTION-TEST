import base64
import json
from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartJavaJobOperator,
)
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubAcknowledgeOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubSubscriptionSensor
from airflow.utils.dates import days_ago


GCP_PROJECT_ID = "totemic-inquiry-475408-u0"
GCP_REGION = "europe-west2"
GCP_CONN_ID = "google_cloud_default"

# The Pub/Sub subscription to listen to (must already exist)
PUBSUB_SUBSCRIPTION = "projects/totemic-inquiry-475408-u0/subscriptions/dag-trigger-sub"

# The GCS path to your deployable Beam JAR file
DATAFLOW_JAR_GCS_PATH = "gs://artifact_bucket_ing_1990/artifacts/jars/original-SampleTask-1.0-SNAPSHOT.jar"
# The GCS path for Dataflow to use as a temporary staging location
DATAFLOW_TEMP_GCS_PATH = "gs://data_bucket_ing_1990/df-job/"

# The *main class* in your JAR file to execute
DATAFLOW_JAVA_MAIN_CLASS = "org.example.DataIngestionPipeline"
# -----------------------------------------------


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


@task(doc_md="Parses the Pub/Sub message to extract file name and ack_id.")
def parse_pubsub_message(pulled_messages: list) -> dict:
    """
    Pulls the first message from the PubSubPullOperator's output,
    decodes it, and extracts the 'name'.

    The expected message format is a JSON string like:
    {
        "name": "inbound/transactions/transaction.csv"
    }

    Returns a dictionary containing the file_name and the ack_id.
    """
    if not pulled_messages:
        raise ValueError("No messages pulled from subscription.")

    # We only process one message at a time, as requested
    message = pulled_messages[0]
    ack_id = message.ack_id

    print(f"Received message with ID: {message.message.message_id}")

    try:
        # 1. Decode base64 data
        data_bytes = base64.b64decode(message.message.data)
        # 2. Decode bytes to UTF-8 string
        data_str = data_bytes.decode("utf-8")
        # 3. Parse string as JSON
        data_json = json.loads(data_str)
    except Exception as e:
        print(f"Error decoding or parsing message data: {e}")
        raise ValueError("Failed to parse Pub/Sub message payload.")

    # 4. Extract the file name
    file_name = data_json.get("name")
    if not file_name:
        raise ValueError("'fileName' key not found in message JSON payload.")

    print(f"Successfully extracted file name: {file_name}")

    return {"file_name": file_name, "ack_id": ack_id}


# --- DAG Definition ---
with DAG(
    dag_id="gcp_pubsub_to_dataflow_java_beam",
    default_args=default_args,
    # This DAG runs every 5 minutes, checking for a message.
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["gcp", "pubsub", "dataflow", "beam"],
    doc_md="""
    This DAG listens to a Pub/Sub subscription for a message containing a file name,
    then triggers a Java Apache Beam Dataflow job with that file name as a
    pipeline option.
    """,
) as dag:

    # Task 1: Wait for a message to arrive in the subscription.
    # This sensor pokes the subscription. If it finds no message within
    # its timeout, it will be marked as 'skipped' (soft_fail=True),
    # and the DAG run will end successfully without running downstream tasks.
    wait_for_message = PubSubSubscriptionSensor(
        task_id="wait_for_message",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=1,
        gcp_conn_id=GCP_CONN_ID,
        poke_interval=15,  # Check every 15 seconds
        timeout=270,       # Wait for 4.5 minutes (just under the 5-min schedule)
        soft_fail=True,    # If no message, skip downstream tasks

    )

    # Task 2: Pull the message(s) that the sensor detected.
    # This task will only run if 'wait_for_message' succeeds.
    pull_message = PubSubPullOperator(
        task_id="pull_message",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=1,  # Pull just one message to process
        return_immediately=True, # We know a message is waiting
        gcp_conn_id=GCP_CONN_ID,
    )

    # Task 3: Parse the message using the Python @task defined above.
    # This pulls the XCom result from the 'pull_message' task.
    parsed_data = parse_pubsub_message(pulled_messages=pull_message.output)

    # Task 4: Trigger the Java Dataflow job.
    # It uses Jinja templating to pull the 'file_name' from the 'parsed_data' task's XCom.
    trigger_dataflow_java_job = DataflowStartJavaJobOperator(
        task_id="trigger_dataflow_java_job",
        job_name=f"beam-job-{{{{ ds_nodash }}}}-{{{{ task_instance_key_str | lower }}}}",
        jar=DATAFLOW_JAR_GCS_PATH,
        job_class=DATAFLOW_JAVA_MAIN_CLASS,
        location=GCP_REGION,
        gcp_conn_id=GCP_CONN_ID,
        # Define your Java Beam pipeline options here
        options={
            "project": GCP_PROJECT_ID,
            "runner": "DataflowRunner",
            "region": GCP_REGION,
            "tempLocation": DATAFLOW_TEMP_GCS_PATH,
            "inputFile": 'gs://data_bucket_ing_1990/{{ ti.xcom_pull(task_ids="parse_pubsub_message")["file_name"] }}',
            "dataType": 'transaction',
            "outputTable": 'totemic-inquiry-475408-u0.transaction_analytics.transactions_raw',
            "workerMachineType":'n2-standard-2'
        },
    )

    # Task 5: Acknowledge the Pub/Sub message.
    ack_message = PubSubAcknowledgeOperator(
        task_id="ack_message",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        # Pull the 'ack_id' from the 'parsed_data' task's XCom
        ack_ids=['{{ ti.xcom_pull(task_ids="parse_pubsub_message")["ack_id"] }}'],
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- Define Task Dependencies ---
    # The full pipeline: Wait -> Pull -> Parse -> Trigger Job -> Acknowledge
    (
        wait_for_message
        >> pull_message
        >> parsed_data
        >> trigger_dataflow_java_job
        >> ack_message
    )