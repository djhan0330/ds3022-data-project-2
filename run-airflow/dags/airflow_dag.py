# airflow DAG goes here
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests, boto3, time

default_args = {
    "owner": "dpv8cf",
    "depends_on_past": False, 
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def get_sqs_client():
    #Creating a boto3 SQS Client using Airflow-Stored Credentials
    return boto3.client(
        "sqs",
        region_name = "us-east-1",
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
)

#Scattering Messages
def scatter_messages(ti):
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/dpv8cf"
    response = requests.post(url)
    response.raise_for_status()
    payload = response.json()
    sqs_url = payload["sqs_url"]
    print(f"SQS Queue Populated at: {sqs_url}")
    print("DEBUG: Scatter API full response:", payload)
    ti.xcom_push(key="sqs_url", value=sqs_url)

#Wait for Queue
def wait_for_messages(ti):
    sqs_url = ti.xcom_pull(task_ids = "scatter_messages", key = "sqs_url")
    sqs = get_sqs_client()

    while True:
        response = sqs.get_queue_attributes(
            QueueUrl = sqs_url,
            AttributeNames= [
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        attributes = response["Attributes"]
        visible= int(attributes["ApproximateNumberOfMessages"])
        not_visible = int(attributes["ApproximateNumberOfMessagesNotVisible"])
        delayed = int(attributes["ApproximateNumberOfMessagesDelayed"])
        total = visible + not_visible + delayed
        print(f"Queue -> visible = {visible}, not_visible = {not_visible}, delayed = {delayed}, total = {total}")
        if total >= 21 and visible >= 21:
            print("All Messages are in the Queue")
            break
        time.sleep(5)

#Receive Messages
def receive_messages(ti):
    sqs_url = ti.xcom_pull(task_ids = "scatter_messages", key = "sqs_url")
    sqs = get_sqs_client()
    all_messages = []

    while True:
        response = sqs.receive_message(
            QueueUrl = sqs_url,
            MaxNumberOfMessages = 10,
            VisibilityTimeout = 30,
            MessageAttributeNames = ["All"],
            WaitTimeSeconds = 10,
        )

        if "Messages" not in response:
            print("No More Messages are available - exiting loop.")
            break

        for message in response["Messages"]:
            attributes = message["MessageAttributes"]
            order_no = int(attributes["order_no"]["StringValue"])
            word = attributes["word"]["StringValue"]
            receipt = message["ReceiptHandle"]

            all_messages.append({"order_no": order_no, "word": word, "receipt": receipt})
            sqs.delete_message(QueueUrl = sqs_url, ReceiptHandle = receipt)

        print(f"Fetched {len(all_messages)} messages so far.")

    print("DEBUG: Total messages received:", len(all_messages))
    print("DEBUG: First few messages:", all_messages[:5])
    print("DEBUG: Using SQS URL:", sqs_url)
    ti.xcom_push(key = "all_messages", value = all_messages)
    

#Reassemble and Submission
def assemble_and_submit(ti):
    uvaid = "dpv8cf"
    platform = "airflow"
    all_messages = ti.xcom_pull(task_ids = "receive_messages", key = "all_messages")
    if not all_messages:
        print("No messages pulled from queue. Phrase will be empty.")
        return None  # prevents invalid submission

    ordered = sorted(all_messages, key = lambda msg: msg["order_no"])
    phrase = " ".join([msg["word"] for msg in ordered])
    print(f"Final Phrase: {phrase}")

    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    sqs = get_sqs_client()

    try:
        response = sqs.send_message(
            QueueUrl = submit_url,
            MessageBody = "submission",
            MessageAttributes={
                "uvaid": {"DataType": "String", "StringValue": uvaid},
                "phrase": {"DataType": "String", "StringValue": phrase},
                "platform": {"DataType": "String", "StringValue": platform},
            },
        )
        print("Submissions Successful: ", response)
    except Exception as e:
        print("Submission Failed: ", e)

    return phrase

#Defining the DAG

with DAG(
    dag_id = "DS3022_DP2_airflow_pipeline",
    default_args = default_args,
    description = "DS3022 Project 2 - Airflow Version",
    start_date = datetime(2025, 10, 29),
    schedule = None,
    catchup = False,
) as dag:
    scatter = PythonOperator(
        task_id = "scatter_messages",
        python_callable = scatter_messages,
    )

    wait = PythonOperator(
        task_id = "wait_for_messages",
        python_callable = wait_for_messages, 
    )

    receive = PythonOperator(
        task_id = "receive_messages",
        python_callable = receive_messages,
    )

    submit = PythonOperator(
        task_id = "assemble_and_submit",
        python_callable = assemble_and_submit,
    )

    scatter >> wait >> receive >> submit
            