# prefect flow goes here
import requests
import pandas as pd
import boto3
import time
from prefect import task, flow, get_run_logger
from datetime import datetime
import json


@task(log_prints=True)
def scatter_messages():
    sqs_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/dpv8cf"
    url = sqs_url
    response = requests.post(url)
    response.raise_for_status()
    payload = response.json()
    return payload["sqs_url"]

@task(log_prints=True)
def wait_for_messages(sqs_url):
    sqs = boto3.client("sqs")
    logger = get_run_logger()

    while True:
        response = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=[
                "ApproximateNumberOfMessages", #Visible
                "ApproximateNumberOfMessagesNotVisible", #Being Processed by AWS temporarily
                "ApproximateNumberOfMessagesDelayed" #Delayed
            ]
        )
        attributes = response["Attributes"]

        visible = int(attributes["ApproximateNumberOfMessages"])
        not_visible = int(attributes["ApproximateNumberOfMessagesNotVisible"])
        delayed = int(attributes["ApproximateNumberOfMessagesDelayed"])
        total = visible + not_visible + delayed

        logger.info(f"Queue Status -> visible = {visible}, not_visible = {not_visible}, delayed = {delayed}, total = {total}")

        if total >= 21 and visible > 3:
            logger.info(f"All 21 Messages are in the queue!")
            break

        time.sleep(10)

@task(log_prints = True)
def receive_messages(sqs_url):
    sqs = boto3.client("sqs")
    logger = get_run_logger()
    all_messages = []

    while True:
        response = sqs.receive_message(
            QueueUrl = sqs_url,
            MaxNumberOfMessages = 10,
            VisibilityTimeout = 30,
            MessageAttributeNames = ["All"],
            WaitTimeSeconds = 10
        )

        #Error Handling
        if "Messages" not in response:
            logger.info("No more Messages are available, exiting the loop.")
            break

        for messages in response["Messages"]:
            attributes = messages["MessageAttributes"]
            order_no = int(attributes["order_no"]["StringValue"])
            word = attributes["word"]["StringValue"]
            receipt = messages["ReceiptHandle"] #Receipt Handle to delete the message later

            all_messages.append({"order_no": order_no, "word": word, "receipt": receipt})

            #Deletion after storing so that it doesn't reread
            sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle = receipt)

        logger.info(f"Fetched {len(all_messages)} messages so far")

    return all_messages


@flow
def prefect_pipeline():
    uvaid = "dpv8cf"
    platform = "prefect"

    sqs_url = scatter_messages()
    wait_for_messages(sqs_url)
    messages = receive_messages(sqs_url)
    ordered = sorted(messages, key = lambda msg: msg["order_no"])
    phrase = " ".join([msg["word"] for msg in ordered])

    sqs = boto3.client('sqs')
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

    try:
        response =  sqs.send_message(
            QueueUrl = submit_url,
            MessageBody = "submission",
            MessageAttributes = {
            "uvaid": {"DataType": "String", "StringValue": uvaid},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}
            }
        )
        print("Submitted Successfully:", response)
    
    except Exception as e:
        print("Submission Failed", e)



if __name__ == "__main__":
    prefect_pipeline()