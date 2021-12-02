
import os
import json

import boto3


def _get_dev_status():
    raw_is_dev = os.environ["XTRACT_IS_DEV"]
    print(f"RAW IS DEV: {raw_is_dev}")
    if raw_is_dev == "TRUE":
        is_dev = True
    elif raw_is_dev == "FALSE":
        is_dev = False
    else:
        raise ValueError("COULD NOT FIND XTRACT_IS_DEV ENVIRONMENT VARIABLE ON DISK")
    return is_dev


def get_sqs_conn():
    client = boto3.client('sqs',
                          aws_access_key_id=os.environ["aws_access"],
                          aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
    return client


def get_crawl_work_queue(client):

    is_dev = _get_dev_status()

    if not is_dev:
        response = client.get_queue_url(
            QueueName='crawl_work_queue',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )
    else:
        response = client.get_queue_url(
            QueueName='crawl_work_queue_DEBUG',
            QueueOwnerAWSAccountId=os.environ["aws_account"]
        )

    crawl_work_queue = response["QueueUrl"]

    return crawl_work_queue


def get_next_task(max_timeout=120):

    client = get_sqs_conn()
    crawl_work_queue = get_crawl_work_queue(client)

    sqs_response = client.receive_message(  # TODO: properly try/except this block.
        QueueUrl=crawl_work_queue,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=max_timeout)

    print("Successfully received task from SQS!")

    message = sqs_response["Messages"][0]["Body"]

    return json.loads(message)


def push_crawl_task(task, unique_id):

    # TODO: when we turn this into a class 'id' should just be a 'count up' variable

    entry = {
        'Id': unique_id,
        'MessageBody': task,

    }

    client = get_sqs_conn()
    client.send_message_batch(QueueUrl=get_crawl_work_queue(client),
                              Entries=[entry])

    print("Successfully sent task to SQS!")
