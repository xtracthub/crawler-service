
import boto3
import os


def king_kong_chaos_sqs():
    client = boto3.client('sqs', aws_access_key_id=os.environ["aws_access"],
                          aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')

    response = client.list_queues()

    urls = response['QueueUrls']

    for url in urls:
        client.delete_queue(QueueUrl=url)

    return "PATH OF DESTRUCTION"


king_kong_chaos_sqs()
