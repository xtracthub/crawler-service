
from flask import Flask, request

from crawlers.utils.crawler_utils import push_to_pg, get_crawl_status, push_crawl_obj
from utils.pg_utils import pg_conn, pg_update
from utils.sqs_utils import push_crawl_task


from uuid import uuid4
import threading
import json

import boto3
from queue import Queue
from globus_sdk import ConfidentialAppAuthClient
import time
import os


init_crawl = []
application = Flask(__name__)
crawler_heartbeat_dict = {}


def hb_check_thread():
    while True:
        # Sleep for 30 seconds
        print("Waiting in hb thread for 30s...")
        time.sleep(30)

        # Query the crawl_DB
        for crawl_id in crawler_heartbeat_dict:
            cur_time = time.time()
            hb_time = crawler_heartbeat_dict[crawl_id]

            # If more than 30 seconds has passed since last heartbeat
            if cur_time - hb_time > 30:
                # Then query the DB to see if in 'INITIALIZING'
                status_json = get_crawl_status(crawl_id)

                status = status_json['crawl_status']

                # TODO: add case when mid-crawl failure.
                if status == 'initializing':
                    conn = pg_conn()
                    cur = conn.cursor()
                    query = f"""UPDATE TABLE crawls SET status='failure_retrying' where crawl_id='{crawl_id}';"""
                    pg_update(cur, query)
                    # TODO: need to assemble an object with Tokens
                    # TODO: get from crawl_q_db!!!!!
                    push_crawl_task(crawl_id, unique_id=str(270))
                    time.sleep(0.25)  # Just to avoid creating too many DB connections in short period.
                elif status == 'complete':
                    # If we are done processing, then we should
                    crawler_heartbeat_dict[crawl_id]['action'] = 'stop'


def get_uid_from_token(auth_token):
    # Step 1: Get Auth Client with Secrets.
    client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
    secret = os.getenv("GLOBUS_FUNCX_SECRET")

    # Step 2: Transform token and introspect it.
    t0 = time.time()

    conf_app_client = ConfidentialAppAuthClient(client_id, secret)
    token = str.replace(str(auth_token), 'Bearer ', '')

    auth_detail = conf_app_client.oauth2_token_introspect(token)
    t1 = time.time()

    try:
        uid = auth_detail['username']
    except KeyError as e:
        raise ValueError(str(e))
    print(f"Authenticated user {uid} in {t1-t0} seconds")
    return uid


def store_tokens(access_token, refresh_token):

    cwd = os.getcwd()
    xtract_box_path = os.path.join(cwd, '.xtract_box/tyler/')
    print(xtract_box_path)

    os.makedirs(f'{xtract_box_path}', exist_ok=True)
    with open(f'{xtract_box_path}access_token', 'w') as f:
        f.write(access_token)
    with open(f'{xtract_box_path}refresh_token', 'w') as g:
        g.write(refresh_token)


@application.route('/', methods=['POST', 'GET'])
def hello():
    return f"Welcome to the Xtract crawler! \n Status: 200 (OK)"


@application.route('/crawl', methods=['POST'])
def crawl_repo():

    r = json.loads(request.data)

    # crawl_id used for tracking crawls, extractions, search index ingestion.
    crawl_id = uuid4()

    endpoints = r['endpoints']
    skip_list = None
    if 'skip_list' in r:
        skip_list = r['skip_list']

    # Record the entire skip list
    from utils.pg_utils import pg_conn

    # TODO: boot a bunch of this into pg_utils
    conn = pg_conn()
    if skip_list is not None:
        for skip_item in skip_list:
            cur = conn.cursor()
            query = f"""INSERT INTO skip_lookup (crawl_id, skip_pattern) VALUES ('{crawl_id}', '{skip_item}');"""
            cur.execute(query)
        conn.commit()

    tokens = r['tokens']  # TODO: no idea why this is arriving as a list.

    print(tokens)

    try:
        user_name = get_uid_from_token(tokens['FuncX'])
    except ValueError as e1:
        # If we fail here, do not even start a crawl
        print(f"Raising e1 error case: {e1}")
        return {'crawl_id': None, 'message': str(e1), 'error': True}
    except Exception as e2:
        # If we fail here, do not even start a crawl
        print(f"Raising e2 error case: {e2}")
        return {'crawl_id': None, 'message': f"Unknown error case: {str(e2)}", 'error': True}

    # TODO: need to do something with username
    print(f"Authenticated username via funcX-Globus credentials: {user_name}")

    try:
        push_to_pg(str(crawl_id), endpoints)

        crawl_q_obj = json.dumps({'crawl_id': str(crawl_id),
                                  'transfer_token': tokens['Transfer'],
                                  'auth_token': tokens['Authorization'],
                                  'funcx_token': tokens['FuncX']})

        push_crawl_task(crawl_q_obj, str(270))  # TODO: what is 270?
        push_crawl_obj(crawl_id=crawl_id, crawl_obj=crawl_q_obj)
    except Exception as e3:
        return {'crawl_id': None, 'message': f"Internal postgres exception: {str(e3)}", 'error': True}

    init_crawl.append(str(crawl_id))
    return {"crawl_id": str(crawl_id), 'status': f"200 (OK)"}


@application.route('/get_crawl_status', methods=['GET'])
def get_status():

    r = request.json
    crawl_id = r['crawl_id']

    print(f"Crawl ID: {crawl_id}")

    status_mdata = get_crawl_status(crawl_id)
    print(f"Status mdata: {status_mdata}")

    # TODO: lol look more closely at this.
    if 'error' in status_mdata and crawl_id in init_crawl:
        status_mdata = {'crawl_id': crawl_id, 'crawl_status': 'initializing'}

    return status_mdata


ret_vals_dict = {"foobar": Queue()}


def fetch_crawl_messages(crawl_id):

    client = boto3.client('sqs',
                          aws_access_key_id=os.environ["aws_access"],
                          aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')

    response = client.get_queue_url(
        QueueName=f'crawl_{crawl_id}',
        QueueOwnerAWSAccountId='576668000072')  # TODO: env variable

    crawl_queue = response["QueueUrl"]

    empty_count = 0

    while True:

        if empty_count == 10:
            print("Empty! Returning! ")
            return   # kill the thread.

        sqs_response = client.receive_message(
            QueueUrl=crawl_queue,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1)

        file_list = []
        del_list = []

        if "Messages" in sqs_response:
            num_messages = len(sqs_response["Messages"])
        else:
            empty_count += 1
            time.sleep(0.1)
            continue

        for message in sqs_response["Messages"]:
            message_body = message["Body"]

            del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                             'Id': message["MessageId"]})

            mdata = json.loads(message_body)

            files = mdata['files']
            crawl_timestamp = mdata['metadata']['crawl_timestamp']

            filename_size_map = dict()
            for group in mdata['groups']:
                for file_obj in group['files']:
                    path = file_obj['path']
                    file_size = file_obj['metadata']['physical']['size']
                    filename_size_map[path] = file_size

            for file in files:
                file['crawl_timestamp'] = crawl_timestamp
                file['size'] = filename_size_map[file['path']]

            for file in files:
                ret_vals_dict[crawl_id].put(file)

            if len(del_list) > 0:
                client.delete_message_batch(
                    QueueUrl=crawl_queue,
                    Entries=del_list)


@application.route('/fetch_crawl_mdata', methods=["GET", "POST"])
def fetch_mdata():
    """ Fetch endpoint -- only for Will & Co's GDrive case to fetch their metadata.
    :returns {crawl_id: str, metadata: dict} (dict)"""

    r = request.json
    crawl_id = r['crawl_id']
    n = r['n']  # TODO: We need to internally set a maximum 'n' value. Probably 100 or 1000.

    queue_empty = False

    if crawl_id not in ret_vals_dict:
        ret_vals_dict[crawl_id] = Queue()
        thr = threading.Thread(target=fetch_crawl_messages, args=(crawl_id,))
        thr.start()

    plucked_files = 0
    file_list = []
    while plucked_files < n:
        if ret_vals_dict[crawl_id].empty():
            queue_empty = True
            break
        file_path = ret_vals_dict[crawl_id].get()
        plucked_files += 1
        file_list.append(file_path)

    return {"crawl_id": str(crawl_id), "num_files": plucked_files, "file_ls": file_list, "queue_empty": queue_empty}


@application.route('/heartbeat', methods=["GET"])
def heartbeat():
    """ Used to keep heartbeats for the crawl-workers.
    :returns {status: 'ok'} (dict)"""

    r = request.json
    crawl_id = r['crawl_id']
    hb_time = time.time()

    if crawl_id not in crawler_heartbeat_dict:
        crawler_heartbeat_dict[crawl_id] = {'action': None, 'time': None}
    crawler_heartbeat_dict[crawl_id]['time'] = hb_time

    # If we need to stop, that means we decided to just resubmit the task.
    if crawler_heartbeat_dict[crawl_id]['action'] == 'stop':
        return {"crawl_id": str(crawl_id), "status": "STOP"}

    return {"crawl_id": str(crawl_id), "status": "OK"}


if __name__ == '__main__':
    hb_thr = threading.Thread(target=hb_check_thread, args=())
    hb_thr.daemon = True
    hb_thr.start()
    application.run(debug=True, threaded=True, ssl_context="adhoc")
