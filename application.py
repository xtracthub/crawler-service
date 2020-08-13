
from flask import Flask, request, redirect
from flask_api import status
# from boxsdk import Client

# Import each of our crawlers.
from crawlers.globus_base import GlobusCrawler
from crawlers.google_drive import GoogleDriveCrawler

from uuid import uuid4

import threading
import logging
import pickle
import json
import time
import boto3
from boxsdk import OAuth2
from queue import Queue
import os

application = Flask(__name__)

# TODO: remove all calls to the database from this application.
# TODO:     - Switch to queues+workers for pushing crawl metadata
# TODO:     - Have the main service get all status checks.


crawler_dict = {}

box_creds = dict()

log = logging.getLogger('werkzeug')


def store_tokens(access_token, refresh_token):

    cwd = os.getcwd()
    xtract_box_path = os.path.join(cwd, '.xtract_box/tyler/')
    print(xtract_box_path)

    os.makedirs(f'{xtract_box_path}', exist_ok=True)
    with open(f'{xtract_box_path}access_token', 'w') as f:
        f.write(access_token)
    with open(f'{xtract_box_path}refresh_token', 'w') as g:
        g.write(refresh_token)

# TODO: bring back.
# current_oauth = dict()
# current_oauth['base'] = OAuth2(
#         client_id=os.environ["box_client_id"],
#         client_secret=os.environ["box_client_secret"],
#         store_tokens=store_tokens,
#     )


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


@application.route('/')
def hello():
    st = status.HTTP_200_OK
    return f"Welcome to the Xtract crawler! \n Status: {str(st)}", st


@application.route('/crawl', methods=['POST'])
def crawl_repo():
    # r = request.json
    r = request.data
    # TODO: bring back unpickling for Google.
    try:
        data = pickle.loads(r)
        repo_type = data["repo_type"]
    except pickle.UnpicklingError as e:
        print(f"Tried and failed to unpickle! Caught: {e}")
        r = json.loads(r)

        repo_type = r["repo_type"]

    # crawl_id used for tracking crawls, extractions, search index ingestion.
    crawl_id = uuid4()

    if repo_type == "GLOBUS":
        endpoint_id = r['eid']
        starting_dir = r['dir_path']
        grouper = r['grouper']
        transfer_token = r['Transfer']
        auth_token = r['Authorization']

        print(f"Received Transfer Token: {transfer_token}")

        base_url = ''
        if 'https_info' in r:
            base_url = r['https_info']['base_url']

        crawler = GlobusCrawler(endpoint_id,
                                starting_dir,
                                crawl_id,
                                transfer_token,
                                auth_token,
                                grouper,
                                base_url=base_url)
        tc = crawler.get_transfer()
        crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
        crawl_thread.start()

    elif repo_type == "GDRIVE":
        # If using Google Drive, we must receive credentials file containing user's Auth info.
        creds = data["auth_creds"]
        crawler = GoogleDriveCrawler(crawl_id, creds[0])
        crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, ''))
        crawl_thread.start()

    else:
        return {"crawl_id": str(crawl_id),
                "message": "Error: Repo must be of type 'GLOBUS' or 'GDRIVE'. "}, status.HTTP_400_BAD_REQUEST

    crawler_dict[str(crawl_id)] = crawler

    return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK


@application.route('/auth_box', methods=['GET', 'POST'])
def auth_box():

    auth_url, csrf_token = current_oauth['base'].get_authorization_url('http://127.0.0.1:5000/get_token')

    # with open()

    # First we check to see if access and refresh tokens
    # TODO: enable auth from refresh token.
    # oauth = OAuth2(
    #     client_id=os.environ["box_client_access"],
    #     client_secret=os.environ["box_client_secret"],
    #     access_token='ACCESS_TOKEN',
    #     refresh_token='REFRESH_TOKEN',
    # )

    return redirect(auth_url, code=302)


@application.route('/get_token', methods=['GET', 'POST'])
def get_token():
    auth_code = request.args.get('code')
    oauth.authenticate(auth_code)

    client = Client(oauth)

    user = client.user().get()

    box_creds[user.id] = client

    print(f"Box creds: {box_creds}")

    print("The current user is {0}".format(user.id))

    return "The current user is {0}. You may shut this page! ".format(user.id)


@application.route('/crawl_box', methods=['POST', 'GET'])
def crawl_box():
    #r = request.json
    #user_id = int(r["user_id"])

    user_id = '1425958733'

    print(f"Box Creds 2: {box_creds}")

    if user_id in box_creds:  # TODO: how do we know if auth is stale?
        client = box_creds[user_id]

    else:
        return {"error": "Error fetching saved auth token. Please Auth again"}

    root_folder = client.folder(folder_id='112657269903').get()

    print(root_folder)
    return root_folder.id


@application.route('/get_crawl_status', methods=['GET'])
def get_status():

    r = request.json
    crawl_id = r['crawl_id']

    print(f"Crawl Dict: {crawler_dict}")
    print(f"Crawl ID: {crawl_id}")
    try:
        crawler = crawler_dict[crawl_id]
    except:  # TODO: Clean this up.
        return {'crawl_id': str(crawl_id), 'Invalid Submission': True}

    if crawl_id in crawler_dict and not isinstance(crawler, GoogleDriveCrawler):

        print("This exists! ")

        files_crawled = crawler_dict[crawl_id].count_files_crawled
        bytes_crawled = crawler_dict[crawl_id].count_bytes_crawled
        groups_crawled = crawler_dict[crawl_id].count_groups_crawled

        status_mdata = {'crawl_id': str(crawl_id), 'files_crawled': files_crawled,
                'bytes_crawled': bytes_crawled, 'groups_crawled': groups_crawled, 'crawl_status': crawler.crawl_status}
        return status_mdata

    else:
        print("WHAT THE HELL?")
        # files_crawled = 0
        status_mdata = {}
        #

    if isinstance(crawler, GoogleDriveCrawler):

        print("This is a Google Drive crawler! ")
        status_mdata["repo_type"] = "GDrive"

        files_crawled = crawler.count_files_crawled

        type_tally = crawler.crawl_tallies
        num_is_gdoc = crawler.numdocs
        num_is_user_upload = files_crawled - num_is_gdoc
        status_mdata["gdrive_mdata"] = {'first_ext_tallies': type_tally, 'doc_types': {"is_gdoc": num_is_gdoc,
                                                                                       "is_user_upload": num_is_user_upload}}
        status_mdata["crawl_start_t"] = crawler.crawl_start
        status_mdata["crawl_status"] = crawler.crawl_status
        status_mdata["n_commit_threads"] = crawler.commit_threads
        status_mdata["groups_crawled"] = files_crawled

        if crawler.crawl_status == "COMPLETED":
            status_mdata["crawl_end_t"] = crawler.crawl_end
            status_mdata["total_crawl_time"] = crawler.crawl_end - crawler.crawl_start

        return status_mdata

    else:
        return {'crawl_id': str(crawl_id), 'Invalid Submission': True}


ret_vals_dict = {"foobar": Queue()}
def fetch_crawl_messages(crawl_id):

    print("IN thread! ")

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
            MaxNumberOfMessages=10,  # TODO: Change back to 10.
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
            print(message_body)

            del_list.append({'ReceiptHandle': message["ReceiptHandle"],
                             'Id': message["MessageId"]})

            mdata = json.loads(message_body)

            files = mdata['files']

            for file_name in files:
                ret_vals_dict[crawl_id].put(file_name)

            if len(del_list) > 0:
                response = client.delete_message_batch(
                    QueueUrl=crawl_queue,
                    Entries=del_list)


@application.route('/fetch_crawl_mdata', methods=["GET", "POST"])
def fetch_mdata():
    """ Fetch endpoint -- only for Will & Co's GDrive case to fetch their metadata.
    :returns {crawl_id: str, metadata: dict} (dict)"""

    r = request.json
    crawl_id = r['crawl_id']
    n = r['n']

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
        print(file_path)
        plucked_files += 1
        file_list.append(file_path)

    return {"crawl_id": str(crawl_id), "num_files": plucked_files, "file_ls": file_list, "queue_empty": queue_empty}


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
