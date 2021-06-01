
from flask import Flask, request
from flask_api import status


from uuid import uuid4
#
import threading
import pickle
import json
import boto3
from queue import Queue

init_crawl = []

application = Flask(__name__)

# TODO: remove all calls to the database from this application.
# TODO:     - Switch to queues+workers for pushing crawl metadata
# TODO:     - Have the main service get all status checks.

crawler_dict = {}

box_creds = dict()

# log = logging.getLogger('werkzeug')


# from globus_sdk import ConfidentialAppAuthClient
# import time
# import os
#
#
# def get_uid_from_token(auth_token):
#     # Step 1: Get Auth Client with Secrets.
#     client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
#     secret = os.getenv("GLOBUS_FUNCX_SECRET")
#
#     # Step 2: Transform token and introspect it.
#     t0 = time.time()
#
#     conf_app_client = ConfidentialAppAuthClient(client_id, secret)
#     token = str.replace(str(auth_token), 'Bearer ', '')
#
#     auth_detail = conf_app_client.oauth2_token_introspect(token)
#     t1 = time.time()
#
#     try:
#         print(auth_detail)
#         uid = auth_detail['username']
#     except KeyError as e:
#         raise ValueError("Unable to identify Globus user. Returning (to reject!)")
#
#     print(f"Authenticated user {uid} in {t1-t0} seconds")
#
#     # print(uid)
#     return uid
#
#
# def store_tokens(access_token, refresh_token):
#
#     cwd = os.getcwd()
#     xtract_box_path = os.path.join(cwd, '.xtract_box/tyler/')
#     print(xtract_box_path)
#
#     os.makedirs(f'{xtract_box_path}', exist_ok=True)
#     with open(f'{xtract_box_path}access_token', 'w') as f:
#         f.write(access_token)
#     with open(f'{xtract_box_path}refresh_token', 'w') as g:
#         g.write(refresh_token)
#
# # TODO: bring back for Box.
# # current_oauth = dict()
# # current_oauth['base'] = OAuth2(
# #         client_id=os.environ["box_client_id"],
# #         client_secret=os.environ["box_client_secret"],
# #         store_tokens=store_tokens,
# #     )
#
#
# def crawl_launch(crawler, tc):
#     crawler.crawl(tc)


@application.route('/', methods=['POST', 'GET'])
def hello():
    # st = status.HTTP_200_OK
    return "hello,world"
    # return f"Welcome to the Xtract crawler! \n Status: {str(st)}", st

#
# @application.route('/crawl', methods=['POST'])
# def crawl_repo():
#     # r = request.json
#     r = request.data
#     try:
#         data = pickle.loads(r)
#         # repo_type = data["repo_type"]
#     except pickle.UnpicklingError as e:
#         print(f"Tried and failed to unpickle! Caught: {e}")
#         r = json.loads(r)
#
#         # repo_type = r["repo_type"]
#
#     # crawl_id used for tracking crawls, extractions, search index ingestion.
#     crawl_id = uuid4()
#
#     endpoints = r['endpoints']
#     tokens = r['tokens']  # TODO: no idea why this is arriving as a list.
#
#     print(tokens)
#
#     # TODO: SOMETHING HERE.
#     print(get_uid_from_token(tokens['Authorization']))
#
#     push_to_pg(str(crawl_id), endpoints)
#
#     push_crawl_task(json.dumps({'crawl_id': str(crawl_id),
#                                 'transfer_token': tokens['Transfer'],
#                                 'auth_token': tokens['Authorization']}), str(270))
#
#     init_crawl.append(str(crawl_id))
#     return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK
#
#
# @application.route('/get_crawl_status', methods=['GET'])
# def get_status():
#
#     r = request.json
#     crawl_id = r['crawl_id']
#
#     print(f"Crawl Dict: {crawler_dict}")
#     print(f"Crawl ID: {crawl_id}")
#
#     status_mdata = get_crawl_status(crawl_id)
#     print(f"Status mdata: {status_mdata}")
#
#     if 'error' in status_mdata and crawl_id in init_crawl:
#         status_mdata = {'crawl_id': crawl_id, 'crawl_status': 'initializing'}
#
#     return status_mdata
#
#     # TODO: Do something with all this gdrive weirdness (maybe add more metadata to the regular status object?)
#     # else:
#     #     status_mdata = {}
#     #
#     # if isinstance(crawler, GoogleDriveCrawler):
#     #
#     #     print("This is a Google Drive crawler! ")
#     #     status_mdata["repo_type"] = "GDrive"
#     #
#     #     files_crawled = crawler.count_files_crawled
#     #
#     #     type_tally = crawler.crawl_tallies
#     #     num_is_gdoc = crawler.numdocs
#     #     num_is_user_upload = files_crawled - num_is_gdoc
#     #     status_mdata["gdrive_mdata"] = {'first_ext_tallies': type_tally, 'doc_types': {"is_gdoc": num_is_gdoc,
#     #                                                                                    "is_user_upload":
#     #                                                                                    num_is_user_upload}}
#     #     status_mdata["crawl_start_t"] = crawler.crawl_start
#     #     status_mdata["crawl_status"] = crawler.crawl_status
#     #     status_mdata["n_commit_threads"] = crawler.commit_threads
#     #     status_mdata["groups_crawled"] = files_crawled
#     #
#     #     if crawler.crawl_status == "COMPLETED":
#     #         status_mdata["crawl_end_t"] = crawler.crawl_end
#     #         status_mdata["total_crawl_time"] = crawler.crawl_end - crawler.crawl_start
#     #
#     #     return status_mdata
#     #
#     # else:
#     #     return {'crawl_id': str(crawl_id), 'Invalid Submission': True}
#
#
# ret_vals_dict = {"foobar": Queue()}
#
#
# def fetch_crawl_messages(crawl_id):
#
#     print("IN thread! ")
#
#     client = boto3.client('sqs',
#                           aws_access_key_id=os.environ["aws_access"],
#                           aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
#
#     response = client.get_queue_url(
#         QueueName=f'crawl_{crawl_id}',
#         QueueOwnerAWSAccountId='576668000072')  # TODO: env variable
#
#     crawl_queue = response["QueueUrl"]
#
#     empty_count = 0
#
#     while True:
#
#         if empty_count == 10:
#             print("Empty! Returning! ")
#             return   # kill the thread.
#
#         sqs_response = client.receive_message(
#             QueueUrl=crawl_queue,
#             MaxNumberOfMessages=10,  # TODO: Change back to 10.
#             WaitTimeSeconds=1)
#
#         file_list = []
#         del_list = []
#
#         if "Messages" in sqs_response:
#             num_messages = len(sqs_response["Messages"])
#         else:
#             empty_count += 1
#             time.sleep(0.1)
#             continue
#
#         for message in sqs_response["Messages"]:
#             message_body = message["Body"]
#             print(message_body)
#
#             del_list.append({'ReceiptHandle': message["ReceiptHandle"],
#                              'Id': message["MessageId"]})
#
#             mdata = json.loads(message_body)
#
#             files = mdata['files']
#
#             for file_name in files:
#                 ret_vals_dict[crawl_id].put(file_name)
#
#             if len(del_list) > 0:
#                 client.delete_message_batch(
#                     QueueUrl=crawl_queue,
#                     Entries=del_list)
#
#
# @application.route('/fetch_crawl_mdata', methods=["GET", "POST"])
# def fetch_mdata():
#     """ Fetch endpoint -- only for Will & Co's GDrive case to fetch their metadata.
#     :returns {crawl_id: str, metadata: dict} (dict)"""
#
#     r = request.json
#     crawl_id = r['crawl_id']
#     n = r['n']  # TODO: We need to internall set a maximum 'n' value. Probably 100 or 1000.
#
#     queue_empty = False
#
#     if crawl_id not in ret_vals_dict:
#         ret_vals_dict[crawl_id] = Queue()
#         thr = threading.Thread(target=fetch_crawl_messages, args=(crawl_id,))
#         thr.start()
#
#     plucked_files = 0
#     file_list = []
#     while plucked_files < n:
#         if ret_vals_dict[crawl_id].empty():
#             queue_empty = True
#             break
#         file_path = ret_vals_dict[crawl_id].get()
#         print(file_path)
#         plucked_files += 1
#         file_list.append(file_path)
#
#     return {"crawl_id": str(crawl_id), "num_files": plucked_files, "file_ls": file_list, "queue_empty": queue_empty}


if __name__ == '__main__':
    application.run(debug=True, threaded=True, ssl_context="adhoc")
