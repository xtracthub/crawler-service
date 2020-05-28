
from flask import Flask, request
from flask_api import status

from crawlers.globus_base import GlobusCrawler
from crawlers.google_drive import g_crawl
from googleapiclient.discovery import build

from uuid import uuid4

import threading
import logging
import pickle

application = Flask(__name__)

# TODO: remove all calls to the database from this application.
# TODO:     - Switch to queues+workers for pushing crawl metadata
# TODO:     - Have the main service get all status checks.


crawler_dict = {}

log = logging.getLogger('werkzeug')
# log.disabled = True
# application.logger.disabled = True


def crawl_launch(crawler, tc):
    crawler.crawl(tc)


@application.route('/')
def hello():
    st = status.HTTP_200_OK
    return f"Welcome to the Xtract crawler! \n Status: {str(st)}", st


@application.route('/crawl', methods=['POST'])
def crawl_repo():

    r = request.json

    endpoint_id = r['eid']
    starting_dir = r['dir_path']
    grouper = r['grouper']
    transfer_token = r['Transfer']
    auth_token = r['Authorization']

    print(f"Received Transfer Token: {transfer_token}")

    crawl_id = uuid4()
    crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, transfer_token, auth_token, grouper)
    tc = crawler.get_transfer()
    crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
    crawl_thread.start()

    crawler_dict[str(crawl_id)] = crawler

    return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK


@application.route('/crawl_gdrive', methods=["POST"])
def crawl_gdrive():
    r = request.data

    creds = pickle.loads(r)[0]

    print(creds)

    service = generate_drive_connection(creds)
    file_mdata = crawl(service)

    return file_mdata


@application.route('/get_crawl_status', methods=['GET'])
def get_status():

    r = request.json
    crawl_id = r['crawl_id']

    print(f"Crawl Dict: {crawler_dict}")
    print(f"Crawl ID: {crawl_id}")

    if crawl_id in crawler_dict:

        files_crawled = crawler_dict[crawl_id].count_files_crawled
        bytes_crawled = crawler_dict[crawl_id].count_bytes_crawled
        groups_crawled = crawler_dict[crawl_id].count_groups_crawled

        return {'crawl_id': str(crawl_id), 'files_crawled': files_crawled,
                'bytes_crawled': bytes_crawled, 'group_crawled': groups_crawled}

    else:
        return {'crawl_id': str(crawl_id), 'Invalid Submission': True}


def get_next_page(service, nextPageToken):
    results = service.files().list(
        pageSize=1000, pageToken=nextPageToken,
        fields="nextPageToken, files(id, name, size, mimeType, fullFileExtension)").execute()

    return results


def crawl(service):
    all_files = []
    nextPageToken = None
    print("CRAWLING")

    while True:

        if not nextPageToken:

            # Call the Drive v3 API
            results = get_next_page(service, None)

        else:
            results = get_next_page(service, nextPageToken)

        items = results.get('files', [])
        nextPageToken = results.get("nextPageToken", [])
        all_files.extend(items)

        # TODO: Do I need to break before this point?
        if len(items) < 1000 or not items:
            print('Time to break... or no files found')
            print(f"Total files processed: {len(all_files)}")
            return {"file_mdata": all_files}


def generate_drive_connection(creds):
    service = build('drive', 'v3', credentials=creds)
    return service


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
