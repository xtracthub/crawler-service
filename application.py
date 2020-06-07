
from flask import Flask, request
from flask_api import status

# Import each of our crawlers.
from crawlers.globus_base import GlobusCrawler
from crawlers.google_drive import GoogleDriveCrawler

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

    r = request.data
    data = pickle.loads(r)

    repo_type = data["repo_type"]

    # crawl_id used for tracking crawls, extractions, search index ingestion.
    crawl_id = uuid4()

    # TODO: Add this to the notebooks.
    print(repo_type)

    if repo_type=="GLOBUS":
        endpoint_id = r['eid']
        starting_dir = r['dir_path']
        grouper = r['grouper']
        transfer_token = r['Transfer']
        auth_token = r['Authorization']

        print(f"Received Transfer Token: {transfer_token}")

        crawler = GlobusCrawler(endpoint_id, starting_dir, crawl_id, transfer_token, auth_token, grouper)
        tc = crawler.get_transfer()
        crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, tc))
        crawl_thread.start()

    elif repo_type=="GDRIVE":
        # If using Google Drive, we must receive credentials file containing user's Auth info.
        # gdrive_data = request.data
        creds = data["auth_creds"]
        crawler = GoogleDriveCrawler(crawl_id, creds[0])
        crawl_thread = threading.Thread(target=crawl_launch, args=(crawler, None))
        crawl_thread.start()

    else:
        return {"crawl_id": str(crawl_id),
                "message": "Error: Repo must be of type 'GLOBUS' or 'GDRIVE'. "}, status.HTTP_400_BAD_REQUEST

    crawler_dict[str(crawl_id)] = crawler

    return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK


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


@application.route('/fetch_crawl_mdata', methods=["POST"])
def fetch_mdata():
    """ Fetch endpoint -- only for Will & Co's GDrive case to fetch their metadata.

    :returns {crawl_id: str, metadata: dict} (dict)"""

    r = request.json
    crawl_id = r['crawl_id']

    mdata = None

    # TODO: Get metadata right here.

    # TODO: Step 1. Save it.
    # TODO: Step 2. Get it.
    # TODO: Step 3. Remove it.

    return {"crawl_id": str(crawl_id), "metadata": mdata}


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
