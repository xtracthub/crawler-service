
from flask import Flask, request
from flask_api import status

from crawlers.globus_base import GlobusCrawler
from uuid import uuid4

import threading

application = Flask(__name__)

# TODO: remove all calls to the database from this application.
# TODO:     - Switch to queues+workers for pushing crawl metadata
# TODO:     - Have the main service get all status checks.


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

    return {"crawl_id": str(crawl_id)}, status.HTTP_200_OK


if __name__ == '__main__':
    application.run(debug=True, threaded=True)
