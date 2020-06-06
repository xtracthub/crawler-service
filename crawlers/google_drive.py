
from .base import Crawler
from crawlers.utils.gdrive_utils import get_next_page, generate_drive_connection


class GoogleDriveCrawler(Crawler):

    def __init__(self, crawl_id, creds, grouper_name="file"):
        Crawler.__init__(self)
        self.crawl_id = crawl_id
        self.creds = creds
        self.grouper_name = grouper_name

        self.count_groups_crawled = 0
        self.count_files_crawled = 0
        self.count_bytes_crawled = 0

        print("Generating connection to Google Drive API...")
        self.drive_conn = generate_drive_connection(self.creds)
        print("Connection to Drive API successful!")

        self.crawl_status = "STARTING"

    def crawl(self, tc=None):  # TODO: Find clean way to remove transfer_client requirement from .crawl() function.
        # TODO: Properly generate the queues for these.
        all_files = []
        next_page_token = None

        while True:

            if not next_page_token:
                # Call the Drive v3 API
                results = get_next_page(self.drive_conn, None)

            else:
                results = get_next_page(self.drive_conn, next_page_token)

            items = results.get('files', [])
            next_page_token = results.get("nextPageToken", [])
            all_files.extend(items)

            self.count_files_crawled += len(items)

            if len(items) < 1000 or not items:
                print('Time to break... or no files found')
                print(f"Total files processed: {len(all_files)}")
                # TODO: this should return, but should probably enqueue (same as Globus HTTPS crawler)
                # return {"file_mdata": all_files, "crawl_id": self.crawl_id}
                self.crawl_status = "SUCCEEDED"
