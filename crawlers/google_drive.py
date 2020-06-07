
import datetime
from .base import Crawler
from crawlers.utils.gdrive_utils import get_next_page, generate_drive_connection
from crawlers.groupers import simple_ext_grouper


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
        starting = True

        grouper = simple_ext_grouper.SimpleExtensionGrouper()

        while True:

            if not next_page_token and starting:
                # Call the Drive v3 API
                results = get_next_page(self.drive_conn, None)
                starting = False

            else:
                results = get_next_page(self.drive_conn, next_page_token)

            items = results.get('files', [])
            next_page_token = results.get("nextPageToken", [])

            clean_mdata = self.gdrive_mdata_cleaner(items)
            all_files.extend(clean_mdata)

            self.count_files_crawled += len(items)

            if not next_page_token and not starting:
                print('Time to break... or no files found')
                print(f"Total files processed: {len(all_files)}")
                # TODO: this should return, but should probably enqueue (same as Globus HTTPS crawler)
                # return {"file_mdata": all_files, "crawl_id": self.crawl_id}
                self.crawl_status = "SUCCEEDED"
                break
        print("Running grouper...")
        grouped_mdata = grouper.gen_groups(all_files)

        print("SUCCESSFULLY GROUPED METADATA!")

        text_tally = 0
        im_tally = 0
        tab_tally = 0
        none_tally = 0
        for item in grouped_mdata:
            t = item["extractor"]
            if t == "text":
                text_tally += 1
            elif t == "tabular":
                tab_tally += 1
            elif t == "images":
                im_tally += 1
            elif t == None:
                none_tally += 1
            else:
                print(t)
                raise ValueError
        print(f"Text: {text_tally}\nTabular: {tab_tally}\nImages: {im_tally}\nNone: {none_tally}")

    def gdrive_mdata_cleaner(self, results_ls):

        new_ls = []
        for res in results_ls:
            if 'size' not in res:
                res['size'] = 0
                res['is_gdoc'] = True

            shared_peeps = []
            res['user_is_owner'] = False
            if "permissions" in res:
                for person in res['permissions']:
                    # if user and the owner
                    if person['id'] == res['id'] and person['role'] == 'owner':
                        res['user_is_owner'] = True
                    shared_peeps.append(person['id'])
                del res["permissions"]

            res["shared_user_ids"] = shared_peeps

            # TODO: Should this really be a list of all parents?
            if 'parents' in res:
                res["parent"] = res["parents"][0]
                del res["parents"]  # No longer useful to us.
            else:
                res["parent"] = None

            last_mod = res["modifiedTime"]
            last_mod = datetime.datetime.strptime(last_mod, '%Y-%m-%dT%H:%M:%S.%fZ')
            last_mod = last_mod.timestamp()

            res["last_modified"] = last_mod
            del res["modifiedTime"]

            if 'fullFileExtension' in res:
                res['extension'] = res['fullFileExtension']
                del res['fullFileExtension']
            else:
                res['extension'] = ""

            new_ls.append(res)
        return new_ls
