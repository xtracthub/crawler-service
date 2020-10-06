
import time
from base import Crawler
import os
import json
import boto3
import threading
from queue import Queue
import sqlite3
from xtract_sdk.packagers import FamilyBatch, Family, Group
from groupers import matio_grouper, simple_ext_grouper


# A couple of global variables used for sanity checks across recursive functions. Change in meaning for debugging.
CLOUD_REPO = False
COUNT = 0

"""
    This ftp/local-directory_scraper will create a path-directory of all files and filesizes in given FTP server.
    Code can be uncommented to (1) do in a single recursive swath, and (2) actually download files
    a local machine. It implements Depth-First Search (DFS).

        @Author: Tyler J. Skluzacek, University of Chicago
        @Email: skluzacek@uchicago.edu
        @Github: tskluzac
        @LastEdited: 09/22/2020
"""


class LocalCrawler(Crawler):

    def __init__(self, crawl_id):
        Crawler.__init__(self)
        self.families_to_enqueue = Queue()
        self.crawl_status = "STARTING"
        self.grouper = "gdrive"
        self.active_commits = 0
        self.crawl_id = crawl_id
        self.success_group_commit_count = 0

        self.csv_handle = open('local_crawler.mdata', 'w')

        self.client = boto3.client('sqs',
                                   aws_access_key_id=os.environ["aws_access"],
                                   aws_secret_access_key=os.environ["aws_secret"], region_name='us-east-1')
        print(f"Creating queue for crawl_id: {self.crawl_id}")
        queue = self.client.create_queue(QueueName=f"crawl_{str(self.crawl_id)}")

        if queue["ResponseMetadata"]["HTTPStatusCode"] == 200:
            self.queue_url = queue["QueueUrl"]
        else:
            raise ConnectionError("Received non-200 status from SQS!")
        print(queue)
        self.commit_threads = 10

        self.sqs_push_threads = {}
        self.thr_ls = []
        for i in range(0, self.commit_threads):
            thr = threading.Thread(target=self.enqueue_loop, args=(i,))
            self.thr_ls.append(thr)
            thr.start()
            self.sqs_push_threads[i] = True
        print(f"Successfully started {len(self.sqs_push_threads)} SQS push threads!")

    def enqueue_loop(self, thr_id):
        while True:
            insertables = []

            # If empty, then we want to return.
            if self.families_to_enqueue.empty():

                # If ingest queue empty, we can demote to "idle"
                if self.crawl_status == "COMMITTING":
                    self.sqs_push_threads[thr_id] = "IDLE"
                    time.sleep(0.25)

                    # NOW if all threads idle, then return!
                    if all(value == "IDLE" for value in self.sqs_push_threads.values()):
                        self.commit_completed = True
                        return 0
                time.sleep(1)
                continue

            self.sqs_push_threads[thr_id] = "ACTIVE"

            # Remove up to n elements from queue, where n is current_batch.
            current_batch = 1
            while not self.families_to_enqueue.empty() and current_batch < 2:  # 10:  # TODO: back to 10, but with proper cleanup.

                family_to_commit = self.families_to_enqueue.get()

                # print(f"Family to commit: {family_to_commit}")

                insertables.append(family_to_commit)

                self.active_commits -= 1
                current_batch += 1

            # logging.debug("[COMMIT] Preparing batch commit -- executing!")

            try:
                # print("Sending!!!")
                response = self.client.send_message_batch(QueueUrl=self.queue_url,
                                                          Entries=insertables)
                # logging.debug(f"SQS response: {response}")
            except Exception as e:  # TODO: too vague
                print(f"WAS UNABLE TO PROPERLY CONNECT to SQS QUEUE: {e}")

            self.success_group_commit_count += current_batch


    def increment(self):
        '''Increments the global COUNT variable. Used for debugging. '''
        global COUNT
        COUNT = COUNT + 1
        return COUNT


    def _is_ftp_dir(self, ftp_handle, name, guess_by_extension=True):
        """ Test to see if we are in an ftp directory
            :param ftp_handle -- our link to the ftp directory connection
            :param name -- file name
            :param guest_by_extension
        """

        if guess_by_extension is True:
            if (name[-4] == '.') or (name[-3] == '.') or (name[-2] == '.'):
                return False

        original_cwd = ftp_handle.pwd()  # Current Working Directory
        try:
            ftp_handle.cwd(name)  # see if name represents a child-subdirectory
            ftp_handle.cwd(original_cwd)  # It works! Go back. Continue DFS.
            return True
        except:
            return False


    def _make_parent_dir(self, fpath):
        """ Does path (given a file) actually exist?
            :param fpath -- path to the file. """
        dirname = os.path.dirname(fpath)
        while not os.path.exists(dirname):
            try:
                os.mkdir(dirname)
            except:
                self._make_parent_dir(dirname)


    def is_compressed(self, filename):
        filename = filename.lower()
        zips = ["zip", "tar", "z", "gz"]
        return filename, zips


    # TODO: [TYLER] Local deployment complicates this. Will return to this.
    def dup_check(file_id):
        return True


    def write_to_postgres(self, info_tuple):
        """ Take a tuple containing path and file-size, and update the table with this information.  This should also
            inform the user that they. """

        count = self.increment()

        if count % 1000 == 0:
            print("Files processed thus far: " + str(count))


        #print(info_tuple)

        return True


    # TODO: Find a means for subdividing the repository for faster DFS.
    def rename_file(self, useremail, filepath):
        try:
            os.chdir("../../tmp")  # TODO: I don't think I need this here.
        except:
            pass

        file_list = filepath

        if '/' in file_list:
            file_list = filepath.split('/')

        elif '\\' in file_list:
            file_list = filepath.split('\\')

        use_rmash = ('-'.join(useremail.split('@'))).replace('.', '')
        file_mash = ''.join(file_list)
        new_name = use_rmash + '-' + file_mash

        return new_name

    def get_metadata(self, directory):
        """Crawl local filesystem. Return state (i.e, file list)
            :param directory string representing root level directory path """
        pass
        r = []
        subdirs = [x[0] for x in os.walk(directory)]
        for subdir in subdirs:
            files = os.walk(subdir).__next__()[2]

            if self.grouper == "gdrive":
                grouper = simple_ext_grouper.SimpleExtensionGrouper(creds=None)

                fdict_ls = []
                for file in files:
                    file_path = subdir + "/" + file
                    size = os.stat(file_path).st_size
                    fdict = {'extension': file.split("/")[-1],
                             'id': file_path,
                             'size': size,
                             'mimeType': "mimeType>>None",
                             'is_gdoc': False}  # This is false because it's just a file on the FS
                    fdict_ls.append(fdict)

                families, _ = grouper.gen_families(fdict_ls)

                for family in families:
                    self.csv_handle.write(json.dumps(family))
                    self.csv_handle.write("\n")

            elif self.grouper == "matio":
                print("TODO: this. ")


        return r


if __name__ == "__main__":

    crawler = LocalCrawler('hello-test1')
    crawler.get_metadata('/Users/tylerskluzacek/Desktop')
