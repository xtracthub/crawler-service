
import os
import sys
import json
import uuid
import time
import logging
import threading
import pickle as pkl

from datetime import datetime
from utils.pg_utils import pg_conn, pg_list
import psycopg2

from queue import Queue
from globus_sdk.exc import GlobusAPIError, TransferAPIError, GlobusTimeoutError
from globus_sdk import (TransferClient, AccessTokenAuthorizer, ConfidentialAppAuthClient)

from .groupers import matio_grouper

from .base import Crawler


class GlobusCrawler(Crawler):

    def __init__(self, eid, path, crawl_id, trans_token, auth_token, grouper_name=None, logging_level='info'):
        Crawler.__init__(self)
        self.path = path
        self.eid = eid
        self.group_count = 0
        self.transfer_token = trans_token
        self.auth_token = auth_token
        self.conn = pg_conn()
        self.crawl_id = crawl_id

        self.crawl_status = "STARTING"
        self.worker_status_dict = {}
        self.idle_worker_count = 0
        self.max_crawl_threads = 20

        self.count_groups_crawled = 0
        self.count_files_crawled = 0
        self.count_bytes_crawled = 0

        self.images = []
        self.matio = []
        self.keyword = []
        self.jsonxml = []

        if grouper_name == 'matio':
            self.grouper = matio_grouper.MatIOGrouper()

        try:
            self.token_owner = self.get_uid_from_token()
        except:  # TODO: Real auth that's not just printing.
            logging.info("Unable to authenticate user: Invalid Token. Aborting crawl.")

        self.logging_level = logging_level

        if self.logging_level == 'debug':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler_debug.log', level=logging.DEBUG)
        elif self.logging_level == 'info':
            logging.basicConfig(format='%(asctime)s - %(message)s', filename='crawler_info.log', level=logging.INFO)
        else:
            raise KeyError("Only logging levels '-d / debug' and '-i / info' are supported.")


    def db_crawl_end(self):
        cur = self.conn.cursor()
        query = f"UPDATE crawls SET status='complete', ended_on='{datetime.utcnow()}' WHERE crawl_id='{self.crawl_id}';"
        cur.execute(query)

        return self.conn.commit()

    def get_extension(self, filepath):
        """Returns the extension of a filepath.
        Parameter:
        filepath (str): Filepath to get extension of.
        Return:
        extension (str): Extension of filepath.
        """
        filename = filepath.split('/')[-1]
        extension = None

        if '.' in filename:
            extension = filename.split('.')[-1]
        return extension

    def get_uid_from_token(self):
        # Step 1: Get Auth Client with Secrets.
        client_id = os.getenv("GLOBUS_FUNCX_CLIENT")
        secret = os.getenv("GLOBUS_FUNCX_SECRET")

        # Step 2: Transform token and introspect it.
        conf_app_client = ConfidentialAppAuthClient(client_id, secret)
        token = str.replace(str(self.auth_token), 'Bearer ', '')

        time0 = time.time()
        auth_detail = conf_app_client.oauth2_token_introspect(token)
        time1 = time.time()
        logging.info(f"INTROSPECT TIME: {time1-time0}")

        uid = auth_detail['username']

        return uid

    def gen_group_id(self):
        return uuid.uuid4()

    def get_transfer(self):
        transfer_token = self.transfer_token
        authorizer = AccessTokenAuthorizer(transfer_token)
        transfer = TransferClient(authorizer=authorizer)

        # Print out a directory listing from an endpoint
        try:
            transfer.endpoint_autoactivate(self.eid)
        except GlobusAPIError as ex:
            logging.error(ex)
            if ex.http_status == 401:
                sys.exit('Refresh token has expired. '
                         'Please delete refresh-tokens.json and try again.')
            else:
                raise ex
        return transfer

    def launch_crawl_worker(self, transfer, worker_id):
        logging.basicConfig(format=f"%(asctime)s - %(message)s', filename='crawler_{worker_id}.log", level=logging.INFO)

        self.worker_status_dict[worker_id] = "STARTING"

        while True:
            t_start = time.time()
            all_file_mdata = {}  # Holds all metadata for a given Globus directory.

            # If so, then we want the worker to return.
            if self.to_crawl.empty():
                # This worker sees an empty queue, AND IF NOT ALREADY "IDLE", should become "IDLE"
                if self.worker_status_dict[worker_id] is not "IDLE":
                    logging.info(f"Worker ID: {worker_id} demoted to IDLE.")
                    self.worker_status_dict[worker_id] = "IDLE"
                    self.idle_worker_count += 1

                # If to_crawl is empty, we want to check and see if other crawl_workers idle AND not in 'starting state'
                # If all of the workers are idle AND state != 'STARTING'.
                if self.idle_worker_count >= self.max_crawl_threads:
                    logging.info(f"Worker ID: {worker_id} is terminating.")
                    return "CRAWL--COMPLETE"  # TODO: Behavior for collapsing a thread w/ no real return val?
                continue

            # OTHERWISE, pluck an item from queue.
            else:
                # Catch the RARE race condition error where queue HAD elements in check, but has since become empty.
                try:
                    cur_dir = self.to_crawl.get()
                    restart_loop = False
                except Exception as e:
                    logging.error("Caught the following race condition exception... ignoring...")
                    logging.error(e)

                    # Go back to beginning and check queue again.
                    continue

            # In the case where we successfully extracted from queue AND worker not "ACTIVE", make it active.
            if self.worker_status_dict[worker_id] is not "ACTIVE":
                self.worker_status_dict[worker_id] = "ACTIVE"
                logging.info(f"Worker ID: {worker_id} promoted to ACTIVE.")

            dir_contents = []
            try:
                while True:
                    try:
                        dir_contents = transfer.operation_ls(self.eid, path=cur_dir)
                        break

                    except GlobusTimeoutError as e:
                        logging.error("Globus Timeout Error -- retrying")
                        logging.info(e)
                        print(e)
                        pass

                    except Exception as e:

                        logging.error(str(e))
                        print(e)
                        if '502' in str(e)[0:4]:
                            logging.error("Directory too large...")
                            restart_loop = True
                            break

                        logging.error(f"Caught error : {e}")
                        logging.error(f"Offending directory: {cur_dir}")
                        time.sleep(0.25)

                if restart_loop:
                    continue

                # Step 1. All files have own file metadata.
                f_names = []
                for entry in dir_contents:

                    logging.debug(f"[DEBUG] Entry: {entry}")

                    full_path = cur_dir + "/" + entry['name']
                    if entry['type'] == 'file':

                        f_names.append(full_path)
                        extension = self.get_extension(entry["name"])

                        logging.debug(f"Metadata for full path: {entry}")
                        all_file_mdata[full_path] = {"physical": {"size": entry["size"],
                                                                  "extension": extension, "path_type": "globus"}}

                    elif entry["type"] == "dir":
                        full_path = cur_dir + "/" + entry['name']
                        self.to_crawl.put(full_path)

                logging.debug(f"Finished parsing files. Metadata: {all_file_mdata}")

                #  We want to process each potential group of files.
                group_start_t = time.time()
                families = self.grouper.group(f_names)
                group_end_t = time.time()

                # For all families
                for family in families:

                    logging.debug(f"Preparing family for DB ingest: {family}")

                    tracked_files = set()
                    num_file_count = 0
                    num_bytes_count = 0

                    groups = families[family]["groups"]

                    # For all groups in the family
                    for group in groups:

                        self.count_groups_crawled += 1
                        parser = groups[group]["parser"]
                        logging.debug(f"Parser: {parser}")

                        gr_id = group
                        file_list = groups[group]["files"]

                        logging.debug("IN GROUP-BY-PARSER LOOP...")
                        logging.debug(f"Group Tuple: {group}")

                        group_info = {"group_id": gr_id, "parser": parser, "files": [], "mdata": []}
                        group_info["files"] = file_list

                        logging.debug(f"Processing number of files: {len(file_list)}")
                        for f in file_list:

                            group_info["mdata"].append({"file": f, "blob": all_file_mdata[f]})

                            if f not in tracked_files:
                                # print(f"Found new file: {f}")
                                tracked_files.add(f)
                                num_file_count += 1
                                self.count_files_crawled += 1
                                num_bytes_count += all_file_mdata[f]["physical"]["size"]
                                self.count_bytes_crawled += all_file_mdata[f]["physical"]["size"]

                        logging.info(group_info)
                        cur = self.conn.cursor()

                        try:
                            files = pg_list(group_info["files"])
                            parsers = pg_list(['crawler'])

                        except ValueError as e:
                            logging.error(f"Caught ValueError {e}")
                            self.failed_groups["illegal_char"].append((group_info["files"], ['crawler']))
                            logging.error("Continuing!")

                        else:
                            t_end = time.time()

                            try:
                                query = f"INSERT INTO group_metadata_2 (group_id, crawl_id, metadata, files, parsers, " \
                                    f"owner, family_id, crawl_start, crawl_end, group_start, group_end, status) " \
                                    f"VALUES ('{gr_id}', '{self.crawl_id}', {psycopg2.Binary(pkl.dumps(group_info))}, " \
                                    f"'{files}', '{parsers}', " \
                                    f"'{self.token_owner}', '{family}', {t_start},{t_end}, {group_start_t}, {group_end_t}, '{'crawled'}')"

                                logging.info(f"Group Metadata query: {query}")
                                self.group_count += 1
                                cur.execute(query)
                            except Exception as e:
                                print(group_info['files'])
                                # TODO: SET TO FAILED.
                                # TODO: Check to see that this rollback actually works.
                                self.conn.rollback()
                                print(e)
                                print("SET TO FAILED")
                                self.conn.close()
                                self.conn = pg_conn()
                                continue


                    try:
                        # TODO: I think the family needs to fail or something if one/more of its groups failed?
                        # Update familes table here.
                        fam_cur = self.conn.cursor()
                        fam_update_q = f"""INSERT INTO families (family_id, status, total_size, total_files, crawl_id) VALUES 
                        ('{family}', 'INIT', {num_bytes_count}, {num_file_count}, '{self.crawl_id}') ;"""
                        fam_cur.execute(fam_update_q)
                        self.conn.commit()
                    except psycopg2.DatabaseError as e:
                        self.conn.rollback()
                        print(e)
                        print("SET AS FAILED 2.")
                        self.conn.close()
                        self.conn = pg_conn()
                        continue

            except TransferAPIError as e:
                logging.error("Problem directory {}".format(cur_dir))
                logging.error("Transfer client received the following error:")
                logging.error(e)
                print(e)
                self.failed_dirs["failed"].append(cur_dir)
                continue

    def crawl(self, transfer):
        dir_name = "./xtract_metadata"
        os.makedirs(dir_name, exist_ok=True)

        self.failed_dirs = {"failed": []}
        self.failed_groups = {"illegal_char": []}

        self.to_crawl = Queue()
        self.to_crawl.put(self.path)

        cur = self.conn.cursor()
        now_time = datetime.utcnow()
        crawl_update = f"INSERT INTO crawls (crawl_id, started_on) VALUES " \
            f"('{self.crawl_id}', '{now_time}');"
        cur.execute(crawl_update)
        self.conn.commit()

        list_threads = []
        for i in range(self.max_crawl_threads):
            t = threading.Thread(target=self.launch_crawl_worker, args=(transfer, i))
            list_threads.append(t)
            t.start()

        for t in list_threads:
            t.join()

        logging.info(f"\n***FINAL groups processed for crawl_id {self.crawl_id}: {self.group_count}***")
        logging.info(f"\n*** CRAWL COMPLETE  (ID: {self.crawl_id})***")

        self.db_crawl_end()

        with open('failed_dirs.json', 'w') as fp:
            json.dump(self.failed_dirs, fp)

        with open('failed_groups.json', 'w') as gp:
            json.dump(self.failed_groups, gp)
