

"""
    This ftp/local-directory_scraper will create a path-directory of all files and filesizes in given FTP server.
    Code can be uncommented to (1) do in a single recursive swath, and (2) actually download files
    a local machine. It implements Depth-First Search (DFS).
    
        @Author: Tyler J. Skluzacek, University of Chicago
        @Email: skluzacek@uchicago.edu
        @Github: tskluzac
        @LastEdited: 01/16/2019
"""

import os
import json
import time
import psycopg2
import hashlib
from decompresser import decompress_file


def _get_extension(filepath):

    filename = filepath.split('/')[-1]
    extension = None

    if '.' in filename:
        extension = filename.split('.')[-1]

    return extension


def _make_parent_dir(fpath):
    """ Does path (given a file) actually exist?
        :param fpath -- path to the file. """
    dirname = os.path.dirname(fpath)
    while not os.path.exists(dirname):
        try:
            os.mkdir(dirname)
        except:
            _make_parent_dir(dirname)


def is_compressed(filename):
    file_type = _get_extension(filename)
    zips = ["zip", "tar", "z", "gz", "tgz", "Z"]

    return True if file_type in zips else False


# TODO: [TYLER] Local deployment complicates this. Will return to this.
def dup_check(file_id):
    return file_id


def md5_hasher(file_path, buff_size=65536):
    md5hash = hashlib.md5()
    with open(file_path, 'rb') as file:
        while True:
            chunk_data = file.read(buff_size)

            if not chunk_data:
                break
            else:
                md5hash.update(chunk_data)

    return md5hash.hexdigest()


def write_metadata_to_postgres(conn, cur, info_tuple):
    """ Take a tuple containing path and file-size, and update the table with this information.  This should also
        inform the user that they. """

    # Postgres annoyingly needs single-quotes on everything
    path, file_size, extension = info_tuple
    meta_empty = json.dumps({})

    # TODO: Change this to actual file_ids from files table.
    file_id = 1

    query = """INSERT INTO metadata (file_id, extension, size_mb, metadata) VALUES ('{}', '{}', {}, '{}');"""
    query = query.format(file_id, extension, file_size, meta_empty)

    print(query)

    cur.execute(query)
    conn.commit()


def get_decompressed_metadata(conn, cur, extracted_files_dir):
    r = []
    sub_dirs = [x[0] for x in os.walk(extracted_files_dir)]

    for subdir in sub_dirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                file_path = subdir + "/" + item
                file_size = os.stat(file_path).st_size
                extension = _get_extension(file_path)
                file_hash = md5_hasher(file_path)
                print(file_path, file_size, extension, file_hash)
                try:
                    write_metadata_to_postgres(conn, cur, (file_path, file_size
                                                           , extension,
                                                           file_hash))
                except psycopg2.Error as e:
                    print(e)
                    pass

    return r


def get_metadata(conn, cur, directory, extracted_files_dir):
    """Crawl local filesystem. Return state (i.e, file list)
        :param directory string representing root level directory path """

    r = []
    sub_dirs = [x[0] for x in os.walk(directory)]

    try:
        os.makedirs(extracted_files_dir)
    except:
        pass

    for subdir in sub_dirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                print(item)
                file_path = subdir + "/" + item
                file_size = os.stat(file_path).st_size
                extension = _get_extension(file_path)
                file_hash = md5_hasher(file_path)
                print(file_path, file_size, extension, file_hash)
                try:
                    write_metadata_to_postgres(conn, cur, (file_path, file_size
                                                           , extension,
                                                           file_hash))
                except psycopg2.Error as e:
                    print(e)
                    pass

                if is_compressed(item):
                    decompress_file(file_path, extracted_files_dir)

    get_decompressed_metadata(conn, cur, extracted_files_dir)

    return r


def launch_crawler(conn, cur, repo_path, extracted_files_dir):
    t0 = time.time()
    directory_input = repo_path
    get_metadata(conn, cur, directory_input, extracted_files_dir)
    t1 = time.time()

    crawl_secs = t1-t0

    return {"crawl_secs": crawl_secs, "status": "DONE"}



