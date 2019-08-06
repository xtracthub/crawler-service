

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
from decompressor import decompress_file


def _get_extension(filepath):
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


def _make_parent_dir(fpath): # TODO: Does this do anything?
    """ Does path (given a file) actually exist?
        :param fpath -- path to the file. """
    dirname = os.path.dirname(fpath)
    while not os.path.exists(dirname):
        try:
            os.mkdir(dirname)
        except:
            _make_parent_dir(dirname)


def is_compressed(filename):
    """Finds whether a filename has a compressed extension.

    Parameter:
    filename (str): Filename to check for compression.

    Return:
    (bool): Whether filename is compressed or not.
    """
    file_type = _get_extension(filename)
    zips = ["zip", "tar", "gz", "tgz", "Z"]

    return True if file_type in zips else False


# TODO: [TYLER] Local deployment complicates this. Will return to this.
def dup_check(file_id):
    return file_id


def md5_hasher(file_path, buff_size=65536):
    """Hashes a file using md5.

    Parameters:
    file_path (str): File path of file to hash.
    buff_size (int): Size of chunks to process file_path in.

    Return:
    (str): Hexidecimal version of md5 hashed file_path.
    """
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
    path, file_size, extension, file_hash = info_tuple
    meta_empty = json.dumps({})

    # TODO: Change this to actual file_ids from files table.
    file_id = 1

    query = """INSERT INTO metadata (file_id, extension, size_mb, file_hash, metadata) VALUES ('{}', '{}, ''{}', '{}', '{}');"""
    query = query.format(file_id, extension, file_size, file_hash, meta_empty)

    print(query)

    cur.execute(query)
    conn.commit()


def recursive_compress_check_helper(extracted_files_dir, compressed_files):
    """Helper function for recursive_compress_check. Recursively checks whether
    there are compressed files in a directory, and extracts them to a separate directory.

    Parameters:
    extracted_files_dir (str): Directory with compressed files to extract.
    compressed_files (list): List of compressed files already extracted.

    Return:
    compressed_files (list): List of extracted compressed files.
    """
    sub_dirs = [x[0] for x in os.walk(extracted_files_dir)]

    for subdir in sub_dirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                if is_compressed(item) and item not in compressed_files:
                    decompress_file(os.path.join(subdir, item),
                                    extracted_files_dir)
                    compressed_files.append(item)
                    compressed_files = recursive_compress_check_helper(extracted_files_dir,
                                                                       compressed_files)

    return compressed_files


def recursive_compress_check(extracted_files_dir):
    """Recursively extracts a directory with compressed files.

    Parameter:
    extract_files_dir (str): Directory with compressed files to extract.
    """
    recursive_compress_check_helper(extracted_files_dir, [])


def get_decompressed_metadata(json_or_server, conn, cur, extracted_files_dir):
    """Gets file path, file size, extension, and file hash of file in a directory.

    Parameters:
    json_or_server (str): "json" or "server", whether to write metadata to a server or to return it as a dictionary.
    conn:
    cur:
    extracted_files_dir (str): Directory of files to extract metadata from.

    Return:
    r (dict): Dictionary of metadata for each file is json_or_server is "json", or else returns empty dict.
    """
    r = {}
    sub_dirs = [x[0] for x in os.walk(extracted_files_dir)]

    for subdir in sub_dirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                file_path = os.path.join(subdir, item)
                file_size = os.stat(file_path).st_size
                extension = _get_extension(file_path)
                file_hash = md5_hasher(file_path)
                if json_or_server == "json":
                    r[file_path] = {"file_path": str(file_path), "file_size": str(file_size), "extension": extension,
                                    "file_hash": file_hash}
                else:
                    try:
                        write_metadata_to_postgres(conn, cur, (file_path, file_size
                                                               , extension,
                                                               file_hash))
                    except psycopg2.Error as e:
                        print(e)
                        pass

    return r


def get_metadata(json_or_server, directory, extracted_files_dir, conn=None, cur=None):
    """Crawl local filesystem. Return state (i.e, file list)

    Parameters:
    json_or_server (str): "json" or "server", whether to write metadata to a server or to return it as a dictionary.
    directory (str): Root level directory path.
    extracted_files_dir (str): Name of directory to extract compressed files to, if the directory doesn't exist, it is
    made.
    conn:
    cur:

    Return:
    r (dict): Dictionary of metadata for each file is json_or_server is "json", or else returns empty dict.
    """

    r = {}
    sub_dirs = [x[0] for x in os.walk(directory)]
    try:
        os.makedirs(extracted_files_dir)
    except:
        pass

    for subdir in sub_dirs:
        files = os.walk(subdir).__next__()[2]
        if len(files) > 0:
            for item in files:
                file_path = os.path.join(subdir, item)
                file_size = os.stat(file_path).st_size
                extension = _get_extension(file_path)
                file_hash = md5_hasher(file_path)
                if json_or_server == "json":
                    r[file_path] = {"file_path": str(file_path), "file_size": str(file_size), "extension": extension,
                                    "file_hash": file_hash}
                else:
                    try:
                        write_metadata_to_postgres(conn, cur, (file_path, file_size, extension, file_hash))
                    except psycopg2.Error as e:
                        print(e)
                        pass

                if is_compressed(item):
                    decompress_file(file_path, extracted_files_dir)

    recursive_compress_check(extracted_files_dir)
    r.update(get_decompressed_metadata(json_or_server, conn, cur, extracted_files_dir))

    return r


def launch_crawler(json_or_server, repo_path, extracted_files_dir, conn=None, cur=None, json_name=None):
    """Crawls through a directory.

    Parameters:
    json_or_server (str): "json" or "server", whether to write metadata to a server or to return it as a dictionary.
    repo_path (str): Root level directory path.
    extracted_files_dir (str): Name of directory to extract compressed files to, if the directory doesn't exist, it is
    made.
    conn:
    cur:
    json_name (str): Name of json file to write file metadata to.

    Return:
    (dict): Dictionary containing crawl time and status.
    """
    t0 = time.time()
    directory_input = repo_path

    if json_or_server == "json":
        metadata_dict = get_metadata("json", directory_input, extracted_files_dir)
        with open(json_name, 'w') as json_file:
            json.dump(metadata_dict, json_file)
    else:
        get_metadata("server", directory_input, extracted_files_dir, conn=None, cur=None)

    t1 = time.time()

    crawl_secs = t1-t0

    return {"crawl_secs": crawl_secs, "status": "DONE"}




