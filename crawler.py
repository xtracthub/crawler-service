
"""
    [Docker_main]: This file allows the Docker container to scoop up the path and crawler_type from the 'docker run'
    args and it launches the appropriate type of crawler.
    1. Posix: allows you to search a local file system
    2. FTP: allows you to search an FTP server
    3. Globus: allows you to recursively scan a Globus endpoint (i.e., remote FS)

    Author: Tyler J. Skluzacek
    Last-Updated: 01/16/2019
"""

import posix_crawler


def launch_crawler(crawler_type, json_or_server, path, extracted_files_dir, json_name=None, conn=None, cur=None):

    if crawler_type == 'local' or 'posix':
        if json_or_server == "json":
            response = posix_crawler.launch_crawler("json", path, extracted_files_dir, json_name=json_name)
        elif json_or_server == "server":
            response = posix_crawler.launch_crawler("server", path, extracted_files_dir, conn=conn, cur=cur)
        else:
            print("Invalid output type")

    elif crawler_type == 'globus':
        print("TODO: ADD GLOBUS!")

    return response



