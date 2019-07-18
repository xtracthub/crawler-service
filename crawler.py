
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


def launch_crawler(conn, cur, crawler_type, path):

    if crawler_type == 'local' or 'posix':
        response = posix_crawler.launch_crawler(conn, cur, path)

    elif crawler_type == 'globus':
        print("TODO: ADD GLOBUS!")

    return response


