
import os
from utils.pg_utils import pg_conn
from globus_sdk import AccessTokenAuthorizer, ConfidentialAppAuthClient


def get_auth_client():
    """
    Create an AuthClient for the portal
    """
    return ConfidentialAppAuthClient(os.environ['GLOBUS_FUNCX_CLIENT'], os.environ['GLOBUS_FUNCX_SECRET'])


def push_to_pg(crawl_id, endpoints):

    conn = pg_conn()
    cursor = conn.cursor()

    # TODO: we should have two tables here.
    # init_query = "INSERT INTO crawls (crawl_id, owner)"

    # TODO: update to one batch insert
    for endpoint in endpoints:

        dir_paths = endpoint['dir_paths']
        # cursor.execute_query

        for dir in dir_paths:
            query = f"INSERT INTO crawl_paths (crawl_id, path, path_type, endpoint_id) VALUES (" \
                f"'{crawl_id}', " \
                f"'{dir}', " \
                f"'{endpoint['repo_type']}', " \
                f"'{endpoint['eid']}')"

            cursor.execute(query)

    stats_init_query = f"INSERT INTO crawl_stats (crawl_id) VALUES ('{crawl_id}');"
    cursor.execute(stats_init_query)
    conn.commit()
    conn.close()
    print(f"Successfully pushed new crawl data to Postgres!")


def get_crawl_status(crawl_id):

    conn = pg_conn()
    cursor = conn.cursor()

    crawl_stats_query = f"SELECT files_crawled, bytes_crawled, groups_crawled " \
                        f"FROM crawl_stats where crawl_id='{crawl_id}';"
    crawl_status_query = f"SELECT status from crawls where crawl_id='{crawl_id}';"

    crawl_stats = dict()
    cursor.execute(crawl_status_query)

    try:
        crawl_status = cursor.fetchall()[0][0]  # There should only be one item, and it is
    except IndexError as e:
        print(f"Caught: {e} -- crawl_id not found!")
        return {'crawl_id': crawl_id, 'error': 'crawl_id not found!'}

    crawl_stats['crawl_id'] = crawl_id
    crawl_stats['crawl_status'] = crawl_status

    # Now get stats.
    cursor.execute(crawl_stats_query)

    files_crawled, bytes_crawled, groups_crawled = cursor.fetchall()[0]
    crawl_stats['files_crawled'] = files_crawled
    crawl_stats['bytes_crawled'] = bytes_crawled
    crawl_stats['groups_crawled'] = groups_crawled

    return crawl_stats

x = get_crawl_status('74a58778-4b92-4637-93d4-d006685ed549')
print(x)