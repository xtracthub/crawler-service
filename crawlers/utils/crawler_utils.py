
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

    conn.commit()
    conn.close()
    print(f"Successfully pushed new crawl data to Postgres!")


