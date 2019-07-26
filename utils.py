
import uuid

# db_cfg_path = "/home/skluzacek/skluma-db.json"
# with open(db_cfg_path) as f:
#     db_login_data = json.load(f)
#
#
# pg_host = db_login_data["host"]
# pg_user = db_login_data["user"]
# pg_password = db_login_data["password"]
# pg_database = db_login_data["database"]
#
# conn = psycopg2.connect(host=pg_host, user=pg_user, password=pg_password, database=pg_database)
# cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def make_task_uuid():
    return uuid.uuid4()


def get_user_id(cur, email_addr):

    user_query = """SELECT id FROM users WHERE email = '{}';""".format(email_addr)
    cur.execute(user_query)
    user_id = cur.fetchall()[0]["id"]

    return user_id


def get_task_status(cur, task_id):

    status_query = """ SELECT status FROM tasks WHERE task_uuid = '{}';""".format(task_id)
    cur.execute(status_query)

    status = cur.fetchall()[0]["status"]

    return status


# TODO: Task listing.
# def list_tasks(conn, cur, username):
#     user_id = get_user_id(conn, cur, username)
#
#     list_query = """ SELECT tasks, start_time FROM """

# get_user_id(conn, cur, 'skluzacek@uchicago.edu')
#
# QUERY = """SELECT * FROM tasks;"""
#
# cur.execute(QUERY)
#
# for row in cur.fetchall():
#     print(row)
