
from flask import Flask, request, jsonify
import psycopg2.extras
import json
from legacy import utils, crawler
import uuid

app = Flask(__name__)


db_cfg_path = "/home/skluzacek/skluma-db.json"
with open(db_cfg_path) as f:
    db_login_data = json.load(f)


pg_host = db_login_data["host"]
pg_user = db_login_data["user"]
pg_password = db_login_data["password"]
pg_database = db_login_data["database"]

conn = psycopg2.connect(host=pg_host, user=pg_user, password=pg_password, database=pg_database)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


@app.route('/', methods=['GET', 'POST'])
def test_connectivity():
    return "You've sent an empty request to crawler service."


@app.route('/status', methods=['GET'])
def get_job_status():

    get_req = request.json
    task_id = get_req["task_id"]

    status = utils.get_task_status(cur, task_id)

    return jsonify({"status": status})


@app.route('/process_dir', methods=['POST'])
def launch_crawler():

    # LOCAL SAMPLE:
    # curl --header "Content-Type: application/json" --request POST
    # --data '{"target":"/tylerskluzacek/","email":"skluzacek@uchicago.edu"}' http://localhost:5001/process_file

    # STATUS: PENDING, CRAWLING, PREEXTRACT, EXTRACT, POSTPROCESS, COMPLETED

    post_req = request.json
    task_uuid = str(uuid.uuid4())

    crawlable_directory = post_req["target"]
    user_email = post_req["email"]

    try:
        # Update TASKS table
        user_id = utils.get_user_id(cur, user_email)

        tasks_query = """INSERT INTO tasks (task_uuid, user_id, status, crawlpath) 
        VALUES ('{}', '{}', 'PENDING', '{}');"""\
            .format(task_uuid, user_id, crawlable_directory)

        cur.execute(tasks_query)
        conn.commit()

    except KeyError as e:
        print(e)

    response = crawler.launch_crawler(conn, cur, 'posix', crawlable_directory)

    return jsonify(response)


@app.route('/kill', methods=['POST'])
def killer():
    """
    Run when it's time to spin down this crawler instance.
    Should update DB and give 'READY' response.
    :return:
    """

    # TODO 1: Check that no jobs currently running.

    # TODO 2: If done, close the database.

    return jsonify({"status": "COMPLETED"})


if __name__ == '__main__':
    app.run(debug=True, port=5001)
