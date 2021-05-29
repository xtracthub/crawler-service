from flask import Flask


application = Flask(__name__)


@application.route('/', methods=['POST', 'GET'])
def hello():
    return 'hello, world'


if __name__ == '__main__':
    application.run(debug=True, threaded=True)