from flask import Flask

from spark.logs import Logs

from api import api_bp

app = Flask(__name__)

app.register_blueprint(api_bp)

logs = Logs()


def main():
    app.run(debug=True)
    logs.session.stop()


if __name__ == '__main__':
    main()
