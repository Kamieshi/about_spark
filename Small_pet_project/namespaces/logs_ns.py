import json
from typing import List
from flask import request
from flask_restx import Resource, Namespace
from pyspark import Row

from spark.logs import Logs

logs_ns = Namespace("Logs Namespace", path="/logs")
log = Logs()


@logs_ns.route('/')
class LogsData(Resource):
    @logs_ns.param("level", "level message", "query")
    def get(self):
        level = request.args.get("level")
        level = level if level else ""
        logs: List[Row] = Logs.get_logs(level=level)
        return json.dumps(
            [item.asDict(True) for item in logs], indent=4, sort_keys=True, default=str
        )


@logs_ns.route('/schema')
class LogsSchema(Resource):
    def get(self):
        schema = Logs.schema()
        return schema

@logs_ns.route('/backUP')
class LogsSchema(Resource):
    def get(self):
        schema = Logs.schema()
        return schema