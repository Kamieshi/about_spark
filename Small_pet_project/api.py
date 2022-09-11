from flask import Blueprint
from flask_restx import Api
from namespaces.logs_ns import logs_ns

api_bp = Blueprint('api', __name__, url_prefix='/api/v1')
api = Api(api_bp, doc='/doc/')

api.add_namespace(logs_ns)
