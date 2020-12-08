from airflow.plugins_manager import AirflowPlugin
from hooks.mongo_hook import MongoHook
from operators.mongo_operator import MongoOperator


class MongoPlugin(AirflowPlugin):
    name = "mongo_plugin"
    operators = [MongoOperator]
    hooks = [MongoHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
