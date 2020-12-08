from bson import json_util
import json

from hooks.mongo_hook import MongoHook
from airflow.models import BaseOperator


class MongoOperator(BaseOperator):
    """
    Mongo -> S3
    :param mongo_conn_id:           The source mongo connection id.
    :type mongo_conn_id:            string
    :param mongo_collection:        The source mongo collection.
    :type mongo_collection:         string
    :param mongo_database:          The source mongo database.
    :type mongo_database:           string
    :param mongo_query:             The specified mongo query.
    :type mongo_query:              string
    """

    # TODO This currently sets job = queued and locks job
    template_fields = ['mongo_query']

    def __init__(self,
                 mongo_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 *args, **kwargs):
        super(MongoOperator, self).__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False

        # KWARGS
        self.replace = kwargs.pop('replace', False)

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()

        # Grab collection and execute query according to whether or not it is a pipeline
        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        results = collection.aggregate(self.mongo_query) if self.is_pipeline else collection.find(self.mongo_query)

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))

        print(docs_str)

    def _stringify(self, iter, joinable='\n'):
        """
        Takes an interable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join([json.dumps(doc, default=json_util.default) for doc in iter])

    def transform(self, docs):
        """
        Processes pyMongo cursor and returns single array with each element being
                a JSON serializable dictionary
        MongoToS3Operator.transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just needs to be
            converted into an array.
        """
        return [doc for doc in docs]
