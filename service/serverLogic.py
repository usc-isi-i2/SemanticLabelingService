# import os
# import re
import validators
import base64
from pymongo import MongoClient
# from collections import OrderedDict
#
# import config
# import service
# from search_engine.indexer import Indexer
# from data_source.data_source import DataSource
# from semantic_labeling.run_experiments import SemanticLabeler

from service.errors import *


not_allowed_chars = '[\\/*?"<>|\s\t]'

NAMESPACE = "namespace"
ID = "_id"
TIMESTAMP = "timestamp"

NAMESPACES = "namespaces"
COLUMN_NAME = "columnName"
COLUMN_NAMES = "columnNames"
SOURCE_COLUMN = "sourceColumn"
SOURCE_COLUMNS = "sourceColumns"
COLUMN_IDS = "columnIds"
MODEL = "model"
MODELS = "models"
MODEL_NAMES = "modelNames"
MODEL_DESC = "modelDesc"
MODEL_IDS = "modelIds"
CLASS = "class"
PROPERTY = "property"
RETURN_COLUMNS = "returnColumns"
RETURN_COLUMN_DATA = "returnColumnData"
DELETE_ALL = "deleteAll"
FORCE = "force"
SHOW_ALL = "showAllData"


class Server(object):
    def __init__(self):
        # self.source_map = OrderedDict()
        self.db = MongoClient().data
        # self.indexer = Indexer()
        # # self.data_set_map[namespace] = source_map
        #
        # # Load the data sources
        # namespace = service.encode("http://schema.org")
        # folder_path = os.path.join(config.UPLOAD_FOLDER, namespace)
        # for semantic_type_encoded in os.listdir(folder_path):
        #     for source_file in os.listdir(os.path.join(folder_path, semantic_type_encoded)):
        #         for column_encoded in os.listdir(os.path.join(folder_path, semantic_type_encoded, source_file)):
        #             file_path = os.path.join(folder_path, semantic_type_encoded, source_file, column_encoded)
        #             source = DataSource(lib.column_path_to_ids(file_path)[1])
        #             source.read(file_path)
        #             self.source_map[source_file + column_encoded] = source
        # namespace_safe = re.sub(not_allowed_chars, "!", namespace).lower()
        # for source in self.source_map.keys():
        #     self.source_map[source].save(namespace_safe)
        print " -- Need to figure out init -- "

    @staticmethod
    def _get_type_id(class_, property_):
        return base64.b64encode(class_) + "-" + base64.b64encode(property_)

    def _create_semantic_type(self, class_, property_, force=False):
        # Verify that class is a valid uri and namespace is a valid uri
        namespace = "/".join(class_.split("/")[:-1])
        if not validators.url(class_) or not validators.url(namespace):
            return message("Invalid class URI was given", 400)

        # Actually add the type
        type_id = self._get_type_id(class_, property_)
        db_body = {CLASS: class_, PROPERTY: property_, NAMESPACE: namespace, ID: type_id}
        if force:
            # TODO: Also delete all other data associated with this one
            self.db.posts.delete_many(db_body)
        else:
            if self.db.posts.find_one(db_body):
                return message("Semantic type already exists", 409)
        self.db.posts.insert_one(db_body)
        return message(type_id, 201)


    def _create_column(self, type_id, column_name, source_column, model, force=False):
        return "column_id"


    def _add_data_to_column(self, type_id, column_id, data):
        return


    ################ Predict ################

    def predict_post(self, args, body):
        #### Assert args and body are valid
        if body is None or body == "":
            return message("Invalid message body", 400)
        args = args.copy()
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        col_name = args.pop(COLUMN_NAME, None)
        model = args.pop(MODEL, None)
        source_col = args.pop(SOURCE_COLUMN, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Predict the types
        return message("Method partially implemented", 601)


    ################ SemanticTypes ################

    def semantic_types_get(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        source_columns = args.pop(SOURCE_COLUMNS).split(",") if args.get(SOURCE_COLUMNS) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_columns = args.pop(RETURN_COLUMNS, None)
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False
        return_columns = True if return_columns is not None and return_columns.lower() == "true" else return_column_data

        #### Get the types

        return message(str(list(self.db.posts.find({CLASS: class_, PROPERTY: property_}))), 601)


    def semantic_types_post(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if class_ is None or property_ is None:
            return message("Both 'class' and 'property' must be specified", 400)

        #### Add the type
        return self._create_semantic_type(class_, property_)


    def semantic_types_put(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if class_ is None or property_ is None:
            return message("Both 'class' and 'property' must be specified", 400)

        #### Add the type
        return self._create_semantic_type(class_, property_, True)


    def semantic_types_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        source_columns = args.pop(SOURCE_COLUMNS).split(",") if args.get(SOURCE_COLUMNS) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        delete_all = args.pop(DELETE_ALL, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Delete the types
        self.db.posts.delete_many({CLASS: class_, PROPERTY: property_})
        return message("Method partially implemented", 601)


    ################ SemanticTypesColumns ################

    def semantic_types_columns_get(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_columns = args.pop(SOURCE_COLUMNS).split(",") if args.get(SOURCE_COLUMNS) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False

        #### Get the columns
        return message("Method partially implemented", 601)


    def semantic_types_columns_post(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_column = args.pop(SOURCE_COLUMN, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if column_name is None or source_column is None:
            return message("Either 'columnName' or 'sourceColumn' was omitted and they are both required")
        if model is None:
            model = "default"

        #### Add the column
        column_id = self._create_column(type_id, column_name, source_column, model, False)
        if column_id is None:
            return message("Column already exists", 409)
        if body is not None and body.strip() != "":
            self._add_data_to_column(type_id, column_id, body.split("\n"))
        return message(column_id, 201)


    def semantic_types_columns_put(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_column = args.pop(SOURCE_COLUMN, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if column_name is None or source_column is None:
            return message("Either 'columnName' or 'sourceColumn' was omitted and they are both required")
        if model is None:
            model = "default"

        #### Add the column
        column_id = self._create_column(type_id, column_name, source_column, model, True)
        if body is not None and body.strip() != "":
            self._add_data_to_column(type_id, column_id, body.split("\n"))

        return message("Method partially implemented", 601)
        # return message(column_id, 201)


    def semantic_types_columns_delete(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_columns = args.pop(SOURCE_COLUMNS).split(",") if args.get(SOURCE_COLUMNS) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Delete the columns
        return message("Method partially implemented", 601)


    ################ SemanticTypesColumnData ################

    def semantic_types_column_data_get(self, type_id, column_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return message("Invalid column_id", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Get the column
        return message("Method partially implemented", 601)


    def semantic_types_column_data_post(self, type_id, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return message("Invalid message body", 400)
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return message("Invalid column_id", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Add the data
        return message("Method partially implemented", 601)


    def semantic_types_column_data_put(self, type_id, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return message("Invalid message body", 400)
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return message("Invalid column_id", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Replace the data
        return message("Method partially implemented", 601)


    def semantic_types_column_data_delete(self, type_id, column_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return message("Invalid column_id", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Delete the data
        return message("Method partially implemented", 601)


    ################ Models ################

    def models_get(self, args):
        #### Assert args are valid
        args = args.copy()
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        show_all = args.pop(SHOW_ALL, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)
        show_all = True if show_all is not None and show_all.lower() == "true" else False

        #### Find the model
        return message("Method partially implemented", 601)


    def models_post(self, args, body):
        #### Assert args are valid
        if body is None or len(body) < 1:
            return message("Invalid message body", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Add the model

        return message("Method partially implemented", 601)


    def models_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        if len(args) > 0:
            return message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Find the model
        return message("Method partially implemented", 601)


    ################ ModelData ################

    def model_data_get(self, model_id, args):
        if model_id is None or len(model_id) < 1:
            return message("Invalid model_id", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Get the model
        return message("Method partially implemented", 601)


    def model_data_post(self, model_id, args, body):
        if model_id is None or len(model_id) < 1:
            return message("Invalid model_id", 400)
        if body is None or len(body) < 1:
            return message("Invalid message body", 400)
        if len(args) > 0:
            return message("Invalid arguments, there should be none", 400)

        #### Process the data
        return message("Method partially implemented", 601)
