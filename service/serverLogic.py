# import os
# import re
import validators
import base64
import collections
from pymongo import MongoClient


# from collections import OrderedDict
#
# import config
# import service
# from search_engine.indexer import Indexer
# from data_source.data_source import DataSource
# from semantic_labeling.run_experiments import SemanticLabeler


not_allowed_chars = '[\\/*?"<>|\s\t]'

NAMESPACE = "namespace"
ID = "_id"
TIMESTAMP = "timestamp"
TYPEID = "typeId"
TYPE_ID = "type_id"
COLUMN_ID = "column_id"
COLUMNS = "columns"
DATA = "data"
DATA_TYPE = "dataType"
DATA_TYPE_SEMANTIC_TYPE = "type"
DATA_TYPE_COLUMN = "column"
DATA_TYPE_MODEL = "model"

NAMESPACES = "namespaces"
COLUMN_NAME = "columnName"
COLUMN_NAMES = "columnNames"
SOURCE_NAME = "sourceName"
SOURCE_NAMES = "sourceNames"
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
        self.db = MongoClient().data.posts
        # self.source_map = OrderedDict()
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
    def message(message, code):
        return message, code, {'Access-Control-Allow-Origin': '*'}


    @staticmethod
    def _get_type_id(class_, property_):
        return base64.b64encode(class_) + "-" + base64.b64encode(property_)


    @staticmethod
    def _get_column_id(type_id, column_name, source_name, model):
        return type_id + "-" + base64.b64encode(column_name) + "-" + base64.b64encode(source_name) + "-" + base64.b64encode(model)


    def _create_semantic_type(self, class_, property_, force=False):
        # Verify that class is a valid uri and namespace is a valid uri
        namespace = "/".join(class_.split("/")[:-1])
        if not validators.url(class_) or not validators.url(namespace):
            return self.message("Invalid class URI was given", 400)

        # Actually add the type
        type_id = self._get_type_id(class_, property_)
        db_body = {ID: type_id, DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, CLASS: class_, PROPERTY: property_, NAMESPACE: namespace}
        if force:
            # TODO: Also delete all other data associated with this one
            self.db.delete_many(db_body)
        else:
            if self.db.find_one(db_body):
                return self.message("Semantic type already exists", 409)
        self.db.insert_one(db_body)
        return self.message(type_id, 201)


    def _create_column(self, type_id, column_name, source_name, model, data=[], force=False):
        column_id = self._get_column_id(type_id, column_name, source_name, model)
        db_body = {ID: column_id, DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id, COLUMN_NAME: column_name, SOURCE_NAME: source_name, MODEL: model, DATA: data}
        if force:
            self.db.delete_many(db_body)
        else:
            if self.db.find_one(db_body):
                return self.message("Column already exists", 409)
        self.db.insert_one(db_body)
        return self.message(column_id, 201)


    def _add_data_to_column(self, column_id, data, replace=False):
        return


    ################ Predict ################

    def predict_post(self, args, body):
        #### Assert args and body are valid
        if body is None or body == "":
            return self.message("Invalid message body", 400)
        args = args.copy()
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        col_name = args.pop(COLUMN_NAME, None)
        model = args.pop(MODEL, None)
        source_col = args.pop(SOURCE_NAME, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Predict the types
        return self.message("Method partially implemented", 601)


    ################ SemanticTypes ################

    def semantic_types_get(self, args):
        #### Assert args are valid and make the db query
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_columns = args.pop(RETURN_COLUMNS, None)
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False
        return_columns = True if return_columns is not None and return_columns.lower() == "true" else return_column_data

        #### Get the types
        # Get all of the semantic types
        db_body = {DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE}
        if class_ is not None: db_body[CLASS] = class_
        if property_ is not None: db_body[PROPERTY] = property_
        if namespaces is not None: db_body[NAMESPACE] = {"$in": namespaces}
        result = list(self.db.find(db_body))
        return_body = []
        for t in result:
            o = collections.OrderedDict()
            o[TYPE_ID] = t[ID]
            o[CLASS] = t[CLASS]
            o[PROPERTY] = t[PROPERTY]
            o[NAMESPACE] = t[NAMESPACE]
            return_body.append(o)
        # Add the columns to the return_body if applicable
        if return_columns:
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN}
            if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
            if column_names is not None: db_body[SOURCE_NAME] = {"$in": column_names}
            if column_ids is not None: db_body[SOURCE_NAME] = {"$in": column_ids}
            if models is not None: db_body[SOURCE_NAME] = {"$in": models}
            for t in return_body:
                db_body[TYPEID] = t_id = t[TYPE_ID]
                result = list(self.db.find(db_body))
                for i in return_body:
                    if i[TYPE_ID] == t_id:
                        # TODO: FINISH ME
                        t[COLUMNS] = result

        return self.message(return_body, 601)


    def semantic_types_post(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if class_ is None or property_ is None:
            return self.message("Both 'class' and 'property' must be specified", 400)

        #### Add the type
        return self._create_semantic_type(class_, property_)


    def semantic_types_put(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if class_ is None or property_ is None:
            return self.message("Both 'class' and 'property' must be specified", 400)

        #### Add the type
        return self._create_semantic_type(class_, property_, True)


    def semantic_types_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        delete_all = args.pop(DELETE_ALL, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Delete the types
        self.db.delete_many({CLASS: class_, PROPERTY: property_})
        return self.message("Method partially implemented", 601)


    ################ SemanticTypesColumns ################

    def semantic_types_columns_get(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False

        #### Get the columns
        return self.message("Method partially implemented", 601)


    def semantic_types_columns_post(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if column_name is None or source_name is None:
            return self.message("Either 'columnName' or 'sourceColumn' was omitted and they are both required")
        if model is None:
            model = "default"

        #### Add the column
        return self._create_column(type_id, column_name, source_name, model, body.split("\n")) if body is not None and body.strip() != "" else self._create_column(type_id, column_name, source_name,
                                                                                                                                                                   model)


    def semantic_types_columns_put(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        if column_name is None or source_name is None:
            return self.message("Either 'columnName' or 'sourceColumn' was omitted and they are both required")
        if model is None:
            model = "default"

        #### Add the column
        return self._create_column(type_id, column_name, source_name, model, body.split("\n"), True) if body is not None and body.strip() != "" else self._create_column(type_id, column_name,
                                                                                                                                                                         source_name, model, force=True)


    def semantic_types_columns_delete(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Delete the columns
        return self.message("Method partially implemented", 601)


    ################ SemanticTypesColumnData ################

    def semantic_types_column_data_get(self, type_id, column_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return self.message("Invalid column_id", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Get the column
        return self.message("Method partially implemented", 601)


    def semantic_types_column_data_post(self, type_id, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return self.message("Invalid message body", 400)
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return self.message("Invalid column_id", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Add the data
        return self.message("Method partially implemented", 601)


    def semantic_types_column_data_put(self, type_id, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return self.message("Invalid message body", 400)
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return self.message("Invalid column_id", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Replace the data
        return self.message("Method partially implemented", 601)


    def semantic_types_column_data_delete(self, type_id, column_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return self.message("Invalid type_id", 400)
        if column_id is None or len(column_id) < 1:
            return self.message("Invalid column_id", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Delete the data
        return self.message("Method partially implemented", 601)


    ################ Models ################

    def models_get(self, args):
        #### Assert args are valid
        args = args.copy()
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        show_all = args.pop(SHOW_ALL, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)
        show_all = True if show_all is not None and show_all.lower() == "true" else False

        #### Find the model
        return self.message("Method partially implemented", 601)


    def models_post(self, args, body):
        #### Assert args are valid
        if body is None or len(body) < 1:
            return self.message("Invalid message body", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Add the model

        return self.message("Method partially implemented", 601)


    def models_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        if len(args) > 0:
            return self.message("The following query parameters are invalid:  " + str(args.keys()), 400)

        #### Find the model
        return self.message("Method partially implemented", 601)


    ################ ModelData ################

    def model_data_get(self, model_id, args):
        if model_id is None or len(model_id) < 1:
            return self.message("Invalid model_id", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Get the model
        return self.message("Method partially implemented", 601)


    def model_data_post(self, model_id, args, body):
        if model_id is None or len(model_id) < 1:
            return self.message("Invalid model_id", 400)
        if body is None or len(body) < 1:
            return self.message("Invalid message body", 400)
        if len(args) > 0:
            return self.message("Invalid arguments, there should be none", 400)

        #### Process the data
        return self.message("Method partially implemented", 601)
