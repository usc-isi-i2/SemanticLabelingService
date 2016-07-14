# import os
# import re
# from pymongo import MongoClient
# from collections import OrderedDict
#
# import config
# import service
# import lib
# from search_engine.indexer import Indexer
# from data_source.data_source import DataSource
# from semantic_labeling.run_experiments import SemanticLabeler

from service.errors import *


not_allowed_chars = '[\\/*?"<>|\s\t]'

NAMESPACES = "namespaces"
COLUMN_NAME = "columnName"
COLUMN_NAMES = "columnNames"
SOURCE_COLUMN = "sourceColumn"
SOURCE_COLUMNS = "sourceColumns"
COLUMN_IDS = "columnIds"
MODEL = "model"
MODELS = "models"
CLASS = "class"
PROPERTY = "property"
RETURN_COLUMNS = "returnColumns"
RETURN_COLUMN_DATA = "returnColumnData"
DELETE_ALL = "deleteAll"

class Server(object):
    def __init__(self):
        # self.source_map = OrderedDict()
        # self.db = MongoClient().data
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
        return message("Method partially implemented", 601)


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
        return message("Method partially implemented", 601)


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
        return message("Method partially implemented", 601)


    ################ SemanticTypesColumns ################

    def semantic_types_columns_get(self, type_id, args):
        return


    def semantic_types_columns_post(self, type_id, args):
        return


    def semantic_types_columns_delete(self, type_id, args):
        return


    ################ SemanticTypesColumnData ################

    def semantic_types_column_data_get(self, type_id, column_id, args):
        return


    def semantic_types_column_data_post(self, type_id, column_id, args):
        return


    def semantic_types_column_data_put(self, type_id, column_id, args):
        return


    def semantic_types_column_data_delete(self, type_id, column_id, args):
        return


    ################ Models ################

    def models_get(self, args):
        return


    def models_post(self, args):
        return


    def models_delete(self, args):
        return


    ################ ModelData ################

    def model_data_get(self, model_id, args):
        return


    def model_data_post(self, model_id, args):
        return
