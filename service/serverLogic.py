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
SOURCE_COLUMN = "sourceColumn"
MODEL = "model"

INVALID_PARAMETERS = "Invalid query parameters"
INVALID_BODY = "Invalid body"


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

    def predict_get(self, args, body):
        if body is None or body == "":
            return message(INVALID_BODY, 400)

        args = args.copy()
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        col_name = args.pop(COLUMN_NAME, None)
        model = args.pop(MODEL, None)
        source_col = args.pop(SOURCE_COLUMN, None)
        if len(args) > 0:
            return message(INVALID_PARAMETERS, 400)

        return message("Method partially implemented", 601)


    ################ SemanticTypes ################

    def semantic_types_get(self, args):
        return


    def semantic_types_post(self, args):
        return


    def semantic_types_delete(self, args):
        return


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
