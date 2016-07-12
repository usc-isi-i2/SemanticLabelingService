from pymongo import MongoClient


# from semantic_labeling.run_experiments import SemanticLabeler

class Server(object):
    def __init__(self):
        # semantic_labeler = SemanticLabeler()
        db = MongoClient().data


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


    ################ Predict ################

    def predict_get(self, args):
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
