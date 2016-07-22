import validators
from pymongo import MongoClient

from service import *


class Server(object):
    def __init__(self):
        self.db = MongoClient().data.posts


    ################ Stuff for use in this file ################

    def _create_semantic_type(self, class_, property_, force=False):
        class_ = class_.rstrip("/")
        property_ = property_.rstrip("/")

        # Verify that class is a valid uri and namespace is a valid uri
        namespace = "/".join(class_.split("/")[:-1])
        if not validators.url(class_) or not validators.url(namespace):
            return "Invalid class URI was given", 400

        # Actually add the type
        type_id = get_type_id(class_, property_)
        db_body = {ID: type_id, DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, CLASS: class_, PROPERTY: property_, NAMESPACE: namespace}
        if force:
            self.db.delete_many({DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id})
            self.db.delete_many(db_body)
        else:
            if self.db.find_one(db_body):
                return "Semantic type already exists", 409
        self.db.insert_one(db_body)
        return type_id, 201


    def _create_column(self, type_id, column_name, source_name, model, data=[], force=False):
        column_id = get_column_id(type_id, column_name, source_name, model)
        db_body = {ID: column_id, DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id, COLUMN_NAME: column_name, SOURCE_NAME: source_name, MODEL: model, DATA: data}
        if force:
            self.db.delete_many(db_body)
        else:
            if self.db.find_one(db_body):
                return "Column already exists", 409
        self.db.insert_one(db_body)
        return column_id, 201


    def _add_data_to_column(self, column_id, body, replace=False):
        result = self.db.update_many({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}, {"$set" if replace else "$pushAll": {DATA: body.split("\n")}})
        if result.matched_count < 1: return "No column with that id was found", 404
        if result.matched_count > 1: return "More than one column was found with that id", 500
        return "Column data updated", 201


    ################ Predict ################

    def predict_post(self, args, body):
        #### Assert args and body are valid
        if body is None or body == "":
            return "Invalid message body", 400
        args = args.copy()
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        col_name = args.pop(COLUMN_NAME, None)
        model = args.pop(MODEL, None)
        source_col = args.pop(SOURCE_NAME, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Predict the types
        return "Method partially implemented", 601


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
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False
        return_columns = True if return_columns is not None and return_columns.lower() == "true" else return_column_data

        #### Get the types
        # Find all of the type ids that satisfy the class, property, and namespaces
        db_body = {DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE}
        if class_ is not None: db_body[CLASS] = class_
        if property_ is not None: db_body[PROPERTY] = property_
        if namespaces is not None: db_body[NAMESPACE] = {"$in": namespaces}
        possible_result = list(self.db.find(db_body))
        possible_type_ids = set()
        for t in possible_result:
            possible_type_ids.add(t[ID])

        # Find all of the type ids from the columns which satisfy the other parameters
        if source_names or column_names or column_ids or models:
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN}
            if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
            if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
            if column_ids is not None: db_body[ID] = {"$in": column_ids}
            if models is not None: db_body[MODEL] = {"$in": models}
            other_possible_ids = set()
            for col in self.db.find(db_body):
                other_possible_ids.add(col[TYPEID])
            possible_type_ids = possible_type_ids & other_possible_ids

        # Construct the return body
        return_body = []
        for t in possible_result:
            if t[ID] in possible_type_ids:
                o = collections.OrderedDict()
                o[TYPE_ID] = t[ID]
                o[CLASS] = t[CLASS]
                o[PROPERTY] = t[PROPERTY]
                o[NAMESPACE] = t[NAMESPACE]
                return_body.append(o)

        # Add the column data if requested
        if return_columns:
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN}
            for type_ in return_body:
                db_body[TYPEID] = type_[TYPE_ID]
                type_[COLUMNS] = clean_columns_output(self.db.find(db_body), return_column_data)

        return json_response(return_body, 200)


    def semantic_types_post(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        if class_ is None or property_ is None:
            return "Both 'class' and 'property' must be specified", 400

        #### Add the type
        return self._create_semantic_type(class_, property_)


    def semantic_types_put(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        if class_ is None or property_ is None:
            return "Both 'class' and 'property' must be specified", 400

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
            return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the types
        if delete_all and delete_all.lower() == "true":
            self.db.delete_many({DATA_TYPE: {"$in": [DATA_TYPE_SEMANTIC_TYPE, DATA_TYPE_COLUMN]}})
            return "All semantic types and their data was deleted", 204

        # Find the parent semantic types and everything below them of everything which meets column requirements
        type_ids_to_delete = []
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[COLUMN_ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        for col in self.db.find(db_body):
            if col[TYPEID] not in type_ids_to_delete:
                type_ids_to_delete.append(col[TYPEID])

        # Find the semantic types which meet the other requirements and delete all types which need to be
        possible_types = []
        db_body = {DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE}
        if class_ is not None: db_body[CLASS] = class_
        if property_ is not None: db_body[PROPERTY] = property_
        if namespaces is not None: db_body[NAMESPACE] = {"$in": namespaces}
        if source_names is None and column_names is None and column_ids is None and models is None:
            self.db.delete_many(db_body)
        else:
            for t in self.db.find(db_body):
                if t[ID] not in possible_types:
                    possible_types.append(t[ID])
            for id_ in type_ids_to_delete:
                if id_ not in possible_types:
                    type_ids_to_delete.remove(id_)
            self.db.delete_many({DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: {"$in": type_ids_to_delete}})
            self.db.delete_many({DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, ID: {"$in": type_ids_to_delete}})

        return "All semantic types which matched the parameters were deleted", 204


    ################ SemanticTypesColumns ################

    def semantic_types_columns_get(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return "Invalid type_id", 400
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False

        #### Get the columns
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        return json_response(clean_columns_output(self.db.find(db_body), return_column_data), 200)


    def semantic_types_columns_post(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return "Invalid type_id", 400
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_name is None or source_name is None:
            return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
        if model is None:
            model = "default"

        #### Add the column
        return json_response(
            self._create_column(type_id, column_name, source_name, model, body.split("\n")) if body is not None and body.strip() != "" else self._create_column(type_id, column_name, source_name, model), 201)


    def semantic_types_columns_put(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return "Invalid type_id", 400
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_name is None or source_name is None:
            return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
        if model is None:
            model = "default"

        #### Add the column
        return json_response(
            self._create_column(type_id, column_name, source_name, model, body.split("\n"), True) if body is not None and body.strip() != "" else self._create_column(type_id, column_name, source_name, model,
                                                                                                                                                                      force=True), 201)


    def semantic_types_columns_delete(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1:
            return "Invalid type_id", 400
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the columns
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        self.db.delete_many(db_body)

        return "Columns deleted successfully", 204


    ################ SemanticTypesColumnData ################

    def semantic_types_column_data_get(self, column_id, args):
        #### Assert args are valid
        if column_id is None or len(column_id) < 1:
            return "Invalid column_id", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Get the column
        result = list(self.db.find({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}))
        if len(result) < 1: return "No column with that id was found", 404
        if len(result) > 1: return "More than one column was found with that id", 500
        return json_response(clean_column_output(result[0]), 200)


    def semantic_types_column_data_post(self, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return "Invalid message body", 400
        if column_id is None or len(column_id) < 1:
            return "Invalid column_id", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Add the data
        return self._add_data_to_column(column_id, body)


    def semantic_types_column_data_put(self, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "":
            return "Invalid message body", 400
        if column_id is None or len(column_id) < 1:
            return "Invalid column_id", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Replace the data
        return self._add_data_to_column(column_id, body, True)


    def semantic_types_column_data_delete(self, column_id, args):
        #### Assert args are valid
        if column_id is None or len(column_id) < 1:
            return "Invalid column_id", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Delete the data
        result = self.db.update_many({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}, {"$set": {DATA: []}})
        if result.matched_count < 1: return "No column with that id was found", 404
        if result.matched_count > 1: return "More than one column was found with that id", 500
        return "Column data deleted", 204


    ################ Models ################

    def models_get(self, args):
        #### Assert args are valid
        args = args.copy()
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        show_all = args.pop(SHOW_ALL, None)
        if len(args) > 0:
            return json_response("The following query parameters are invalid:  " + str(args.keys()), 400)
        show_all = True if show_all is not None and show_all.lower() == "true" else False

        #### Find the model
        return "Method partially implemented", 601


    def models_post(self, args, body):
        #### Assert args are valid
        if body is None or len(body) < 1:
            return "Invalid message body", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Add the model

        return "Method partially implemented", 601


    def models_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        if len(args) > 0:
            return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Find the model
        return "Method partially implemented", 601


    ################ ModelData ################

    def model_data_get(self, model_id, args):
        if model_id is None or len(model_id) < 1:
            return "Invalid model_id", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Get the model
        return "Method partially implemented", 601


    def model_data_post(self, model_id, args, body):
        if model_id is None or len(model_id) < 1:
            return "Invalid model_id", 400
        if body is None or len(body) < 1:
            return "Invalid message body", 400
        if len(args) > 0:
            return "Invalid arguments, there should be none", 400

        #### Process the data
        return "Method partially implemented", 601
