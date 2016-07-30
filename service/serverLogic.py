import validators
from pymongo import MongoClient

from data_source.data_source import Attribute
from machine_learning.classifier import Classifier
from search_engine.searcher import Searcher
from service import *


class Server(object):
    def __init__(self):
        self.db = MongoClient().data.posts
        self.classifier = Classifier(None)
        self.classifier.load(DATA_MODEL_PATH)


    ################ Stuff for use in this file ################

    def _create_semantic_type(self, class_, property_, force=False):
        class_ = class_.rstrip("/")
        property_ = property_.rstrip("/")

        # Verify that class is a valid uri and namespace is a valid uri
        namespace = "/".join(class_.split("/")[:-1])
        if not validators.url(class_) or not validators.url(namespace): return "Invalid class URI was given", 400

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
        db_body = get_column_create_db_body(column_id, type_id, column_name, source_name, model)
        if force:
            self.db.delete_many(db_body)
        else:
            if self.db.find_one(db_body):
                return "Column already exists", 409
        db_body[DATA] = data
        self.db.insert_one(db_body)
        return column_id, 201


    def _add_data_to_column(self, column_id, body, replace=False):
        result = self.db.update_many({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}, {"$set" if replace else "$pushAll": {DATA: body}})
        if result.matched_count < 1: return "No column with that id was found", 404
        if result.matched_count > 1: return "More than one column was found with that id", 500

        column = self.db.find_one({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id})
        att = Attribute(column[COLUMN_NAME], column[SOURCE_NAME], get_type_from_column_id(column_id))
        for value in body:
            att.add_value(value)
        att.update(INDEX_NAME)

        return "Column data updated", 201


    ################ Predict ################

    def predict_post(self, args, body):
        #### Assert args and body are valid
        if body is None or body == "": return "Invalid message body", 400
        args = args.copy()
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        column_names = args.pop(COLUMN_NAME).split(",") if args.get(COLUMN_NAME) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        models = args.pop(MODEL).split(",") if args.get(MODEL) else None
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_names is None: column_names = ["default"]
        if source_names is None:
            source_names = set()
            for col in self.db.find({DATA_TYPE: DATA_TYPE_COLUMN}):
                source_names.add(col[SOURCE_NAME])
            source_names = list(source_names)

        #### Predict the types
        att = Attribute(column_names[0], source_names[0])
        for value in body.split("\n"):
            att.add_value(value)
        prediction = att.predict_type(INDEX_NAME, source_names, Searcher.search_columns_data(INDEX_NAME, source_names), self.classifier, CONFIDENCE)
        if len(prediction) < 1: return "No matches found", 404

        allowed_ids_namespaces = None
        allowed_ids_models = None
        all_allowed_ids = None
        if namespaces is not None:
            allowed_ids_namespaces = set()
            current_allowed_types = list(self.db.find({DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, NAMESPACE: {"$in": namespaces}}))
            for t in current_allowed_types:
                allowed_ids_namespaces.add(t[ID])
        if models:
            allowed_ids_models = set()
            current_allowed_types = list(self.db.find({DATA_TYPE: DATA_TYPE_COLUMN, MODEL: {"$in": models}}))
            for c in current_allowed_types:
                allowed_ids_models.add(c[TYPEID])
        if allowed_ids_namespaces is not None and allowed_ids_models is not None: all_allowed_ids = allowed_ids_namespaces & allowed_ids_models
        elif allowed_ids_namespaces is not None and allowed_ids_models is None: all_allowed_ids = allowed_ids_namespaces
        elif allowed_ids_namespaces is None and allowed_ids_models is not None: all_allowed_ids = allowed_ids_models
        return_body = []
        for t in prediction:
            if all_allowed_ids is not None:
                if t["semantic_type"] not in all_allowed_ids:
                    continue
            o = collections.OrderedDict()
            o[TYPE_ID] = t["semantic_type"]
            o[SCORE] = t["prob"]
            return_body.append(o)
        return json_response(return_body, 200)


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
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
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

        if len(return_body) < 1: return "No Semantic types matching the given parameters were found", 404
        return json_response(return_body, 200)


    def semantic_types_post(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if class_ is None or property_ is None: return "Both 'class' and 'property' must be specified", 400

        #### Add the type
        return self._create_semantic_type(class_, property_)


    def semantic_types_put(self, args):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if class_ is None or property_ is None: return "Both 'class' and 'property' must be specified", 400

        #### Add the type
        return self._create_semantic_type(class_, property_, True)


    def semantic_types_delete(self, args):
        #### Assert args are valid
        if len(args) < 1: return "At least one argument needs to be provided", 400
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        delete_all = args.pop(DELETE_ALL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the types
        if delete_all and delete_all.lower() == "true":
            return "All " + str(self.db.delete_many({DATA_TYPE: {"$in": [DATA_TYPE_SEMANTIC_TYPE, DATA_TYPE_COLUMN]}}).deleted_count) + " semantic types and their data were deleted", 200

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
        if source_names is None and column_names is None and column_ids is None and models is None: deleted = self.db.delete_many(db_body).deleted_count
        else:
            for t in self.db.find(db_body):
                if t[ID] not in possible_types:
                    possible_types.append(t[ID])
            for id_ in type_ids_to_delete:
                if id_ not in possible_types:
                    type_ids_to_delete.remove(id_)
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: {"$in": type_ids_to_delete}}
            found_columns = list(self.db.find(db_body))
            for col in found_columns:
                Attribute(col[COLUMN_NAME], col[SOURCE_NAME]).delete(INDEX_NAME)
            self.db.delete_many(db_body)
            deleted = self.db.delete_many({DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, ID: {"$in": type_ids_to_delete}}).deleted_count

        return str(deleted) + " semantic types matched parameters and were deleted", 200


    ################ SemanticTypesColumns ################

    def semantic_types_columns_get(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        return_column_data = args.pop(RETURN_COLUMN_DATA, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False

        #### Get the columns
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        result = self.db.find(db_body)
        if len(list(result)) < 1: return "No columns matching the given parameters were found", 404
        return json_response(clean_columns_output(result, return_column_data), 200)


    def semantic_types_columns_post(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_name is None or source_name is None: return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
        if model is None: model = "default"

        #### Add the column
        body_useful = body is not None and body.strip() != "" and body.strip() != "{}"
        if body_useful: body = body.split("\n")
        result = self._create_column(type_id, column_name, source_name, model, body if body_useful else [])
        if result[1] == 201 and body_useful:
            att = Attribute(column_name, source_name, type_id)
            for value in body:
                att.add_value(value)
            att.save(INDEX_NAME)
        return result


    def semantic_types_columns_put(self, type_id, args, body):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_name is None or source_name is None: return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
        if model is None: model = "default"

        #### Add/Replace the column
        body_useful = body is not None and body.strip() != "" and body.strip() != "{}"
        if body_useful: body = body.split("\n")
        result = self._create_column(type_id, column_name, source_name, model, body if body_useful else [], True)
        if result[1] == 201 and body_useful:
            att = Attribute(column_name, source_name, type_id)
            for value in body:
                att.add_value(value)
            att.update(INDEX_NAME)
        return result


    def semantic_types_columns_delete(self, type_id, args):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
        args = args.copy()
        column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
        source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
        column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
        models = args.pop(MODELS).split(",") if args.get(MODELS) else None
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the columns
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPEID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        found_columns = list(self.db.find(db_body))
        if len(found_columns) < 1: return "No columns were found with the given parameters", 404
        for col in found_columns:
            Attribute(col[COLUMN_NAME], col[SOURCE_NAME]).delete(INDEX_NAME)
        self.db.delete_many(db_body)

        # FIXME: this is returning incorrect number of types deleted
        return str(len(found_columns)) + " columns deleted successfully", 200


    ################ SemanticTypesColumnData ################

    def semantic_types_column_data_get(self, column_id, args):
        #### Assert args are valid
        if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Get the column
        result = list(self.db.find({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}))
        if len(result) < 1: return "No column with that id was found", 404
        if len(result) > 1: return "More than one column was found with that id", 500
        return json_response(clean_column_output(result[0]), 200)


    def semantic_types_column_data_post(self, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "": return "Invalid message body", 400
        if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Add the data
        return self._add_data_to_column(column_id, body.split("\n"))


    def semantic_types_column_data_put(self, column_id, args, body):
        #### Assert args are valid
        if body is None or body == "": return "Invalid message body", 400
        if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Replace the data
        values = body.split("\n")
        result = self._add_data_to_column(column_id, values, True)
        if result[1] == 201:
            column = self.db.find_one({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id})
            att = Attribute(column[COLUMN_NAME], column[SOURCE_NAME], get_type_from_column_id(column_id))
            for value in values:
                att.add_value(value)
            att.save(INDEX_NAME)
        return result


    def semantic_types_column_data_delete(self, column_id, args):
        #### Assert args are valid
        if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Delete the data
        result = self.db.update_many({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}, {"$set": {DATA: []}})
        if result.matched_count < 1: return "No column with that id was found", 404
        if result.matched_count > 1: return "More than one column was found with that id", 500
        column = self.db.find_one({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id})
        Attribute(column[COLUMN_NAME], column[SOURCE_NAME]).delete(INDEX_NAME)
        return "Column data deleted", 200


    ################ BulkAddModels ################

    def bulk_add_models_get(self, args):
        #### Assert args are valid
        args = args.copy()
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        show_all = args.pop(SHOW_ALL, None)
        if len(args) > 0: return json_response("The following query parameters are invalid:  " + str(args.keys()), 400)
        show_all = True if show_all is not None and show_all.lower() == "true" else False

        #### Find the model
        db_body = {DATA_TYPE: DATA_TYPE_MODEL}
        if model_ids is not None: db_body[ID] = {"$in": model_ids}
        if model_names is not None: db_body[NAME] = {"$in": model_names}
        if model_desc is not None: db_body[MODEL_DESC] = model_desc
        db_result = list(self.db.find(db_body))
        if len(db_result) < 1: return "No models were found with the given parameters", 404

        # Construct the return body
        return_body = []
        for mod in db_result:
            o = collections.OrderedDict()
            o[MODEL_ID] = mod[ID]
            o[NAME] = mod[NAME]
            o[DESC] = mod[DESC]
            if show_all:
                # TODO: possibly update the learned semantic types here
                o[MODEL] = mod[BULK_ADD_MODEL_DATA]
            return_body.append(o)
        return json_response(return_body, 601)


    def bulk_add_models_post(self, args, body):
        #### Assert args are valid
        if body is None or len(body) < 1: return "Invalid message body", 400
        column_model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_model is None: column_model = "bulk_add"

        #### Assert the required elements exist
        model = json.loads(body)
        if "id" not in model: return "The given model must have an id", 400
        if "name" not in model: return "The given model must have a name", 400
        if "description" not in model: return "The given model must have a description", 400
        if "graph" not in model: return "The given model must have a graph", 400
        if "nodes" not in model["graph"]: return "The given model must have nodes within the graph", 400
        if len(list(self.db.find({ID: model["id"]}))) > 0: return "Model id already exists", 409

        #### Parse and add the model
        # Try to add of the given semantic types and columns
        new_type_count = 0
        new_column_count = 0
        existed_type_count = 0
        existed_column_count = 0
        for n in model["graph"]["nodes"]:
            if n.get("userSemanticTypes"):
                for ust in n["userSemanticTypes"]:
                    semantic_status = self._create_semantic_type(ust["domain"]["uri"], ust["type"]["uri"])
                    if semantic_status[1] == 201: new_type_count += 1
                    elif semantic_status[1] == 409: existed_type_count += 1
                    elif semantic_status[1] == 400: return semantic_status
                    else: return "Error occurred while adding semantic type: " + str(ust), 500
                    column_status = self._create_column(get_type_id(ust["domain"]["uri"], ust["type"]["uri"]), n["columnName"], model["name"], column_model)
                    if column_status[1] == 201: new_column_count += 1
                    elif column_status[1] == 409: existed_column_count += 1
                    elif column_status[1] == 400: return column_status
                    else: return "Error occurred while adding column for semantic type: " + str(ust), 500

        # Nothing bad happened when creating the semantic types and columns, so add the model to the DB
        self.db.insert_one({DATA_TYPE: DATA_TYPE_MODEL, ID: model["id"], NAME: model["name"], DESC: model["description"], BULK_ADD_MODEL_DATA: model})
        return "Model and columns added, " + str(new_type_count) + " semantic types created, " + \
               str(existed_type_count) + " semantic types already existed, " + \
               str(new_column_count) + " columns created, and " + \
               str(existed_column_count) + " columns already existed.", 201


    def bulk_add_models_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        no_args = len(args) < 1
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the model
        db_body = {DATA_TYPE: DATA_TYPE_MODEL}
        if not no_args:
            if model_ids is not None: db_body[ID] = {"$in": model_ids}
            if model_names is not None: db_body[NAME] = {"$in": model_names}
            if model_desc is not None: db_body[MODEL_DESC] = model_desc
        deleted_count = self.db.delete_many(db_body).deleted_count

        if deleted_count < 1: return "No models were found with the given parameters", 404
        return str(deleted_count) + " models deleted successfully", 200


    ################ BulkAddModelData ################

    def bulk_add_model_data_get(self, model_id, args):
        if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Get the model
        db_result = list(self.db.find({DATA_TYPE: DATA_TYPE_MODEL, ID: model_id}))
        if len(db_result) < 1: return "A model was not found with the given id", 404
        if len(db_result) > 1: return "More than one model was found with the given id", 500
        db_result = db_result[0]
        # TODO: possibly update the learned semantic types here
        return json_response(db_result[BULK_ADD_MODEL_DATA], 601)


    def bulk_add_model_data_post(self, model_id, args, body):
        if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
        if body is None or len(body) < 1: return "Invalid message body", 400
        column_model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_model is None: column_model = "bulk_add"

        #### Process the data
        # Get the model and parse the json lines
        model = list(self.db.find({DATA_TYPE: DATA_TYPE_MODEL, ID: model_id}))
        if len(model) < 1: return "The given model was not found", 404
        if len(model) > 1: return "More than one model was found with the id", 500
        model = model[0][BULK_ADD_MODEL_DATA]
        data = []
        for line in body.split("\n"):
            if line.strip() != "":
                data.append(json.loads(line))
        # Get all of the data in each column
        for n in model["graph"]["nodes"]:
            column_data = []
            for line in data:
                if n.get("columnName"):
                    column_data.append(line[n["columnName"]])
            # Add it to the db
            if n.get("userSemanticTypes"):
                for ust in n["userSemanticTypes"]:
                    result = self._add_data_to_column(get_column_id(get_type_id(ust["domain"]["uri"], ust["type"]["uri"]), n["columnName"], model["name"], column_model), column_data)[1]
                    if result == 201: continue
                    elif result == 404: return "A required column was not found", 404
                    else: return "Error occurred while adding data to the column", 500

        return "Data successfully added to columns", 201
