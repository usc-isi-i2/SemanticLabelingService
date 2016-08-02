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
        """
        Creates a semantic type and returns the id if it was successful.

        Notes: If the type already exists and force is not set to true a 409 will be returned and no data will be modified

        :param class_:    The class of the semantic type, note that this must be a valid URL
        :param property_: The property of the semantic type
        :param force:     Force create the semantic type, if this is true and the type already exists the existing type (and all of its columns and data) will be deleted before creation
        :return: The id of the new semantic type and a response code of 201 if the creation was successful, otherwise it will be an error message with the appropriate error code
        """

        class_ = class_.rstrip("/")
        property_ = property_.rstrip("/")

        ## Verify that class is a valid uri and namespace is a valid uri
        namespace = "/".join(class_.split("/")[:-1])
        if not validators.url(class_) or not validators.url(namespace): return "Invalid class URI was given", 400

        ## Actually add the type
        type_id = get_type_id(class_, property_)
        db_body = {ID: type_id, DATA_TYPE: DATA_TYPE_SEMANTIC_TYPE, CLASS: class_, PROPERTY: property_, NAMESPACE: namespace}
        if self.db.find_one(db_body):
            if force:
                # Remove the columns from the semantic labeler
                found_columns = list(self.db.find({DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id}))
                for col in found_columns:
                    Attribute(col[COLUMN_NAME], col[SOURCE_NAME]).delete(INDEX_NAME)
                # Remove the columns from the db
                self.db.delete_many({DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id})
                self.db.delete_many(db_body)
            else:
                return "Semantic type already exists", 409
        self.db.insert_one(db_body)
        return type_id, 201


    def _create_column(self, type_id, column_name, source_name, model, data=[], force=False):
        """
        Create a column in a semantic type and return the column's id if it was created successfully.

        Notes: If the column already exists and force is not set to true, a 409 will be returned and no data will be modified.

        :param type_id:     Id of the semantic type this column belongs to
        :param column_name: Name of the column to be created
        :param source_name: Name of the source of the column to be created
        :param model:       Model of the column to be created
        :param data:        Data which will be added to the column on creation
        :param force:       Force create the column, if this is true and the column exists the old column will be deleted (with all of its data) before creation
        :return: The id of the new column and a response code of 201 if the creation was successful, otherwise it will be an error message with the appropriate error code
        """

        column_id = get_column_id(type_id, column_name, source_name, model)
        db_body = {ID: column_id, DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id, COLUMN_NAME: column_name, SOURCE_NAME: source_name, MODEL: model}
        if self.db.find_one(db_body):
            if force:
                found_columns = list(self.db.find({DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id}))
                for col in found_columns:
                    Attribute(col[COLUMN_NAME], col[SOURCE_NAME]).delete(INDEX_NAME)
                self.db.delete_many(db_body)
            else:
                return "Column already exists", 409
        db_body[DATA] = data
        self.db.insert_one(db_body)
        return column_id, 201


    def _add_data_to_column(self, column_id, body, replace=False):
        """
        Add or replace data on an existing column

        Notes: If the column does not exist a 404 will be returned

        :param column_id: Id of the column to add/replace the data of
        :param body:      An array of the new data
        :param replace:   True if the current data in the column should be replaced, false if the new data should just be appended
        :return: A conformation with a 201 if it was added successfully or an error message with an appropriate error code if it was not successful
        """

        result = self.db.update_many({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id}, {"$set" if replace else "$pushAll": {DATA: body}})
        if result.matched_count < 1: return "No column with that id was found", 404
        if result.matched_count > 1: return "More than one column was found with that id", 500

        column = self.db.find_one({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id})
        att = Attribute(column[COLUMN_NAME], column[SOURCE_NAME], get_type_from_column_id(column_id))
        if replace: att.delete(INDEX_NAME)
        for value in body:
            att.add_value(value)
        att.update(INDEX_NAME)

        return "Column data updated", 201


    def _predict_column(self, column_name, source_names, data):
        """
        Predicts the semantic type of a column.

        :param column_name:  Name of the column
        :param source_names: List of source names
        :param data:         The data to predict based opon
        :return: A list of dictionaries which each contain the semantic type and confidence score
        """
        att = Attribute(column_name, source_names[0])
        for value in data:
            att.add_value(value)
        return att.predict_type(INDEX_NAME, source_names, Searcher.search_columns_data(INDEX_NAME, source_names), self.classifier, CONFIDENCE)


    def _update_bulk_add_model(self, model, column_model):
        """
        Updates the bulk add model in the db and also returns it

        :param model:        The current bulk add model
        :param column_model: The model of the columns which are being updated against
        :return: The updated bulk add model
        """
        for n in model[BAC_GRAPH][BAC_NODES]:
            if n.get(BAC_COLUMN_NAME):
                if n[BAC_COLUMN_NAME] == BAC_COLUMN_NAME_FILE_NAME:
                    continue
                column_id = get_column_id(get_type_id(n[BAC_USER_SEMANTIC_TYPES][0][BAC_CLASS][BAC_URI], n[BAC_USER_SEMANTIC_TYPES][0][BAC_PROPERTY][BAC_URI]), n[BAC_COLUMN_NAME], model[BAC_NAME], column_model)
                prediction = self._predict_column(n[BAC_COLUMN_NAME], [model[BAC_NAME]], self.db.find_one({DATA_TYPE: DATA_TYPE_COLUMN, ID: column_id})[DATA])
                n[BAC_LEARNED_SEMANTIC_TYPES] = []
                for t in prediction:
                    type_info = decode_type_id(t[SL_SEMANTIC_TYPE])
                    od = collections.OrderedDict()
                    od[BAC_CLASS] = {BAC_URI: type_info[0]}
                    od[BAC_PROPERTY] = {BAC_URI: type_info[1]}
                    od[BAC_CONFIDENCE_SCORE] = t[SL_CONFIDENCE_SCORE]
                    n[BAC_LEARNED_SEMANTIC_TYPES].append(od)
        self.db.update_one({DATA_TYPE: DATA_TYPE_MODEL, ID: model[BAC_ID]}, {"$set": {BULK_ADD_MODEL_DATA: model}})
        return model

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
        if column_names is None: column_names = [DEFAULT_NAME]
        if source_names is None:
            # If no source names are given just use all of the source names in the db
            source_names = set()
            for col in self.db.find({DATA_TYPE: DATA_TYPE_COLUMN}):
                source_names.add(col[SOURCE_NAME])
            source_names = list(source_names)
        if len(source_names) < 1: return "You must have columns to be able to predict", 400

        #### Predict the types
        ## Do the actual predicting using the semantic labeler
        prediction = self._predict_column(column_names[0], source_names, body.split("\n"))
        if len(prediction) < 1: return "No matches found", 404

        ## Filter the results
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
                allowed_ids_models.add(c[TYPE_ID])
        if allowed_ids_namespaces is not None and allowed_ids_models is not None: all_allowed_ids = allowed_ids_namespaces & allowed_ids_models
        elif allowed_ids_namespaces is not None and allowed_ids_models is None: all_allowed_ids = allowed_ids_namespaces
        elif allowed_ids_namespaces is None and allowed_ids_models is not None: all_allowed_ids = allowed_ids_models
        return_body = []
        for t in prediction:
            # Construct the new return body
            if all_allowed_ids is not None:
                if t[SL_SEMANTIC_TYPE] not in all_allowed_ids:
                    continue
            o = collections.OrderedDict()
            o[TYPE_ID_PATH] = t[SL_SEMANTIC_TYPE]
            o[SCORE] = t[SL_CONFIDENCE_SCORE]
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
                other_possible_ids.add(col[TYPE_ID])
            possible_type_ids = possible_type_ids & other_possible_ids

        # Construct the return body
        return_body = []
        for t in possible_result:
            if t[ID] in possible_type_ids:
                o = collections.OrderedDict()
                o[TYPE_ID_PATH] = t[ID]
                o[CLASS] = t[CLASS]
                o[PROPERTY] = t[PROPERTY]
                o[NAMESPACE] = t[NAMESPACE]
                return_body.append(o)

        # Add the column data if requested
        if return_columns:
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN}
            for type_ in return_body:
                db_body[TYPE_ID] = type_[TYPE_ID_PATH]
                type_[COLUMNS] = clean_columns_output(self.db.find(db_body), return_column_data)

        if len(return_body) < 1: return "No Semantic types matching the given parameters were found", 404
        return json_response(return_body, 200)


    def semantic_types_post_put(self, args, force):
        #### Assert args are valid
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if class_ is None or property_ is None: return "Both 'class' and 'property' must be specified", 400

        #### Add the type
        return self._create_semantic_type(class_, property_, force)


    def semantic_types_delete(self, args):
        #### Assert args are valid
        if len(args) < 1: return "At least one argument needs to be provided", 400
        args = args.copy()
        class_ = args.pop(CLASS, None)
        property_ = args.pop(PROPERTY, None)
        type_ids = args.pop(TYPE_IDS).split(",") if args.get(TYPE_IDS) else None
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
        if type_ids is not None: db_body[TYPE_IDS] = {"$in": type_ids}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[COLUMN_ID_PATH] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        for col in self.db.find(db_body):
            if col[TYPE_ID] not in type_ids_to_delete:
                type_ids_to_delete.append(col[TYPE_ID])

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
            db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: {"$in": type_ids_to_delete}}
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
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        result = list(self.db.find(db_body))
        if len(result) < 1: return "No columns matching the given parameters were found", 404
        return json_response(clean_columns_output(result, return_column_data), 200)


    def semantic_types_columns_post_put(self, type_id, args, body, force):
        #### Assert args are valid
        if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
        args = args.copy()
        column_name = args.pop(COLUMN_NAME, None)
        source_name = args.pop(SOURCE_NAME, None)
        model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_name is None or source_name is None: return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
        if model is None: model = DEFAULT_MODEL

        #### Add the column
        body_useful = body is not None and body.strip() != "" and body.strip() != "{}"
        if body_useful: body = body.split("\n")
        result = self._create_column(type_id, column_name, source_name, model, body if body_useful else [], force)
        if result[1] == 201 and body_useful:
            att = Attribute(column_name, source_name, type_id)
            for value in body:
                att.add_value(value)
            att.save(INDEX_NAME)
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
        db_body = {DATA_TYPE: DATA_TYPE_COLUMN, TYPE_ID: type_id}
        if source_names is not None: db_body[SOURCE_NAME] = {"$in": source_names}
        if column_names is not None: db_body[COLUMN_NAME] = {"$in": column_names}
        if column_ids is not None: db_body[ID] = {"$in": column_ids}
        if models is not None: db_body[MODEL] = {"$in": models}
        found_columns = list(self.db.find(db_body))
        if len(found_columns) < 1: return "No columns were found with the given parameters", 404
        for col in found_columns:
            Attribute(col[COLUMN_NAME], col[SOURCE_NAME]).delete(INDEX_NAME)

        return str(self.db.delete_many(db_body).deleted_count) + " columns deleted successfully", 200


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


    def semantic_types_column_data_post_put(self, column_id, args, body, force):
        #### Assert args are valid
        if body is None or body == "": return "Invalid message body", 400
        if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
        if len(args) > 0: return "Invalid arguments, there should be none", 400

        #### Add the data
        return self._add_data_to_column(column_id, body.split("\n"), force)


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
        crunch_data = args.pop(DO_NOT_CRUNCH_DATA_NOW, None)
        if len(args) > 0: return json_response("The following query parameters are invalid:  " + str(args.keys()), 400)
        show_all = True if show_all is not None and show_all.lower() == "true" else False
        crunch_data = False if crunch_data is not None and crunch_data.lower() == "false" else True

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
            if show_all: o[MODEL] = self._update_bulk_add_model(mod[BULK_ADD_MODEL_DATA], mod[MODEL]) if crunch_data else mod[BULK_ADD_MODEL_DATA]
            return_body.append(o)
        return json_response(return_body, 200)


    def bulk_add_models_post(self, args, body):
        #### Assert args are valid
        if body is None or len(body) < 1: return "Invalid message body", 400
        args = args.copy()
        column_model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_model is None: column_model = DEFAULT_BULK_MODEL

        #### Assert the required elements exist
        model = json.loads(body)
        if BAC_ID not in model: return "The given model must have an id", 400
        if BAC_NAME not in model: return "The given model must have a name", 400
        if BAC_DESC not in model: return "The given model must have a description", 400
        if BAC_GRAPH not in model: return "The given model must have a graph", 400
        if BAC_NODES not in model[BAC_GRAPH]: return "The given model must have nodes within the graph", 400
        if len(list(self.db.find({ID: model[BAC_ID]}))) > 0: return "Model id already exists", 409

        #### Parse and add the model
        # Try to add of the given semantic types and columns
        new_type_count = 0
        new_column_count = 0
        existed_type_count = 0
        existed_column_count = 0
        for n in model[BAC_GRAPH][BAC_NODES]:
            if n.get(BAC_USER_SEMANTIC_TYPES):
                for ust in n[BAC_USER_SEMANTIC_TYPES]:
                    semantic_status = self._create_semantic_type(ust[BAC_CLASS][BAC_URI], ust[BAC_PROPERTY][BAC_URI])
                    if semantic_status[1] == 201: new_type_count += 1
                    elif semantic_status[1] == 409: existed_type_count += 1
                    elif semantic_status[1] == 400: return semantic_status
                    else: return "Error occurred while adding semantic type: " + str(ust), 500
                    column_status = self._create_column(get_type_id(ust[BAC_CLASS][BAC_URI], ust[BAC_PROPERTY][BAC_URI]), n[BAC_COLUMN_NAME], model[BAC_NAME], column_model)
                    if column_status[1] == 201: new_column_count += 1
                    elif column_status[1] == 409: existed_column_count += 1
                    elif column_status[1] == 400: return column_status
                    else: return "Error occurred while adding column for semantic type: " + str(ust), 500

        # Nothing bad happened when creating the semantic types and columns, so add the model to the DB
        self.db.insert_one({DATA_TYPE: DATA_TYPE_MODEL, ID: model["id"], NAME: model[BAC_NAME], DESC: model["description"], MODEL: column_model, BULK_ADD_MODEL_DATA: model})
        return "Model and columns added, " + str(new_type_count) + " semantic types created, " + \
               str(existed_type_count) + " semantic types already existed, " + \
               str(new_column_count) + " columns created, and " + \
               str(existed_column_count) + " columns already existed.", 201


    def bulk_add_models_delete(self, args):
        #### Assert args are valid
        args = args.copy()
        model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
        model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
        model_desc = args.pop(MODEL_DESC, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400

        #### Delete the model
        db_body = {DATA_TYPE: DATA_TYPE_MODEL}
        if model_ids is not None: db_body[ID] = {"$in": model_ids}
        if model_names is not None: db_body[NAME] = {"$in": model_names}
        if model_desc is not None: db_body[MODEL_DESC] = model_desc
        deleted_count = self.db.delete_many(db_body).deleted_count

        if deleted_count < 1: return "No models were found with the given parameters", 404
        return str(deleted_count) + " models deleted successfully", 200


    ################ BulkAddModelData ################

    def bulk_add_model_data_get(self, model_id, args):
        if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
        crunch_data = args.pop(DO_NOT_CRUNCH_DATA_NOW, None)
        if len(args) > 0: return json_response("The following query parameters are invalid:  " + str(args.keys()), 400)
        crunch_data = False if crunch_data is not None and crunch_data.lower() == "false" else True

        #### Get the model
        db_result = list(self.db.find({DATA_TYPE: DATA_TYPE_MODEL, ID: model_id}))
        if len(db_result) < 1: return "A model was not found with the given id", 404
        if len(db_result) > 1: return "More than one model was found with the given id", 500
        db_result = db_result[0]
        return json_response(self._update_bulk_add_model(db_result[BULK_ADD_MODEL_DATA], db_result[MODEL]) if crunch_data else db_result[BULK_ADD_MODEL_DATA], 200)


    def bulk_add_model_data_post(self, model_id, args, body):
        if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
        if body is None or len(body) < 1: return "Invalid message body", 400
        args = args.copy()
        column_model = args.pop(MODEL, None)
        if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
        if column_model is None: column_model = DEFAULT_BULK_MODEL

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
        for n in model[BAC_GRAPH][BAC_NODES]:
            column_data = []
            for line in data:
                if n.get(BAC_COLUMN_NAME):
                    column_data.append(line[n[BAC_COLUMN_NAME]])
            # Add it to the db
            if n.get(BAC_USER_SEMANTIC_TYPES):
                for ust in n[BAC_USER_SEMANTIC_TYPES]:
                    result = self._add_data_to_column(get_column_id(get_type_id(ust[BAC_CLASS][BAC_URI], ust[BAC_PROPERTY][BAC_URI]), n[BAC_COLUMN_NAME], model[BAC_NAME], column_model), column_data)[1]
                    if result == 201: continue
                    elif result == 404: return "A required column was not found", 404
                    else: return "Error occurred while adding data to the column", 500

        return "Data successfully added to columns", 201
