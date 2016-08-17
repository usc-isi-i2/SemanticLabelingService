import traceback

from flask import Flask, request
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful_swagger import swagger

import service.serverLogic
from service import *

app = Flask(__name__, static_folder='../static')
api = swagger.docs(Api(app), apiVersion='0.2', basePath='/', resourcePath='/',
                   produces=["application/json", "text/html"], api_spec_url='/api/spec', description='Semantic Typing')
CORS(app)
service = service.serverLogic.Server()


################################################################################################################################
#                                                                                                                              #
#  This class is only used for what gets called by the API and swagger docs.  There isn't any logic code here aside from the   #
#  try/catch for returning 500's and verifying parameters since I think it would be messy and harder to maintain.  It's all    #
#  in service/serverLogic.py                                                                                                   #
#                                                                                                                              #
#  All of the constant values used here are set in service/__init__.py                                                         #
#                                                                                                                              #
#  Each of the API functions in this class should call a helper function in the Server class of serverLogic.py whose name      #
#  follows the form of {class_name}_{type}, where {class_name} is the name of the class in this file, but with underscores     #
#  instead of camelCase and {type} is the HTTP method name, such as GET or DELETE.  Example: semantic_types_get()              #
#  Note that POST and PUT call the same method since the code for each of them is nearly identical.  These follow the form:    #
#  {class_name}_post_put().  Example: semantic_types_post_put()                                                                #
#                                                                                                                              #
################################################################################################################################


class parameters(object):
    @staticmethod
    def type_id(required=False, multiple=True, param_type="query"):
        return {
            "name": TYPE_ID_PATH if param_type == "path" else TYPE_IDS if multiple else TYPE_ID,
            "description": "Ids of the semantic types" if multiple else "Id of the semantic type",
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
        }

    @staticmethod
    def class_(required=False):
        return {
            "name": CLASS,
            "description": "Uri of a class",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def property(required=False):
        return {
            "name": PROPERTY,
            "description": "Uri of a property",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def namespaces(required=False):
        return {
            "name": NAMESPACES,
            "description": "List of URIs of parent URIs of property which to consider",
            "required": required,
            "allowMultiple": True,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def source_names(required=False, desc="List of source names that the column(s) should have", multiple=True):
        return {
            "name": SOURCE_NAMES if multiple else SOURCE_NAME,
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def column_names(required=False, desc="List of column names which the semantic type(s) should have", multiple=True):
        return {
            "name": COLUMN_NAMES if multiple else COLUMN_NAME,
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def column_ids(required=False, desc="List of column ids which the semantic type(s) should have", multiple=True,
                   param_type="query"):
        return {
            "name": COLUMN_ID_PATH if param_type == "path" or multiple else COLUMN_IDS,
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
        }

    @staticmethod
    def models(required=False, desc="List of models which the column(s) should have", multiple=True):
        return {
            "name": MODELS if multiple else MODEL,
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def return_column_data(desc="If the data in the columns should be in the return body"):
        return {
            "name": RETURN_COLUMN_DATA,
            "description": desc,
            "required": False,
            "allowMultiple": False,
            "dataType": "boolean",
            "paramType": "query"
        }

    @staticmethod
    def body(required=False,
             desc="List of data values which will be inserted into the column (one per line), all lines will be included as values, including blank ones"):
        return {
            "name": BODY,
            "description": desc,
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "body"
        }

    @staticmethod
    def model_names(required=False):
        return {
            "name": MODEL_NAMES,
            "description": "Name of the models",
            "required": required,
            "allowMultiple": True,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def model(desc):
        return {
            "name": MODEL,
            "description": desc,
            "required": False,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def model_desc(required=False):
        return {
            "name": MODEL_DESC,
            "description": "Part or all of a model description",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }

    @staticmethod
    def model_id(required=False, multiple=True, param_type="query"):
        return {
            "name": MODEL_ID_PATH if param_type == "path" else MODEL_IDS if multiple else MODEL_ID,
            "description": "Id(s) of the model.json",
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
        }

    @staticmethod
    def crunch_data():
        return {
            "name": DO_NOT_CRUNCH_DATA_NOW,
            "description": "If this is true, the model will not be evaluated and whatever is currently stored will be used",
            "required": False,
            "allowMultiple": False,
            "dataType": "boolean",
            "paramType": "query"
        }


class responses(object):
    @staticmethod
    def standard_get():
        return [
            {"code": 200, "message": "Success"},
            {"code": 400, "message": "Bad Request"},
            {"code": 404, "message": "Not Found"},
            {"code": 500, "message": "Internal Server Error"}
        ]

    @staticmethod
    def standard_put():
        return [
            {"code": 201, "message": "Created"},
            {"code": 400, "message": "Bad Request"},
            {"code": 500, "message": "Internal Server Error"}
        ]

    @staticmethod
    def standard_post():
        return [
            {"code": 201, "message": "Created"},
            {"code": 400, "message": "Bad Request"},
            {"code": 409, "message": "Already exists"},
            {"code": 500, "message": "Internal Server Error"}
        ]

    @staticmethod
    def standard_delete():
        return [
            {"code": 200, "message": "Deleted"},
            {"code": 400, "message": "Bad Request"},
            {"code": 404, "message": "Not Found"},
            {"code": 500, "message": "Internal Server Error"}
        ]


class Predict(Resource):
    @swagger.operation(
        parameters=[
            parameters.namespaces(),
            parameters.column_names(False, "Name of the column the data may belong to", False),
            parameters.models(False, "The model the data may belong to", False),
            parameters.source_names(False, "Sources the data may belong to", True),
            parameters.body(True, "List of data values to predict")
        ],
        responseMessages=responses.standard_get()
    )
    def post(self):
        """
        Predict the semantic type of data
        Predicts the semantic type of the given data.  Returns a list of types in the following format (sorted from most to least likely):
        <pre>
        [
            {
                "type_id": "",
                "score":
            }
        ]
        </pre>
        """
        try:
            if request.data is None or request.data == "": return "Invalid message body", 400
            args = request.args.copy()
            namespaces = args.pop(NAMESPACES).split(",") if args.get(NAMESPACES) else None
            column_names = args.pop(COLUMN_NAME).split(",") if args.get(COLUMN_NAME) else None
            source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
            models = args.pop(MODEL).split(",") if args.get(MODEL) else None
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if column_names is None: column_names = [DEFAULT_NAME]
            return service.predict_post(request.data.split(","), namespaces, column_names, source_names, models)
        except:
            return str(traceback.format_exc()), 500


class SemanticTypes(Resource):
    @swagger.operation(
        parameters=[
            parameters.class_(),
            parameters.property(),
            parameters.namespaces(),
            parameters.source_names(),
            parameters.column_names(),
            parameters.column_ids(),
            parameters.models(),
            {
                "name": RETURN_COLUMNS,
                "description": "If the columns for the semantic type(s) should be in the return body",
                "required": False,
                "allowMultiple": False,
                "dataType": "boolean",
                "paramType": "query"
            },
            parameters.return_column_data(
                "If the data in the columns should be in the return body, if this is true it will override returnColumns")
        ],
        responseMessages=responses.standard_get()
    )
    def get(self):
        """
        Get semantic types
        Returns all of the semantic types which fit the given parameters.

        Returned body will have the following format, but if returnColumns or returnColumnData is not given as true, then "columns" will be omitted and if returnDataColumns is not given as true "data" will be omitted:
        <pre>
        [
            {
                "type_id": "",
                "class": "",
                "property": "",
                "namespace": "",
                "columns": [
                    {
                        "column_id": "",
                        "name": "",
                        "source": "",
                        "model": "",
                        "data": [
                            "",
                            "",
                            ""
                        ]
                    }
                ]
            }
        ]
        </pre>
        Note that giving no parameters will return all semantic types with no columns.
        """
        try:
            args = request.args.copy()
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
            return service.semantic_types_get(class_, property_, namespaces, source_names, column_names, column_ids,
                                              models, return_columns, return_column_data)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.class_(True),
            parameters.property(True)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self):
        """
        Create a semantic type
        Creates a semantic type and returns its id.
        """
        try:
            args = request.args.copy()
            class_ = args.pop(CLASS, None)
            property_ = args.pop(PROPERTY, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if class_ is None or property_ is None: return "Both 'class' and 'property' must be specified", 400
            return service.semantic_types_post_put(class_, property_, False)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.class_(True),
            parameters.property(True)
        ],
        responseMessages=responses.standard_put()
    )
    def put(self):
        """
        Create/Replace a semantic type
        Creates a semantic type if it doesn't exist or replaces it if it does, then returns its id.  Note that replacing a semantic type will remove all of it's columns.
        """
        try:
            args = request.args.copy()
            class_ = args.pop(CLASS, None)
            property_ = args.pop(PROPERTY, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if class_ is None or property_ is None: return "Both 'class' and 'property' must be specified", 400
            return service.semantic_types_post_put(class_, property_, True)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.type_id(),
            parameters.class_(),
            parameters.property(),
            parameters.namespaces(),
            parameters.source_names(),
            parameters.column_names(),
            parameters.column_ids(),
            parameters.models(),
            {
                "name": DELETE_ALL,
                "description": "Set this to true to delete all semantic types",
                "required": False,
                "allowMultiple": False,
                "dataType": "boolean",
                "paramType": "query"
            }
        ],
        responseMessages=responses.standard_delete(),
    )
    def delete(self):
        """
        Delete a semantic type
        Deletes all semantic types which match the given parameters.  Note that if no parameters are given a 400 will be returned.  If deleteAll is true, all semantic types will be deleted regardless of other parameters.
        """
        try:
            args = request.args.copy()
            if len(args) < 1: return "At least one argument needs to be provided", 400
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
            return service.semantic_types_delete(class_, property_, type_ids, namespaces, source_names, column_names,
                                                 column_ids, models, delete_all and delete_all.lower() == "true")
        except:
            return str(traceback.format_exc()), 500


class SemanticTypeColumns(Resource):
    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(desc="The ids of the column(s) to be returned"),
            parameters.source_names(),
            parameters.column_names(desc="The names of the column(s) to be returned"),
            parameters.models(),
            parameters.return_column_data()
        ],
        responseMessages=responses.standard_get()
    )
    def get(self, type_id):
        """
        Get the columns in a semantic type
        Returns all of the columns in a semantic type that match the given parameters.

        Returned body will have the following format, but if returnColumnData is not given as true, "data" will be omitted:
        <pre>
        [
            {
                "column_id": "",
                "name": "",
                "source": "",
                "model": "",
                "data": [
                    "",
                    "",
                    ""
                ]
            }
        ]
        </pre>
        Note that giving no parameters will return all columns with no data.
        """
        try:
            if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
            args = request.args.copy()
            column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
            column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
            source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
            models = args.pop(MODELS).split(",") if args.get(MODELS) else None
            return_column_data = args.pop(RETURN_COLUMN_DATA, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            return_column_data = True if return_column_data is not None and return_column_data.lower() == "true" else False
            return service.semantic_types_columns_get(type_id, column_ids, column_names, source_names, models,
                                                      return_column_data)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_names(True, "Name of the column to be created", False),
            parameters.source_names(True, "Name of the source of the column to be created", False),
            parameters.models(False, "Model of the column to be created, if none is given 'default' will be used",
                              False),
            parameters.body(False)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self, type_id):
        """
        Add a column to a semantic type
        Creates the column and returns the id
        """
        try:
            if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
            args = request.args.copy()
            column_name = args.pop(COLUMN_NAME, None)
            source_name = args.pop(SOURCE_NAME, None)
            model = args.pop(MODEL, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if column_name is None or source_name is None: return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
            if model is None: model = DEFAULT_MODEL
            data = request.data.split(
                "\n") if request.data is not None and request.data.strip() != "" and request.data.strip() != "{}" else []
            return service.semantic_types_columns_post_put(type_id, column_name, source_name, model, data, False)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_names(True, "Name of the column to be created", False),
            parameters.source_names(True, "Name of the source of the column to be created", False),
            parameters.models(False, "Model of the column to be created, if none is given 'default' will be used",
                              False),
            parameters.body(False)
        ],
        responseMessages=responses.standard_put()
    )
    def put(self, type_id):
        """
        Add/Replace a column to a semantic type
        Creates the column if it does not exist and replaces the column if it does, then returns the id if the column.
        """
        try:
            if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
            args = request.args.copy()
            column_name = args.pop(COLUMN_NAME, None)
            source_name = args.pop(SOURCE_NAME, None)
            model = args.pop(MODEL, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if column_name is None or source_name is None: return "Either 'columnName' or 'sourceColumn' was omitted and they are both required"
            if model is None: model = DEFAULT_MODEL
            data = request.data.split(
                "\n") if request.data is not None and request.data.strip() != "" and request.data.strip() != "{}" else []
            return service.semantic_types_columns_post_put(type_id, column_name, source_name, model, data, False)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(desc="The ids of the column(s) to be deleted"),
            parameters.source_names(),
            parameters.column_names(desc="The names of the column(s) to be deleted"),
            parameters.models()
        ],
        responseMessages=responses.standard_delete(),
    )
    def delete(self, type_id):
        """
        Delete a column from a semantic type
        Deletes all columns which match the given parameters.  Note that if no parameters are given all columns in that semantic type are deleted.
        """
        try:
            if type_id is None or len(type_id) < 1: return "Invalid type_id", 400
            args = request.args.copy()
            column_ids = args.pop(COLUMN_IDS).split(",") if args.get(COLUMN_IDS) else None
            column_names = args.pop(COLUMN_NAMES).split(",") if args.get(COLUMN_NAMES) else None
            source_names = args.pop(SOURCE_NAMES).split(",") if args.get(SOURCE_NAMES) else None
            models = args.pop(MODELS).split(",") if args.get(MODELS) else None
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            return service.semantic_types_columns_delete(type_id, column_ids, column_names, source_names, models)
        except:
            return str(traceback.format_exc()), 500


class SemanticTypeColumnData(Resource):
    @swagger.operation(
        parameters=[parameters.column_ids(True, "The ids of the column to get the info on", False, "path")],
        responseMessages=responses.standard_get()
    )
    def get(self, column_id):
        """
        Get the information on a column in a semantic type
        Returns all of the information on a column in a semantic type.

        Returned body will have the following format:
        <pre>
        {
            "column_id": "",
            "name": "",
            "source": "",
            "model": "",
            "data": [
                "",
                "",
                ""
            ]
        }
        </pre>
        """
        try:
            if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
            if len(request.args) > 0: return "Invalid arguments, there should be none", 400
            return service.semantic_types_column_data_get(column_id)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.column_ids(True, "The ids of the column to add the data to", False, "path"),
            parameters.body(True)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self, column_id):
        """
        Adds data to the given column
        Appends data to the given column.  Use put to replace the data instead
        """
        try:
            if request.data is None or request.data == "": return "Invalid message body", 400
            if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
            if len(request.args) > 0: return "Invalid arguments, there should be none", 400
            return service.semantic_types_column_data_post_put(column_id, request.data, False)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.column_ids(True, "The ids of the column to add the data to", False, "path"),
            parameters.body(True)
        ],
        responseMessages=responses.standard_put()
    )
    def put(self, column_id):
        """
        Replaces the data in the column
        Replaces the data in the column with the provided data
        """
        try:
            if request.data is None or request.data == "": return "Invalid message body", 400
            if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
            if len(request.args) > 0: return "Invalid arguments, there should be none", 400
            return service.semantic_types_column_data_post_put(column_id, request.data, True)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[parameters.column_ids(True, "The ids of the column to remove the data from", False, "path")],
        responseMessages=responses.standard_delete(),
    )
    def delete(self, column_id):
        """
        Delete all of the data in a column
        Removes all of the data in the column
        """
        try:
            if column_id is None or len(column_id) < 1: return "Invalid column_id", 400
            if len(request.args) > 0: return "Invalid arguments, there should be none", 400
            return service.semantic_types_column_data_delete(column_id)
        except:
            return str(traceback.format_exc()), 500


class BulkAddModels(Resource):
    @swagger.operation(
        parameters=[
            parameters.model_id(),
            parameters.model_names(),
            parameters.model_desc(),
            parameters.crunch_data(),
            {
                "name": SHOW_ALL,
                "description": "Show all of the model data",
                "required": False,
                "allowMultiple": False,
                "dataType": "boolean",
                "paramType": "query"
            }
        ],
        responseMessages=responses.standard_get()
    )
    def get(self):
        """
        Get bulk add models
        Returns all of the models which fit all of the given parameters (and all of them if no parameters are given).  If showAllData is true then the current state of each of the model.json files, otherwise "model" will be omitted.  If doNotCrunchDataNow is true, the learned semantic types will not be generated now, instead whatever is in the db will be used, which may or may not be current.  Every time a GET is run on a model with this set to false (or not given at all) the model in the db will be updated as well as returned as long as showAllData is true.  Return body will have the following format:
        <pre>
        [
            {
                "modelId": "",
                "name": "",
                "description": "",
                "model": { ... }
            }
        ]
        </pre>
        """
        try:
            args = request.args.copy()
            model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
            model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
            model_desc = args.pop(MODEL_DESC, None)
            show_all = args.pop(SHOW_ALL, None)
            crunch_data = args.pop(DO_NOT_CRUNCH_DATA_NOW, None)
            if len(args) > 0: return json_response("The following query parameters are invalid:  " + str(args.keys()),
                                                   400)
            show_all = True if show_all is not None and show_all.lower() == "true" else False
            crunch_data = False if crunch_data is not None and crunch_data.lower() == "false" else True
            return service.bulk_add_models_get(model_ids, model_names, model_desc, show_all, crunch_data)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.model(
                "The model for each of the created columns to be, if none is given 'bulk_add' will be used"),
            parameters.body(True, "The model.json file")
        ],
        responseMessages=responses.standard_put()
    )
    def post(self):
        """
        Add a bulk add model
        Add a bulk add model for adding information through POST /models/{model_id}, note that the id listed in the model is the id assigned to it, so it is not returned and must be unique.  The semantic types and columns given in the model will be created when this is sent.
        """
        try:
            if request.data is None or len(request.data) < 1: return "Invalid message body", 400
            args = request.args.copy()
            column_model = args.pop(MODEL, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if column_model is None: column_model = DEFAULT_BULK_MODEL
            return service.bulk_add_models_post(json.loads(request.data), column_model)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.model_id(),
            parameters.model_names(),
            parameters.model_desc()
        ],
        responseMessages=responses.standard_delete()
    )
    def delete(self):
        """
        Remove a bulk add model
        Removes all models which fit all of the given parameters.  Note that if no parameters are given all models will be removed, but the semantic types and data inside them will be left intact.
        """
        try:
            args = request.args.copy()
            model_ids = args.pop(MODEL_IDS).split(",") if args.get(MODEL_IDS) else None
            model_names = args.pop(MODEL_NAMES).split(",") if args.get(MODEL_NAMES) else None
            model_desc = args.pop(MODEL_DESC, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            return service.bulk_add_models_delete(model_ids, model_names, model_desc)
        except:
            return str(traceback.format_exc()), 500


class BulkAddModelData(Resource):
    @swagger.operation(
        parameters=[
            parameters.model_id(True, False, "path"),
            parameters.crunch_data()
        ],
        responseMessages=responses.standard_get()
    )
    def get(self, model_id):
        """
        Gets the current state of a bulk add model
        Returns the current state of the given bulk add model id.  If doNotCrunchDataNow is true, the learned semantic types will not be generated now, instead whatever is in the db will be used, which may or may not be current.  Every time a GET is run on a model with this set to false (or not given at all) the model in the db will be updated as well as returned.
        """
        try:
            if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
            args = request.args.copy()
            crunch_data = args.pop(DO_NOT_CRUNCH_DATA_NOW, None)
            if len(args) > 0: return json_response("The following query parameters are invalid:  " + str(args.keys()),
                                                   400)
            crunch_data = False if crunch_data is not None and crunch_data.lower() == "false" else True
            return service.bulk_add_model_data_get(model_id, crunch_data)
        except:
            return str(traceback.format_exc()), 500

    @swagger.operation(
        parameters=[
            parameters.model_id(True, False, "path"),
            parameters.model(
                "The model of the columns the data should be sent to, if none is given 'bulk_add' will be used"),
            parameters.body(True, "The jsonlines which contain the data to add")
        ],
        responseMessages=responses.standard_put()
    )
    def post(self, model_id):
        """
        Add data to the semantic types
        Adds data from jsonlines into the semantic types.  Each line of the body should be a full json file, with everything specified in the model.json.  This is the same as using POST /semantic_types/{type_id} to add data to columns, but faster for large amounts of data.
        """
        try:
            if model_id is None or len(model_id) < 1: return "Invalid model_id", 400
            if request.data is None or len(request.data) < 1: return "Invalid message body", 400
            args = request.args.copy()
            column_model = args.pop(MODEL, None)
            if len(args) > 0: return "The following query parameters are invalid:  " + str(args.keys()), 400
            if column_model is None: column_model = DEFAULT_BULK_MODEL
            data = []
            for line in request.data.split("\n"):
                if line.strip() != "":
                    data.append(json.loads(line))
            return service.bulk_add_model_data_post(model_id, column_model, data)
        except:
            return str(traceback.format_exc()), 500


api.add_resource(Predict, "/predict")
api.add_resource(SemanticTypes, "/semantic_types")
api.add_resource(SemanticTypeColumns, "/semantic_types/<string:" + TYPE_ID_PATH + ">")
api.add_resource(SemanticTypeColumnData, "/semantic_types/type/<string:" + COLUMN_ID_PATH + ">")
api.add_resource(BulkAddModels, "/bulk_add_models")
api.add_resource(BulkAddModelData, "/bulk_add_models/<string:" + MODEL_ID_PATH + ">")
app.run(debug=True, port=5000, use_reloader=False)
