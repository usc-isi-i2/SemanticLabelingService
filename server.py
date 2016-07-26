import traceback
from flask import Flask, request
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful_swagger import swagger

import config
import service.serverLogic


app = Flask(__name__, static_folder='../static')
api = swagger.docs(Api(app), apiVersion='0.2', basePath='/', resourcePath='/', produces=["application/json", "text/html"], api_spec_url='/api/spec', description='Semantic Typing')
CORS(app)
service = service.serverLogic.Server()


################################################################################################################################
#                                                                                                                              #
#  This class is only used for what gets called by the API and swagger docs.  There isn't any logic code here aside from the   #
#  try/catch for returning 500's since I think it would be messy and harder to maintain.  It's all in service/serverLogic.py   #
#                                                                                                                              #
#  Each of the API functions in this class should call a helper function in the Server class of serverLogic.py whose name      #
#  follows the form of {class_name}_{type}, where {class_name} is the name of the class in this file, but with underscores     #
#  instead of camelCase and {type} is the HTTP method name, such as GET or POST.  Example: semantic_types_get()                #
#                                                                                                                              #
################################################################################################################################


class parameters(object):
    @staticmethod
    def type_id(required=False, multiple=True, param_type="query"):
        return {
            "name": "type_id" if param_type == "path" else "typeIds" if multiple else "typeId",
            "description": "Ids of the semantic types" if multiple else "Id of the semantic type",
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
        }


    @staticmethod
    def class_(required=False):
        return {
            "name": "class",
            "description": "Uri of a class",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def property(required=False):
        return {
            "name": "property",
            "description": "Uri of a property",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def namespaces(required=False):
        return {
            "name": "namespaces",
            "description": "List of URIs of parent URIs of property which to consider",
            "required": required,
            "allowMultiple": True,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def source_names(required=False, desc="List of source names that the column(s) should have", multiple=True):
        return {
            "name": "sourceNames" if multiple else "sourceName",
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def column_names(required=False, desc="List of column names which the semantic type(s) should have", multiple=True):
        return {
            "name": "columnNames" if multiple else "columnName",
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def column_ids(required=False, desc="List of column ids which the semantic type(s) should have", multiple=True, param_type="query"):
        return {
            "name": "column_id" if param_type == "path" else "columnIds" if multiple else "columnId",
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
        }


    @staticmethod
    def models(required=False, desc="List of models which the column(s) should have", multiple=True):
        return {
            "name": "models" if multiple else "model",
            "description": desc,
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def return_column_data(desc="If the data in the columns should be in the return body"):
        return {
            "name": "returnColumnData",
            "description": desc,
            "required": False,
            "allowMultiple": False,
            "dataType": "boolean",
            "paramType": "query"
        }


    @staticmethod
    def body(required=False, desc="List of data values which will be inserted into the column (one per line), all lines will be included as values, including blank ones"):
        return {
            "name": "body",
            "description": desc,
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "body"
        }


    @staticmethod
    def model_names(required=False):
        return {
            "name": "modelNames",
            "description": "Name of the models",
            "required": required,
            "allowMultiple": True,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def model_desc(required=False):
        return {
            "name": "modelDesc",
            "description": "Part or all of a model description",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "query"
        }


    @staticmethod
    def model_id(required=False, multiple=True, param_type="query"):
        return {
            "name": "model_id" if param_type == "path" else "modelIds" if multiple else "modelId",
            "description": "Id(s) of the model.json",
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": param_type
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
            parameters.source_names(False, "Source the data may belong to", False),
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
                "id": "",
                "score":
            }
        ]
        </pre>
        """
        try: return service.predict_post(request.args, request.data)
        except: return str(traceback.format_exc()), 500


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
                "name": "returnColumns",
                "description": "If the columns for the semantic type(s) should be in the return body",
                "required": False,
                "allowMultiple": False,
                "dataType": "boolean",
                "paramType": "query"
            },
            parameters.return_column_data("If the data in the columns should be in the return body, if this is true it will override returnColumns")
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
        try: return service.semantic_types_get(request.args)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_post(request.args)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_put(request.args)
        except: return str(traceback.format_exc()), 500


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
                "name": "deleteAll",
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
        try: return service.semantic_types_delete(request.args)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_columns_get(type_id, request.args)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_names(True, "Name of the column to be created", False),
            parameters.source_names(True, "Name of the source of the column to be created", False),
            parameters.models(False, "Model of the column to be created, if none is given 'default' will be used", False),
            parameters.body(False)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self, type_id):
        """
        Add a column to a semantic type
        Creates the column and returns the id
        """
        try: return service.semantic_types_columns_post(type_id, request.args, request.data)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_names(True, "Name of the column to be created", False),
            parameters.source_names(True, "Name of the source of the column to be created", False),
            parameters.models(False, "Model of the column to be created, if none is given 'default' will be used", False),
            parameters.body(False)
        ],
        responseMessages=responses.standard_put()
    )
    def put(self, type_id):
        """
        Add/Replace a column to a semantic type
        Creates the column if it does not exist and replaces the column if it does, then returns the id if the column.
        """
        try: return service.semantic_types_columns_put(type_id, request.args, request.data)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_columns_delete(type_id, request.args)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_column_data_get(column_id, request.args)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_column_data_post(column_id, request.args, request.data)
        except: return str(traceback.format_exc()), 500


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
        try: return service.semantic_types_column_data_put(column_id, request.args, request.data)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[parameters.column_ids(True, "The ids of the column to remove the data from", False, "path")],
        responseMessages=responses.standard_delete(),
    )
    def delete(self, column_id):
        """
        Delete all of the data in a column
        Removes all of the data in the column
        """
        try: return service.semantic_types_column_data_delete(column_id, request.args)
        except: return str(traceback.format_exc()), 500


class Models(Resource):
    @swagger.operation(
        parameters=[
            parameters.model_id(),
            parameters.model_names(),
            parameters.model_desc(),
            {
                "name": "showAllData",
                "description": "Show all of the model data",
                "required": False,
                "allowMultiple": False,
                "dataType": 'boolean',
                "paramType": "query"
            }
        ],
        responseMessages=responses.standard_get()
    )
    def get(self):
        """
        Get bulk add models
        Returns all of the models which fit all of the given parameters (and all of them if no parameters are given).  If showAllData is true then the current state of each of the model.json files, otherwise "model" will be omitted.  Return body will have the following format:
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
        try: return service.models_get(request.args)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[parameters.body(True, "The model.json file")],
        responseMessages=responses.standard_put()
    )
    def post(self):
        """
        Add a bulk add model
        Add a bulk add model for adding information through POST /models/{model_id}, note that the id listed in the model is the id assigned to it, so it is not returned and must be unique.  The semantic types and columns given in the model will be created when this is sent.
        """
        try: return service.models_post(request.args, request.data)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[
            parameters.model_id(),
            parameters.model_names(),
            parameters.model_desc(),
        ],
        responseMessages=responses.standard_delete()
    )
    def delete(self):
        """
        Remove a bulk add model
        Removes all models which fit all of the given parameters.  Note that if no parameters are given all models will be removed, but the semantic types and data inside them will be left intact.
        """
        try: return service.models_delete(request.args)
        except: return str(traceback.format_exc()), 500


class ModelData(Resource):
    @swagger.operation(
        parameters=[parameters.model_id(True, False, "path")],
        responseMessages=responses.standard_get()
    )
    def get(self, model_id):
        """
        Gets the current state of a bulk add model
        Returns the current state of the given bulk add model id
        """
        try: return service.model_data_get(model_id, request.args)
        except: return str(traceback.format_exc()), 500


    @swagger.operation(
        parameters=[
            parameters.model_id(True, False, "path"),
            parameters.body(True, "The jsonlines which contain the data to add")
        ],
        responseMessages=responses.standard_put()
    )
    def post(self, model_id):
        """
        Add data to the semantic types
        Adds data from jsonlines into the semantic types.  Each line of the body should be a full json file, with everything specified in the model.json.  This is the same as using POST /semantic_types/{type_id} to add data to columns, but faster for large amounts of data.
        """
        try: return service.model_data_post(model_id, request.args, request.data)
        except: return str(traceback.format_exc()), 500


api.add_resource(Predict, '/predict')
api.add_resource(SemanticTypes, '/semantic_types')
api.add_resource(SemanticTypeColumns, '/semantic_types/<string:type_id>')
api.add_resource(SemanticTypeColumnData, '/semantic_types/type/<string:column_id>')
api.add_resource(Models, '/bulk_add_models')
api.add_resource(ModelData, '/bulk_add_models/<string:model_id>')
app.run(debug=True, port=config.PORT, use_reloader=False)
