from flask import Flask
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
from pymongo import MongoClient

import config


# from semantic_labeling.run_experiments import SemanticLabeler


# semantic_labeler = SemanticLabeler()
db = MongoClient().data
app = Flask(__name__, static_folder='../static')
api = swagger.docs(Api(app), apiVersion='0.2', basePath='/', resourcePath='/', produces=["application/json", "text/html"], api_spec_url='/api/spec', description='Semantic Typing')
CORS(app)


class parameters(object):
    @staticmethod
    def type_id(required=False, multiple=True, param_type="query"):
        return {
            "name": "type_ids" if multiple else "type_id",
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
            "name": "sourceColumns" if multiple else "sourceColumn",
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
            "name": "columnIds" if multiple else "columnId",
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
    def body(required=False):
        return {
            "name": "body",
            "description": "List of data values which will be inserted into the column (one per line), all lines will be included as values, including blank ones",
            "required": required,
            "allowMultiple": False,
            "dataType": "string",
            "paramType": "body"
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
    def standard_post():
        return [
            {"code": 201, "message": "Created"},
            {"code": 400, "message": "Bad Request"},
            {"code": 500, "message": "Internal Server Error"}
        ]


    @staticmethod
    def standard_delete():
        return [
            {"code": 204, "message": "Deleted"},
            {"code": 400, "message": "Bad Request"},
            {"code": 404, "message": "Not Found"},
            {"code": 500, "message": "Internal Server Error"}
        ]


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
                "id": "",
                "class": "",
                "property": "",
                "namespace": "",
                "columns": [
                    {
                        "id": "",
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
        return


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
        Creates a semantic type and returns the its id.
        """
        return


    @swagger.operation(
        parameters=[
            parameters.type_id(),
            parameters.class_(),
            parameters.property(),
            parameters.namespaces(),
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
        return


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
                "id": "",
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
        return


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_names(True, "Name of the column to be created", False),
            parameters.source_names(True, "Name of the source of the column to be created", False),
            parameters.models(True, "Model of the column to be created, if none is given 'default' will be used", False),
            {
                "name": "force",
                "description": "Force add the column, replacing it if already exists",
                "required": False,
                "allowMultiple": False,
                "dataType": 'boolean',
                "paramType": "query"
            },
            parameters.body(False)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self, type_id):
        """
        Add a column to a semantic type
        Creates the column and returns the id
        """
        return


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
        return


class SemanticTypeColumnData(Resource):
    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(True, "The ids of the column to get the info on", False, "path")
        ],
        responseMessages=responses.standard_get()
    )
    def get(self):
        """
        Get the information on a column in a semantic type
        Returns all of the information on a column in a semantic type.

        Returned body will have the following format:
        <pre>
        {
            "id": "",
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
        return


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(True, "The ids of the column to add the data to", False, "path"),
            parameters.body(True)
        ],
        responseMessages=responses.standard_post()
    )
    def post(self):
        """
        Adds data to the given column
        Appends data to the given column.  Use put to replace the data instead
        """
        return


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(True, "The ids of the column to add the data to", False, "path"),
            parameters.body(True)
        ],
        responseMessages=responses.standard_post()
    )
    def put(self):
        """
        Replaces the data in the column
        Replaces the data in the column with the provided data
        """
        return


    @swagger.operation(
        parameters=[
            parameters.type_id(True, False, "path"),
            parameters.column_ids(True, "The ids of the column to remove the data from", False, "path")
        ],
        responseMessages=responses.standard_delete(),
    )
    def delete(self):
        """
        Delete all of the data in a column
        Removes all of the data in the column
        """
        return


class Predict(Resource):
    def get(self):
        return


class Models(Resource):
    def get(self):
        return


    def post(self):
        return


    def delete(self):
        return


class ModelData(Resource):
    def post(self):
        return


api.add_resource(SemanticTypes, '/semantic_types')
api.add_resource(SemanticTypeColumns, '/semantic_types/<string:type_id>')
api.add_resource(SemanticTypeColumnData, '/semantic_types/<string:type_id>/<string:column_id>')
api.add_resource(Predict, '/predict')
api.add_resource(Models, '/models')
api.add_resource(ModelData, '/models/<string:model_id>')

if __name__ == '__main__':
    app.run(debug=True, port=config.PORT, use_reloader=False)
