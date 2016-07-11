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
    def type_id(required=False, multiple=True):
        return {
            "name": "type_id",
            "description": "Id of the semantic type",
            "required": required,
            "allowMultiple": multiple,
            "dataType": "string",
            "paramType": "query"
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
    def column_names(required=False, desc="List of column names which the semantic type(s) should have"):
        return {
            "name": "columnNames",
            "description": desc,
            "required": required,
            "allowMultiple": True,
            "dataType": "string",
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
            parameters.column_names(),
            {
                "name": "returnColumns",
                "description": "If the columns for the semantic type(s) should be in the return body",
                "required": False,
                "allowMultiple": False,
                "dataType": "boolean",
                "paramType": "query"
            },
            {
                "name": "returnColumnData",
                "description": "If the data in the columns for the semantic type(s) should be in the return body, if this is true it will override returnColumns",
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
        Get semantic types
        Returns all of the semantic types which fit the given parameters.

        Returned body will have the format, but if returnColumns or returnColumnData is not given as true, then "columns" will be omitted and if returnDataColumns is not given as true "data" will be omitted:
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
        Deletes all semantic types which match the given parameters.  Note that if no parameters are given a 400 will be returned and if deleteAll is true, all semantic types will be deleted regardless of other parameters.
        """
        return


class SemanticTypeColumns(Resource):
    def get(self):
        return


    def post(self):
        return


    def delete(self):
        return


class SemanticTypeColumnData(Resource):
    def get(self):
        return


    def post(self):
        return


    def put(self):
        return


    def delete(self):
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
