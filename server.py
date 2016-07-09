from flask import Flask
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
from pymongo import MongoClient

import config
# from semantic_labeling.run_experiments import SemanticLabeler
from service.errors import message


# semantic_labeler = SemanticLabeler()
db = MongoClient().data
app = Flask(__name__, static_folder='../static')
api = swagger.docs(Api(app), apiVersion='0.2', basePath='/', resourcePath='/', produces=["application/json", "text/html"], api_spec_url='/api/spec', description='Semantic Typing')
CORS(app)


class SemanticTypes(Resource):
    def get(self):
        return


    def post(self):
        return


    def delete(self):
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
