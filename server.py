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


class Test(Resource):
    @swagger.operation(
        title="mehmeh",
        notes="Meh",
        responseMessages=[
            {
                "code": 246,
                "message": "Worked"
            }
        ]
    )
    def get(self):
        return message("Yay", 246)


api.add_resource(Test, '/test')

if __name__ == '__main__':
    app.run(debug=True, port=config.PORT, use_reloader=False)
