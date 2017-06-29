import base64
import collections
import json

from flask import Response

######## General Constants #########
DATA_MODEL_PATH = "model/lr.pkl"  # File path for the model used by the semantic labeling
INDEX_NAME = "data"  # The index_name for use when saving attributes
DEFAULT_NAME = "default"  # Just a name for using when there isn't one
DEFAULT_MODEL = "default"  # Default model name for use when one isn't provided
DEFAULT_BULK_MODEL = "bulk_add"  # Default model for columns added using bulk add
NOT_ALLOWED_CHARS = '[\\/*?"<>|\s\t]'  # Characters not allowed by elastic search
ID_DIVIDER = "-"  # The divider that is used to separate the different parts of ID's, like class and property
CONFIDENCE = 0.1  # Semantic types which have a confidence of lower than this number on predict will not be returned
SAMPLE_SIZE = 1000 #if the size of the training data is MORE than this threshold value, then sample this threshold values randomly

######## Mongodb Names ########
ID = "_id"  # ID for any entry in the db
DATA_TYPE = "dataType"  # Name for the type of data the entry in the db is, should be used with one of the constants here like DATA_TYPE_SEMANTIC_TYPE
DATA_TYPE_SEMANTIC_TYPE = "type"  # Name for the Semantic Type, should be used with DATA_TYPE
DATA_TYPE_COLUMN = "column"  # Name for the Semantic Type's column, should be used with DATA_TYPE
DATA_TYPE_MODEL = "model"  # Name for the karma model that is uploaded, should be used with DATA_TYPE
TYPE_ID = "typeId"  # A column's semantic type's id
DATA = "data"  # Name for a column's data in the db
NAME = "name"  # A column's name
SOURCE = "source"  # A column's source
DESC = "description"  # Bulk add model description
BULK_ADD_MODEL_DATA = "bulkAddData"  # The full model that was given to the POST /bulk_add_models

######## Bulk Add model.json Constants ########
BAC_ID = "id"
BAC_URI = "uri"
BAC_NAME = "name"
BAC_DESC = "description"
BAC_GRAPH = "graph"
BAC_NODES = "nodes"
BAC_CLASS = "domain"
BAC_PROPERTY = "type"
BAC_COLUMN_NAME = "columnName"
BAC_USER_SEMANTIC_TYPES = "userSemanticTypes"
BAC_LEARNED_SEMANTIC_TYPES = "learnedSemanticTypes"
BAC_CONFIDENCE_SCORE = "confidenceScore"
BAC_COLUMN_NAME_FILE_NAME = "file_name"

######## Semantic Labeler Terms ########
SL_CONFIDENCE_SCORE = "prob"
SL_SEMANTIC_TYPE = "semantic_type"

######## Path Parameters ########
COLUMN_ID_PATH = "column_id"
TYPE_ID_PATH = "type_id"
MODEL_ID_PATH = "model_id"
TYPE_IDS = "typeIds"

######## Query Parameters ########
CLASS = "class"
PROPERTY = "property"
NAMESPACE = "namespace"
NAMESPACES = "namespaces"
COLUMN_NAME = "columnName"
COLUMN_NAMES = "columnNames"
SOURCE_NAME = "sourceName"
SOURCE_NAMES = "sourceNames"
COLUMN_IDS = "columnIds"
MODEL = "model"
MODELS = "models"
COLUMNS = "columns"
DELETE_ALL = "deleteAll"
RETURN_COLUMNS = "returnColumns"
RETURN_COLUMN_DATA = "returnColumnData"
BODY = "body"
#### Query parameters and return values for bulk add ####
SHOW_ALL = "showAllData"
MODEL_NAMES = "modelNames"
MODEL_DESC = "modelDesc"
MODEL_IDS = "modelIds"
MODEL_ID = "modelId"
DO_NOT_CRUNCH_DATA_NOW = "doNotCrunchDataNow"

######## Other return names ########
SCORE = "score"


def json_response(json_body, code):
    """
    Use this to return a nice looking and ordered json with flask.

    :param json_body: The body to make a pretty printed json object
    :param code:      The return code to return
    :return: A flask Response
    """
    return Response(response=str(json.dumps(json_body, ensure_ascii=False, indent=4)), status=code,
                    mimetype="application/json")


def get_type_id(class_, property_):
    """
    Returns the id of the semantic type with the given class and property.

    :param class_:    Class of the semantic type
    :param property_: Property of the semantic type
    :return: The id string of the semantic type
    """
    return base64.b64encode(class_) + ID_DIVIDER + base64.b64encode(property_)


def decode_type_id(type_id):
    """
    Returns the class and property of the semantic type with the given id.

    :param type_id: Id of the semantic type to decode
    :return: The semantic type's class and property in the form (class, property)
    """
    split_id = type_id.split(ID_DIVIDER)
    return base64.b64decode(split_id[0]), base64.b64decode(split_id[1])


def get_column_id(type_id, column_name, source_name, model):
    """
    Returns the id of the column with the given semantic type id, column name, source name, and model.

    :param type_id:     Id of the column's semantic type
    :param column_name: Name of the column
    :param source_name: Name of the column's source
    :param model:       The column's model
    :return: The id string of the column
    """
    return type_id + ID_DIVIDER + base64.b64encode(column_name) + ID_DIVIDER + base64.b64encode(
        source_name) + ID_DIVIDER + base64.b64encode(model)


def clean_column_output(column, show_data=True):
    """
    Gives nice, clean, ordered output for a column (in an OrderedDict).

    :param column:    The dictionary which contains all of the column info and data
    :param show_data: If the column's data should be in the output dictionary
    :return: An OrderedDict of the column
    """
    o = collections.OrderedDict()
    o[COLUMN_ID_PATH] = str(column[ID])
    o[NAME] = column[COLUMN_NAME]
    o[SOURCE] = column[SOURCE_NAME]
    o[MODEL] = column[MODEL]
    if show_data:
        o[DATA] = column["values"]
    return o


def clean_columns_output(column_input, show_data):
    """
    Gives nice, clean, ordered output of columns for returning to the user.

    :param column_input: A list of column dictionaries
    :param show_data:    If each column should have its data listed with it
    :return: Map of all of the columns for returning to the user
    """
    return map(lambda t: clean_column_output(t, show_data), column_input)


def get_type_from_column_id(column_id):
    """
    Returns the type id from a column id

    :param column_id: Id of the column to get the semantic type id out of
    :return: The semantic type id
    """
    split_type_id = column_id.split(ID_DIVIDER)
    return split_type_id[0] + ID_DIVIDER + split_type_id[1]
