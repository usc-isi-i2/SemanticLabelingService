import base64


def encode(string):
    return base64.b64encode(string)


def decode(string64):
    return base64.b64decode(string64)


# names of the columns in mongoDB
NAMESPACE = "namespace"
IS_TRAIN = "isTrain"
CLASS = "class"
PROPERTY = "property"
COLUMN = "column"
SOURCE_NAME = "source_name"
COLUMN_NAME = "column_name"
ID = "id"
COLUMN_ID = "column_id"
MODEL = "model"
TIMESTAMP = "timestamp"
