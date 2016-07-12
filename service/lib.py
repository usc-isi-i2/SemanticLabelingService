from config import UPLOAD_FOLDER
import os
from service import encode, decode


def decode_semantic_type(encoded):
    semantic_type = decode(encoded).split("\n")
    return {"domain": {"uri": semantic_type[0]}, "type": {"uri": semantic_type[1]}}


def encode_semantic_type(domain, _property):
    return encode(domain + "\n" + _property)


def get_semantic_type_id(namespace, encoded_semantic_type):
    return encode(namespace + "\n" + encoded_semantic_type)


def semantic_type_id_decode(_id):
    encoded_namespace, semantic_type = decode(_id).split("\n")
    return decode(encoded_namespace), decode_semantic_type(semantic_type)


def semantic_type_folder(_id):
    namespace, semantic_type = decode(_id).split("\n")
    return os.path.join(UPLOAD_FOLDER, namespace, semantic_type)


def get_column_id(source_name, column_name):
    return encode(source_name + "\n" + encode(column_name))


def column_id_decode(_column_id):
    source_name, column_name_encoded = decode(_column_id).split("\n")
    return source_name, decode(column_name_encoded)


def get_column_path(_id, _column_id):
    semantic_type_folder_path = semantic_type_folder(_id)
    source_name, column_name = column_id_decode(_column_id)
    return os.path.join(semantic_type_folder_path, source_name, encode(column_name))


def get_semantic_type_path(_id):
    namespace, semantic_type_encoded = decode(_id).split("\n")
    return os.path.join(UPLOAD_FOLDER, namespace, semantic_type_encoded)


def column_path_to_ids(path):
    path = path.split(os.sep)
    column_name = decode(path[-1])
    source_name = path[-2]
    _column_id = get_column_id(source_name, column_name)
    encoded_semantic_type = path[-3]
    namespace = path[-4]
    _id = get_semantic_type_id(namespace, encoded_semantic_type)
    return _id, _column_id


def column_path_to_properties(path):
    path = path.split(os.sep)
    column_name = decode(path[-1])
    source_name = path[-2]
    semantic_type = decode_semantic_type(path[-3])
    namespace = path[-4]
    return namespace, semantic_type, source_name, column_name
