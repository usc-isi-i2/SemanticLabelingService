from search_engine import es
from semantic_labeling import data_collection, relation_collection, TF_TEXT


class Indexer:
    def __init__(self):
        pass

    @staticmethod
    def check_index_exists(index_name):
        return es.indices.exists(index_name)

    @staticmethod
    def index_column(column, source_name, index_name):
        body = column.to_json()
        for key in body.keys():
            obj = {"name": body["name"], "semantic_type": body["semantic_type"], "source_name": source_name,
                   "num_fraction": body['num_fraction']}
            if key in ["name", "semantic_type", "source_name", "num_fraction"]:
                continue
            obj[key] = body[key]
            obj["set_name"] = index_name
            obj["metric_type"] = key
            if key == TF_TEXT:
                es.index(index=index_name, doc_type=source_name, body=obj)
            else:
                data_collection.insert_one(obj)
                # es.index(index=index_name, doc_type=source_name, body=obj)

    @staticmethod
    def index_relation(relation, type1, type2, flag):
        query = {"type1": type1, "type2": type2, "relation": relation, "true_count": {"$exists": True}}
        relation_collection.find_and_modify(query, {"$inc": {"true_count": 1 if flag else 0, "total_count": 1}},
                                            upsert=True)
        query["true_count"]["$exists"] = False
        relation_collection.find_and_modify(query, {"$set": {"true_count": 1 if flag else 0, "total_count": 1}},
                                            upsert=True)

    @staticmethod
    def delete_column(column_name, source_name, index_name):
        data_collection.delete_many({"name": column_name, "source_name": source_name, "set_name": index_name})
        es.delete_by_query(index_name, {"name": column_name}, source_name)
