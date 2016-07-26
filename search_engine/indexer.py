from search_engine import es
from semantic_labeling import data_collection, relation_collection


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
            data_collection.insert_one(obj)
            # es.index(index=index_name, doc_type=source_name, body=obj)

    @staticmethod
    def index_relation(relation, type1, type2, flag):
        query = {"type1": type1, "type2": type2, "relation": relation}
        relation_collection.update(query, {"$inc": {"true_count": 1 if flag else 0, "total_count": 1}},
                                   {"$setOnInsert": {"true_count": 1 if flag else 0, "total_count": 1}},
                                   {"upsert": True})

    @staticmethod
    def delete_column(index_name):
        if es.indices.exists(index_name):
            es.delete(index=index_name)
            return True
        return False
