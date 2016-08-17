from elasticsearch.helpers import scan, bulk

from search_engine import es
from semantic_labeling import data_collection, relation_collection, TF_TEXT, coop_collection


class Indexer:
    def __init__(self):
        pass

    @staticmethod
    def check_set_indexed(index_name):
        return es.indices.exists(index_name)

    @staticmethod
    def store_attribute(attr, source_name, index_name):
        body = attr.to_json()
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

    @staticmethod
    def delete_attribute(attr_name, source_name, index_name):
        data_collection.delete_many({"name": attr_name, "source_name": source_name, "set_name": index_name})
        bulk_deletes = []
        for result in scan(es, query={
            "query": {
                "match": {
                    "name": attr_name,
                }
            }
        }, index=index_name, doc_type=source_name, _source=False,
                           track_scores=False, scroll='5m'):
            result['_op_type'] = 'delete'
            bulk_deletes.append(result)
        bulk(es, bulk_deletes)
