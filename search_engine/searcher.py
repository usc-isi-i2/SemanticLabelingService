from elasticsearch.helpers import scan

from search_engine import es
from semantic_labeling import TF_TEXT, data_collection, relation_collection


class Searcher:
    def __init__(self):
        pass

    @staticmethod
    def search_columns_data(index_name, source_names):
        # result = list(scan(es, index=index_name, doc_type=','.join(source_names),
        #                    query={"query": {"match_all": {}}}))
        result = list(data_collection.find({"set_name": index_name, "source_name": {"$in": source_names}}))
        return result

    @staticmethod
    def search_relations_data(type1, type2, relation):
        result = relation_collection.find_one({"type1": type1, "type2": type2, "relation": relation})
        return result

    @staticmethod
    def search_similar_text_data(index_name, text, source_names):
        try:
            result = es.search(index=index_name, doc_type=','.join(source_names),
                               body={
                                   "query": {
                                       "match": {
                                           TF_TEXT: text,
                                       }
                                   }
                               },
                               size=10)
        except Exception as e:
            print e
            result = {"hits": {"hits": []}}
        return result

    @staticmethod
    def search_types_data(index_name, source_names):
        return Searcher.search_columns_data(index_name, source_names)
