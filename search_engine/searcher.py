from search_engine import es
from semantic_labeling import TF_TEXT, data_collection


class Searcher:
    def __init__(self):
        pass

    @staticmethod
    def search_attribute_data(set_name, source_names):
        result = list(data_collection.find({"set_name": set_name, "source_name": {"$in": source_names}}, {"_id": 0}))
        return result

    @staticmethod
    def search_attribute_data_by_name(column_name, index_name, source_name):
        result = data_collection.find_one(
            {"set_name": index_name, "source_name": source_name, "name": column_name, "value_list": {"$exists": True}},
            {"_id": 0})
        return result

    @staticmethod
    def search_similar_text_data(set_name, text, source_names):
        try:
            result = es.search(index=set_name, doc_type=','.join(source_names),
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
        return Searcher.search_attribute_data(index_name, source_names)
