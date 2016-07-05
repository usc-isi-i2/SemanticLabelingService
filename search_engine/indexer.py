from search_engine import es


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
            obj["metric_type"] = key
            es.index(index=index_name, doc_type=source_name, body=obj)

    @staticmethod
    def index_source(source, index_name):
        for column in source.column_map.values():
            if column.semantic_type:
                Indexer.index_column(column, source.index_name, index_name)

    @staticmethod
    def delete_column(index_name):
        if es.indices.exists(index_name):
            es.delete(index=index_name)
            return True
        return False
