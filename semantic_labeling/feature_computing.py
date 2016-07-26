from collections import defaultdict

from semantic_labeling import sc, similarity_test_map, TF_TEXT
from utils.helpers import adjust_result


def compute_feature_vectors(labeled_attrs_map, test_attr_map, tf_idf_map):
    similarity_result_map = sc.parallelize(labeled_attrs_map).filter(
        lambda hit: hit["metric_type"] in similarity_test_map.keys()).map(
        lambda hit: (
            (hit["semantic_type"], hit["source_name"] + "!" + hit["name"], hit["metric_type"]), hit)).mapValues(
        lambda hit: similarity_test_map[hit["metric_type"]](hit[hit["metric_type"]], test_attr_map[hit["metric_type"]],
                                                            hit["num_fraction"],
                                                            test_attr_map["num_fraction"])).reduceByKey(max).collect()
    feature_vectors = defaultdict(lambda: defaultdict(lambda: 0))

    for result in similarity_result_map:
        feature_vectors[(result[0][0], result[0][1])][result[0][2]] = result[1]
        feature_vectors[(result[0][0], result[0][1])][TF_TEXT] = 0

    for hit in tf_idf_map['hits']['hits']:
        source = hit['_source']
        score = adjust_result(hit['_score'], 1 - test_attr_map["num_fraction"], 1 - source["num_fraction"])
        name = (source['semantic_type'], source["source_name"] + '!' + source["name"])
        if feature_vectors[name][TF_TEXT] < score:
            feature_vectors[name][TF_TEXT] = score

    for key in feature_vectors.keys():
        feature_vectors[key]["semantic_type"] = key[0]
        feature_vectors[key]["column_name"] = key[1]
        feature_vectors[key]["test_name"] = test_attr_map["source_name"] + "!" + test_attr_map["name"]
        feature_vectors[key]["truth"] = test_attr_map["semantic_type"]
        feature_vectors[key]["target"] = (key[0] == test_attr_map["semantic_type"])
        if TF_TEXT not in feature_vectors[key]:
            feature_vectors[key][TF_TEXT] = 0

    return feature_vectors.values()
