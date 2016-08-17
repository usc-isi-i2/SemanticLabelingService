import csv
import itertools
import locale
import math
import operator
import os
from collections import Counter, defaultdict

from search_engine.indexer import Indexer
from search_engine.searcher import Searcher
from semantic_labeling import KS_NUM, JC_NUM, JC_TEXT, MW_HIST, JC_NAME, TF_TEXT, debug_writer, EL_DIST
from semantic_labeling.feature_computing import compute_feature_vectors
from utils.helpers import split_number_text


class DataSet:
    def __init__(self, name):
        self.name = name
        self.folder_path = None
        self.source_map = {}

    def read(self, folder_path):
        self.folder_path = folder_path
        is_saved = self.is_saved()
        for file_path in os.listdir(folder_path):
            print file_path
            source = DataSource(os.path.splitext(file_path)[0])
            source.read(os.path.join(folder_path, file_path))
            if not is_saved:
                source.save(self.name)
            self.source_map[source.name] = source

    def is_saved(self):
        return Indexer.check_set_indexed(self.name)

    def save(self):
        for source in self.source_map.values():
            source.save(self.name)

    def test(self, size_list, classifier):
        mrr_scores = {}
        for size in size_list:
            score = 0
            count = 0
            for idx, key in enumerate(sorted(self.source_map.keys())):
                source = self.source_map[key]
                labeled_sources, labeled_attrs_map = self.get_labeled_sources(idx, size)
                print key, labeled_sources
                prediction_map = source.label(self.name, labeled_attrs_map, classifier, labeled_sources)
                for attr_name in prediction_map:
                    find = False
                    attr = source.attr_map[attr_name]
                    rank = 0
                    count += 1
                    result = defaultdict(lambda: [])
                    for obj in sorted(prediction_map[attr_name].items(), key=operator.itemgetter(1), reverse=True):
                        result[round(obj[1], 2)].append(obj[0])
                    for obj in sorted(result.items(), key=operator.itemgetter(0), reverse=True):
                        rank += 1
                        debug_writer.write(source.name + "\t" +
                                           attr.name + "\t" + attr.semantic_type + "\t" + str(obj) + "\n")
                        if attr.semantic_type in obj[1]:
                            find = True
                            break
                    if find:
                        score += 1.0 / rank
                    debug_writer.write(str(score) + "\n")
            mrr_scores[size] = score * 1.0 / count
        return mrr_scores

    def test_with_different_set(self, classifier, set_name, labeled_sources):
        score = 0
        count = 0
        labeled_attrs_map = Searcher.search_attribute_data(set_name, labeled_sources)
        for idx, key in enumerate(sorted(self.source_map.keys())):
            source = self.source_map[key]
            prediction_map = source.label(self.name, labeled_attrs_map, classifier, labeled_sources)
            print source.resolve_coocurence(prediction_map, self.name, labeled_sources)
            for attr_name in prediction_map:
                find = False
                attr = source.attr_map[attr_name]
                rank = 0
                count += 1
                for obj in prediction_map[attr_name]:
                    rank += 1
                    debug_writer.write(source.name + "\t" +
                                       attr.name + "\t" + attr.semantic_type + "\t" + str(obj) + "\n")
                    if obj["semantic_type"] == attr.semantic_type:
                        find = True
                        break
                if find:
                    score += 1.0 / rank
                debug_writer.write(str(score) + "\n")
        mrr_score = score * 1.0 / count
        return mrr_score

    def get_labeled_sources(self, idx=0, size=0):
        double_list = sorted(self.source_map.keys()) * 2
        labeled_sources = double_list[idx + 1: idx + size + 1]
        labeled_attrs_map = Searcher.search_attribute_data(self.name, labeled_sources)
        return labeled_sources, labeled_attrs_map

    def generate_training_data(self, size_list):
        train_data = []
        for size in size_list:
            for idx, source in enumerate(self.source_map.values()):
                labeled_sources, labeled_attrs_map = self.get_labeled_sources(idx, size)
                for attr in source.attr_map.values():
                    if attr.semantic_type and attr.value_list:
                        feature_vectors = attr.compute_features(self.name, labeled_sources, labeled_attrs_map)
                        train_data += feature_vectors
        return train_data


class DataSource:
    def __init__(self, name):
        self.attr_map = {}
        self.entity_list = []
        self.name = name

    def read(self, file_path):
        with open(file_path) as csv_file:
            reader = csv.DictReader(csv_file)
            type_row = True
            for attr_name in reader.fieldnames:
                self.attr_map[attr_name] = Attribute(attr_name, self.name)

            for row in reader:
                if type_row:
                    for attr_name in reader.fieldnames:
                        self.attr_map[attr_name].semantic_type = row[attr_name]
                    type_row = False
                else:
                    for attr_name in reader.fieldnames:
                        self.attr_map[attr_name].add_value(row[attr_name])

    def save(self, set_name):
        for attr in self.attr_map.values():
            if attr.semantic_type and attr.value_list:
                attr.save(set_name)

    def label(self, set_name, labeled_attrs_map, classifier, labeled_sources):
        result = defaultdict(lambda: {})
        for attr in self.attr_map.values():
            if attr.semantic_type and attr.value_list:
                prediction = attr.predict_type(set_name, labeled_sources, labeled_attrs_map, classifier)
                for semantic_type in prediction:
                    result[attr.name][semantic_type["semantic_type"]] = semantic_type["prob"]
        return result


class Attribute:
    def __init__(self, name, source_name):
        self.name = name
        self.source_name = source_name
        self.semantic_type = None

        self.text = ""

        self.value_list = []

        self.numeric_list = []
        self.frequency_list = []
        self.textual_list = []

        self.is_prepared = False
        self.num_fraction = 0

        self.num_len = 0
        self.text_len = 0

    def add_value(self, value):
        if len(self.value_list) > 500:
            return

        value = value.strip()

        try:
            value = value.encode("ascii", "ignore")
        except:
            value = value.decode("unicode_escape").encode("ascii", "ignore")

        if not value:
            return

        num, text = split_number_text(value)

        if text:
            self.textual_list.append(text)
            self.text_len += 1
        if num:
            self.numeric_list.append(max([locale.atof(v[0]) for v in num]))
            self.num_len += 1

        self.value_list.append(value)

    def to_json(self):
        self.prepare_data()
        json_obj = {"name": self.name, "source_name": self.source_name, "semantic_type": self.semantic_type,
                    "num_fraction": self.num_fraction, "value_list": self.value_list, KS_NUM: self.numeric_list,
                    JC_NUM: self.numeric_list, JC_TEXT: list(set(self.textual_list)), MW_HIST: self.frequency_list,
                    EL_DIST: self.numeric_list, JC_NAME: self.name, TF_TEXT: self.text,}
        return json_obj

    def compute_features(self, set_name, labeled_sources, labeled_attrs_map):
        self.prepare_data()
        tf_idf_map = Searcher.search_similar_text_data(set_name, self.text, labeled_sources)
        feature_vectors = compute_feature_vectors(labeled_attrs_map, self.to_json(), tf_idf_map)
        return feature_vectors

    def predict_type(self, set_name, labeled_sources, labeled_attrs_map, classifier):
        feature_vectors = self.compute_features(set_name, labeled_sources, labeled_attrs_map)
        predictions = classifier.predict(feature_vectors)

        predictions = predictions.sort_values(["prob"], ascending=[False])

        if os.path.exists("debug.csv"):
            predictions.to_csv("debug.csv", mode='a', header=False)
        else:
            predictions.to_csv("debug.csv", mode='w', header=True)

        predictions = predictions[["prob", 'semantic_type', "column_name"]].T.to_dict().values()

        semantic_type_set = set()

        predictions = sorted(predictions, key=lambda x: x["prob"], reverse=True)

        final_predictions = []

        for prediction in predictions:
            if prediction["semantic_type"] in semantic_type_set:
                continue
            else:
                semantic_type_set.add(prediction["semantic_type"])
                final_predictions.append(prediction)

        return final_predictions

    def save(self, index_name):
        Indexer.store_attribute(self, self.source_name, index_name)

    def update(self, index_name):
        result = Searcher.search_attribute_data_by_name(self.name, self.source_name, index_name)
        if result:
            value_list = result["value_list"]
            for value in value_list:
                self.add_value(value)
            Indexer.delete_attribute(self.name, self.source_name, index_name)
        Indexer.store_attribute(self, self.source_name, index_name)

    def delete(self, index_name):
        Indexer.delete_attribute(self.name, self.source_name, index_name)

    def prepare_data(self):
        if not self.is_prepared:
            self.is_prepared = True
            self.num_fraction = self.num_len * 1.0 / (self.num_len + self.text_len)
            self.frequency_list = sorted(
                [count * 1.0 / len(self.value_list) for count in Counter(self.value_list).values()])
            self.frequency_list = [[idx] * int(math.ceil(freq * 100)) for idx, freq in enumerate(self.frequency_list)]
            self.frequency_list = list(itertools.chain(*self.frequency_list))
            if max(self.frequency_list) > 50:
                self.frequency_list = []
            self.text = " ".join(self.value_list)


