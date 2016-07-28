import csv
import locale
import os
from collections import Counter, defaultdict

import math

import itertools

import re
from string import digits

from search_engine.indexer import Indexer
from search_engine.searcher import Searcher
from semantic_labeling import KS_NUM, JC_NUM, JC_TEXT, MW_HIST, JC_NAME, TF_TEXT, debug_writer, JC_FULL_TEXT, KS_FRAC, \
    KS_LENGTH, EL_DIST, relations, relation_test_map
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
        return Indexer.check_index_exists(self.name)

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
                prediction_map = source.label(labeled_attrs_map, classifier, labeled_sources)
                prediction_map = source.align_semantic_types(prediction_map)
                for attr_name in prediction_map:
                    attr = source.column_map[attr_name]
                    rank = 0
                    count += 1
                    for obj in prediction_map[attr_name]:
                        rank += 1
                        debug_writer.write(source.name + "\t" +
                                           attr.name + "\t" + attr.semantic_type + "\t" + str(prediction) + "\n")
                        if obj["semantic_type"] == attr.semantic_type:
                            break
                    score += 1.0 / rank
                    debug_writer.write(str(score) + "\n")
            mrr_scores[size] = score * 1.0 / count
        return mrr_scores

    def predict_with_different_set(self, classifier, set_name, labeled_sources):
        semantic_type_map = defaultdict(lambda: {})
        for source in self.source_map.values():
            labeled_attrs_map = Searcher.search_columns_data(set_name, labeled_sources)
            for attr in source.attr_map.values():
                if attr.semantic_type and attr.value_list:
                    prediction = attr.predict_type(self.name, labeled_sources, labeled_attrs_map, classifier)[0]
                    semantic_type_map[source.name][attr.semantic_type] = prediction["semantic_type"]
        for file_name in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, file_name)
            source_name = os.path.splitext(file_name)[0]
            with open(file_path, 'r') as f:
                lines = f.readlines()
                for key in semantic_type_map[source_name].keys():
                    lines[0] = lines[0].replace('"%s"' % str(key).translate(None, digits),
                                                semantic_type_map[source_name][key])
            with open(file_path, "w") as f:
                for line in lines:
                    f.write(line)

    def get_labeled_sources(self, idx=0, size=0, ):
        double_list = sorted(self.source_map.keys()) * 2
        labeled_sources = double_list[idx + 1: idx + size + 1]
        labeled_attrs_map = Searcher.search_columns_data(self.name, labeled_sources)
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
                    entity = Entity()
                    for attr_name in reader.fieldnames:
                        entity.add_attribute(attr_name, row[attr_name])
                        self.attr_map[attr_name].add_value(row[attr_name])
                    self.entity_list.append(entity)

    def save(self, index_name):
        self.learn_relation(True)
        for attr in self.attr_map.values():
            if attr.semantic_type and attr.value_list:
                attr.save(index_name)

    def label(self, labeled_attrs_map, classifier, labeled_sources):
        result = {}
        for attr in self.attr_map.values():
            if attr.semantic_type:
                tf_idf_map = Searcher.search_similar_text_data(self.name, attr.text, labeled_sources)
                prediction = attr.predict_type(labeled_attrs_map, tf_idf_map, classifier)
                result[attr.name] = prediction
        return result

    def align_semantic_types(self, prediction_map):
        prediction_map = sorted(prediction_map, key=lambda x: (-len(x), x[0]["prob"] - x[1]["prob"], x[0]["prob"]),
                                reverse=True)

        relation_map = self.learn_relation()

        for key in relation_map.keys():
            for idx1, obj1 in enumerate(prediction_map[key[0]]):
                for idx2, obj2 in enumerate(prediction_map[key[1]]):
                    relation_score = Searcher.get_relation_score(obj1["semantic_type"], obj2["semantic_type"],
                                                                 relation_map[key])
                    prediction_map[key[0]][idx1]["prob"] += (relation_score - 0.5)
                    prediction_map[key[1]][idx2]["prob"] += (relation_score - 0.5)
        return prediction_map

    def learn_relation(self, is_saving=False):
        relation_map = {}
        for idx, attr1 in enumerate(self.attr_map.values()):
            for attr2 in self.attr_map.values()[idx + 1:]:
                for test in relation_test_map.keys():
                    if relation_test_map[test](attr1.value_list, attr2.value_list):
                        flag = True
                        relation_map[(attr1.name, attr2.name)] = test
                    else:
                        flag = False
                        relation_map[(attr1.name, attr2.name)] = None
                    if is_saving:
                        Indexer.index_relation(test, attr1.semantic_type, attr2.semantic_type, flag)
        return relation_map


class Entity:
    def __init__(self, attributes=None):
        if attributes is None:
            attributes = {}
        self.attributes = attributes

    def add_attribute(self, name, value):
        self.attributes[name] = value


class Attribute:
    def __init__(self, name, source_name, semantic_type=None):
        self.name = name
        self.source_name = source_name
        self.semantic_type = semantic_type

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
            self.text_len += len(text)
            # self.text_len += 1
        if num:
            self.numeric_list.append(max([locale.atof(v[0]) for v in num]))
            self.num_len += sum([len(n) for n in num])
            # self.num_len += 1

        self.value_list.append(value)

    def to_json(self):
        self.prepare_data()
        json_obj = {"name": self.name, "source_name": self.source_name, "semantic_type": self.semantic_type,
                    "num_fraction": self.num_fraction, "value_list": self.value_list, KS_NUM: self.numeric_list,
                    JC_NUM: self.numeric_list, JC_TEXT: list(set(self.textual_list)), MW_HIST: self.frequency_list,
                    EL_DIST: self.numeric_list, JC_NAME: self.name, TF_TEXT: self.text,}
        return json_obj

    def compute_features(self, index_name, labeled_sources, labeled_attrs_map):
        self.prepare_data()
        tf_idf_map = Searcher.search_similar_text_data(index_name, self.text, labeled_sources)
        feature_vectors = compute_feature_vectors(labeled_attrs_map, self.to_json(), tf_idf_map)
        return feature_vectors

    def predict_type(self, index_name, labeled_sources, labeled_attrs_map, classifier, confidence_threshold=0.2):
        feature_vectors = self.compute_features(index_name, labeled_sources, labeled_attrs_map)
        predictions = classifier.predict(feature_vectors)

        predictions = predictions.sort_values(["prob"], ascending=[False])

        if os.path.exists("debug.csv"):
            predictions.to_csv("debug.csv", mode='a', header=False)
        else:
            predictions.to_csv("debug.csv", mode='w', header=True)

        predictions = predictions[["prob", 'semantic_type', "column_name"]].T.to_dict().values()

        semantic_type_set = set()

        predictions = sorted(predictions, key=lambda x: x["prob"], reverse=True)

        for prediction in predictions:
            if prediction["semantic_type"] in semantic_type_set or prediction["prob"] < confidence_threshold:
                predictions.remove(prediction)
            else:
                semantic_type_set.add(prediction["semantic_type"])

        return predictions

    def save(self, index_name):
        Indexer.index_column(self, self.source_name, index_name)

    def update(self, index_name):
        result = Searcher.search_column_data_by_name(self.name, self.source_name, index_name)
        if result:
            value_list = result["value_list"]
            for value in value_list:
                self.add_value(value)
            Indexer.delete_column(self.name, self.source_name, index_name)
        Indexer.index_column(self, self.source_name, index_name)

    def delete(self, index_name):
        Indexer.delete_column(self.name, self.source_name, index_name)

    def prepare_data(self):
        if not self.is_prepared:
            self.is_prepared = True
            self.num_fraction = self.num_len * 1.0 / (self.num_len + self.text_len)
            self.frequency_list = sorted(
                [count * 1.0 / len(self.value_list) for count in Counter(self.value_list).values()])
            self.frequency_list = [[idx] * int(math.ceil(freq * 100)) for idx, freq in enumerate(self.frequency_list)]
            self.frequency_list = list(itertools.chain(*self.frequency_list))
            # if max(self.frequency_list) > 50:
            #     self.frequency_list = []
            self.text = " ".join(self.value_list)


class TimeSeriesAttribute(Attribute):
    def __init__(self, *args):
        super(Attribute, self).__init__(*args)

        self.time_series_list = []

    def add_value(self, value, time):
        timed_value = (time, value)
        super(Attribute, self).add_value(value)

        self.time_series_list.append(timed_value)

    def prepare_data(self):
        super(Attribute, self).prepare_data()
