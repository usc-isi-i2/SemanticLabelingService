import csv
import locale
import os
from collections import Counter

import math

import itertools

from search_engine.indexer import Indexer
from search_engine.searcher import Searcher
from semantic_labeling import KS_NUM, JC_NUM, JC_TEXT, MW_HIST, JC_NAME, TF_TEXT, debug_writer, JC_FULL_TEXT
from semantic_labeling.feature_computing import compute_feature_vectors
from utils.helpers import split_number_text


class DataSet:
    def __init__(self, name):
        self.name = name
        self.source_map = {}

    def read(self, folder_path):
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
            for idx, source in enumerate(self.source_map.values()):
                labeled_sources, labeled_attrs_map = self.get_labeled_sources(idx, size)
                for attr in source.attr_map.values():
                    if attr.semantic_type and attr.value_list:
                        predictions = attr.predict_type(self.name, labeled_sources, labeled_attrs_map, classifier)
                        rank = 0
                        count += 1
                        for prediction in predictions:
                            rank += 1
                            debug_writer.write(
                                attr.name + "\t" + attr.semantic_type + "\t" + str(prediction) + "\n")
                            if prediction["semantic_type"] == attr.semantic_type:
                                break
                        score += 1.0 / rank
                        debug_writer.write(str(score) + "\n")
            mrr_scores[size] = score * 1.0 / count
        return mrr_scores

    def get_labeled_sources(self, idx, size):
        double_list = self.source_map.keys() * 2
        labeled_sources = double_list[idx + 1: idx + size + 1]
        labeled_attrs_map = Searcher.search_columns_data(self.name, labeled_sources)
        return labeled_sources, labeled_attrs_map

    def generate_training_data(self, size_list):
        train_data = []
        for size in size_list:
            for idx, source in enumerate(self.source_map.values()):
                labeled_sources, labeled_attrs_map = self.get_labeled_sources(idx, size)
                for attr in source.attr_map.values():
                    if attr.semantic_type:
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
        for attr in self.attr_map.values():
            if attr.semantic_type and attr.value_list:
                attr.save(self.name, index_name)

    def label(self, labeled_attrs_map, classifier, labeled_sources):
        result = {}
        for attr in self.attr_map.values():
            if attr.semantic_type:
                tf_idf_map = Searcher.search_similar_text_data(self.name, attr.text, labeled_sources)
                predictions = attr.predict_type(labeled_attrs_map, tf_idf_map, classifier)
                result[attr.name] = predictions
        return result


class Entity:
    def __init__(self, attributes=None):
        if attributes is None:
            attributes = {}
        self.attributes = attributes

    def add_attribute(self, name, value):
        self.attributes[name] = value


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
        self.fingerprint_list = []

        self.is_prepared = False
        self.num_fraction = 0

    def add_value(self, value):
        value = value.strip()

        if not value:
            return

        try:
            value = value.encode("ascii", "ignore")
        except:
            value = value.decode("unicode_escape").encode("ascii", "ignore")

        num, text = split_number_text(value)

        if text:
            self.textual_list.append(text)
        if num:
            self.numeric_list.append(max([locale.atof(v[0]) for v in num]))

        self.value_list.append(value)

    def to_json(self):
        self.prepare_data()
        json_obj = {"name": self.name, "source_name": self.source_name, "semantic_type": self.semantic_type,
                    "num_fraction": self.num_fraction, KS_NUM: self.numeric_list, JC_NUM: self.numeric_list,
                    JC_TEXT: self.textual_list, JC_FULL_TEXT: self.text, MW_HIST: self.frequency_list,
                    JC_NAME: self.name, TF_TEXT: self.text}
        return json_obj

    def compute_features(self, index_name, labeled_sources, labeled_attrs_map):
        self.prepare_data()
        tf_idf_map = Searcher.search_similar_text_data(index_name, self.text, labeled_sources)
        feature_vectors = compute_feature_vectors(labeled_attrs_map, self.to_json(), tf_idf_map)
        return feature_vectors

    def predict_type(self, index_name, labeled_sources, labeled_attrs_map, classifier):
        feature_vectors = self.compute_features(index_name, labeled_sources, labeled_attrs_map)
        predictions = classifier.predict(feature_vectors)

        predictions = predictions.sort_values(["prob"], ascending=[False])

        if os.path.exists("debug.csv"):
            predictions.to_csv("debug.csv", mode='a', header=False)
        else:
            predictions.to_csv("debug.csv", mode='w', header=True)

        predictions = predictions[["prob", 'semantic_type']].T.to_dict().values()

        semantic_type_set = set()

        predictions = sorted(predictions, key=lambda x: x["prob"], reverse=True)

        for prediction in predictions:
            if prediction["semantic_type"] in semantic_type_set:
                predictions.remove(prediction)
            else:
                semantic_type_set.add(prediction["semantic_type"])

        return predictions

    def save(self, source_name, index_name):
        Indexer.index_column(self, source_name, index_name)

    def prepare_data(self):
        if not self.is_prepared:
            self.is_prepared = True
            self.num_fraction = len(self.numeric_list) * 1.0 / (len(self.numeric_list) + len(self.textual_list))
            self.frequency_list = sorted(
                [count * 1.0 / len(self.value_list) for count in Counter(self.value_list).values()])
            self.frequency_list = [[idx] * int(math.ceil(freq * 100)) for idx, freq in enumerate(self.frequency_list)]
            self.frequency_list = list(itertools.chain(*self.frequency_list))
            self.text = " ".join(self.textual_list)


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
