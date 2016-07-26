import random

from data_source.data_source import DataSet
from machine_learning.classifier import Regression, RandomForest


class SemanticLabeler:
    def __init__(self, option):
        self.classifier = None
        self.data_set_map = {}
        self.option = option

    def train_classifier(self, classifier_training_sets):
        if self.option == "LOGISTIC":
            self.classifier = Regression(classifier_training_sets)
        else:
            self.classifier = RandomForest(classifier_training_sets)
        self.classifier.train(self.data_set_map)
        self.classifier.debug()

    def store_data_sets(self, set_names):
        for name in set_names:
            data_set = DataSet(name)
            data_set.read("data/%s" % name)
            self.data_set_map[name] = data_set

    def test_semantic_typing1(self, labeling_sets):
        with open("result.txt", "a") as f:
            for name in labeling_sets:
                sizes = labeling_sets[name]
                data_set = self.data_set_map[name]
                f.write(name + "\t")
                f.write(str(data_set.test(sizes, self.classifier)) + "\n")

    def test_semantic_typing2(self, labeled_set, labeling_set):
        labeled_sources = self.data_set_map[labeled_set].source_map.keys()
        data_set = self.data_set_map[labeling_set]
        data_set.predict_with_different_set(self.classifier, labeled_set, labeled_sources)


if __name__ == "__main__":
    labeler = SemanticLabeler("LOGISTIC")
    labeler.store_data_sets(["soccer2", "weather2"])
    # sizes = random.sample(range([1, 2, 3, 4, 5]), 2)
    labeler.train_classifier({"soccer2": [1]})
    labeler.test_semantic_typing1(
        {"weather2": [1]})
    # , "museum2": [1, 2, 3, 4, 5, 14], "weather_old2": [1, 2, 3], "soccer2": [1, 2, 3, 4, 5]
