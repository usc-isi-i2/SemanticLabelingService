from data_source.data_source import DataSet
from machine_learning.classifier import Regression, RandomForest


class SemanticLabeler:
    def __init__(self, option, classifier_training_sets, labeling_sets):
        self.classifier_training_sets = classifier_training_sets
        self.labeling_sets = labeling_sets
        self.classifier = None
        self.data_set_map = {}
        self.option = option

    def train_classifier(self):
        if self.option == "LOGISTIC":
            self.classifier = Regression(self.classifier_training_sets)
        else:
            self.classifier = RandomForest(self.classifier_training_sets)
        self.classifier.train(self.data_set_map)
        self.classifier.debug()

    def store_data_sets(self):
        set_names = list(set(self.labeling_sets.keys()) | set(self.classifier_training_sets.keys()))
        for name in set_names:
            data_set = DataSet(name)
            data_set.read("data/%s" % name)
            self.data_set_map[name] = data_set

    def label_sources(self):
        for name in self.labeling_sets:
            sizes = self.labeling_sets[name]
            data_set = self.data_set_map[name]
            print data_set.test(sizes, self.classifier)


if __name__ == "__main__":
    labeler = SemanticLabeler("LOGISTIC", {"soccer": [1, 2, 3, 4, 5]}, {"weapon": [1]})
    labeler.store_data_sets()
    labeler.train_classifier()
    labeler.label_sources()
