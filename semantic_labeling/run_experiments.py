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
        self.classifier.save("learned_models")

    def store_data_sets(self, set_names):
        for name in set_names:
            data_set = DataSet(name)
            data_set.read("data/%s" % name)
            self.data_set_map[name] = data_set

    def test_semantic_typing(self, labeling_sets):
        with open("result.txt", "a") as f:
            for name in labeling_sets:
                sizes = labeling_sets[name]
                data_set = self.data_set_map[name]
                f.write(name + "\t")
                f.write(str(data_set.test(sizes, self.classifier)) + "\n")


if __name__ == "__main__":
    labeler = SemanticLabeler("LOGISTIC")
    labeler.store_data_sets(["museum2", "soccer2"])
    labeler.train_classifier({"soccer2": [1]})
    labeler.test_semantic_typing({"soccer2": [10]})
