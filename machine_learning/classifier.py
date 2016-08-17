import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
import os

from semantic_labeling import features, DEBUG


class Classifier(object):
    def __init__(self, train_data_map=None):
        self.train_data_map = train_data_map
        self.model = None

    def train(self, data_set_map):
        train_data = []
        for data_name in self.train_data_map.keys():
            data = data_set_map[data_name].generate_training_data(self.train_data_map[data_name])
            train_data += data
        train_data = pd.DataFrame(train_data)
        train_data = train_data.replace([np.inf, -np.inf, np.nan], 0)
        train_data = train_data.drop_duplicates()
        train_data.to_csv("train.csv", mode='w', header=True)

        self.model.fit(train_data[features], train_data["target"])
        if DEBUG:
            self.debug()

    def predict(self, feature_vectors):
        test_df = pd.DataFrame(feature_vectors)
        test_df = test_df.replace([np.inf, -np.inf, np.nan], 0)
        test_df['prob'] = [x[1] for x in self.model.predict_proba(test_df[features])]
        return test_df

    def save(self, file_path):
        joblib.dump(self.model, os.path.join(file_path, type(self).__name__ + ".pkl"))

    def load(self, file_path):
        self.model = joblib.load(file_path)

    def debug(self):
        pass


class RandomForest(Classifier):
    def __init__(self, *args):
        super(RandomForest, self).__init__(*args)
        self.model = RandomForestClassifier(n_estimators=100, class_weight="balanced")

    def debug(self):
        print self.model.feature_importances_


class Regression(Classifier):
    def __init__(self, *args):
        super(Regression, self).__init__(*args)
        self.model = LogisticRegression(class_weight="balanced")

    def debug(self):
        print self.model.coef_
