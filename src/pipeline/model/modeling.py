
import logging
import pickle as pkl
import importlib
import numpy as np
import pandas as pd

def matrix_splitter(df):
    # takes in a matrix from the matrix creator and splits it into features (df) and labels (list)
    drop_cols = ['cohort_date', 'person_id', 'label']
    feature_matrix = df.drop(columns=drop_cols)
    labels = df['label']
    return (feature_matrix, labels)


class Model:
    def __init__(self, model_type_name, model_hyperparams, random_seed, train_matrix, val_matrix):

        # basic model parameters
        self.model_type_name = model_type_name
        self.model_hyperparams = model_hyperparams
        self.train_matrix = train_matrix
        self.val_matrix = val_matrix

        # Import the specific module neede to run this model
        # note: maxsplit splits once on the final '.'
        module_name, class_name = self.model_type_name.rsplit(".", maxsplit = 1) 
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        if 'pytorch_tabnet' in self.model_type_name:
            self.model_hyperparams['optimizer_params']['lr'] = np.float(self.model_hyperparams['optimizer_params']['lr'])
            self.model = cls(**model_hyperparams)
        else: 
            self.model = cls(random_state = random_seed, **model_hyperparams)

    def train(self, train_matrix):
        train_feature_matrix, train_labels = matrix_splitter(train_matrix)
        self.feature_names = list(train_feature_matrix.columns)
        if 'pytorch_tabnet' in self.model_type_name:
            logging.info("Training a tabnet... let's see how this goes")
            val_feature_matrix, val_labels = matrix_splitter(self.val_matrix)
            self.model.fit(X_train = train_feature_matrix.values, y_train = train_labels.values.reshape(-1,1), eval_set=[(val_feature_matrix.values, val_labels.values.reshape(-1,1))])
        else:
            self.model = self.model.fit(train_feature_matrix, train_labels)
        return self

    def get_feature_importances(self):
        if self.model_type_name == 'triage.component.catwalk.estimators.classifiers.ScaledLogisticRegression':
            self.feature_importances = self.model.coef_.squeeze()
        else:
            self.feature_importances = self.model.feature_importances_
        feature_importances_dict = {k:v for k,v in zip(self.feature_names, self.feature_importances)}
        return feature_importances_dict
        
    def predict_score(self, val_matrix):
        feature_matrix, _ = matrix_splitter(val_matrix)
        # Note: scikitlearn predict_proba returns probability for each class [0, 1] 
        if 'pytorch_tabnet' in self.model_type_name:
            self.scores = self.model.predict(feature_matrix.values).squeeze()
        else:
            self.scores = self.model.predict_proba(feature_matrix)[:, 1]
        return self.scores

    def clean_up_time(self):
        delattr(self, 'train_matrix')
        delattr(self, 'val_matrix')
        return self

class Baseline:
    def __init__(self, random_state, feature_list, ascending_list):

        # Basic model parameters
        self.random_state = random_state
        self.feature_list = feature_list
        self.ascending_list = ascending_list
        
    def fit(self, X, y):
        # Returns equal importances for each feature included in feature_list
        all_features = list(X.columns)
        num_features = len(self.feature_list)
        feat_importance = 1/num_features
        feat_in_list = [feat in self.feature_list for feat in all_features]
        self.feature_importances_ = np.where(feat_in_list, feat_importance, 0)
        return self


    def predict_proba(self, X):
        # Set seed
        np.random.seed(self.random_state)

        # Append the random number column to the feature list
        full_feature_list = self.feature_list + ['random_num']
        full_ascending_list = self.ascending_list + [True]

        # Populate the random_num column with random uniform draws, sort X
        X['random_num'] = np.random.uniform(0, 1, X.shape[0])
        X = X.sort_values(by = full_feature_list, axis = 0, ascending = full_ascending_list)

        # Add ranking and ranking method column to the features matrix (tiebreaking = random)
        X['rank'] = range(X['random_num'].shape[0])

        # Reset index
        X = X.sort_index()
    
        # Take out random_num column
        X = X.drop('random_num', axis = 1)

        # Add probability columns based on the ranking of features, convert to numpy array
        X['label0'] = (X['rank'] / (X.shape[0] - 1))
        X['label1'] = 1 - X['label0']
        r = X[['label0', 'label1']].to_numpy()

        return r

     
    def __repr__(self):
        return("You printed me!!")
        pass