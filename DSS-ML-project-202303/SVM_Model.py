import streamlit as st
from sklearn.datasets import load_iris
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
import pandas as pd 
from IPython.core.pylabtools import figsize
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.sparse import csr_matrix
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor
from sklearn import neighbors                           
from sklearn import tree, linear_model                         
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis 
from sklearn import datasets

from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import cross_validate
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import recall_score
from sklearn.metrics import mean_squared_error
from sklearn import metrics
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
import joblib
import pickle

df = pd.read_csv('/Users/veronica/Downloads/heloc_dataset_v1.csv')

# Convert the values in Y to 1 and 0 for the labels "Bad" and "Good"
X = df.iloc[:, 1:24]
Y = df['RiskPerformance'].replace({'Bad': 1, 'Good': 0})

# Remove any observation where ExternalRiskEstimate is -9 from X
X = X.loc[X['ExternalRiskEstimate'] != -9]
Y = Y[X.index]


#pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.impute import MissingIndicator
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion

do_nothing_imputer = ColumnTransformer([("Imputer -7 to mean", SimpleImputer(missing_values=-7, strategy='mean'), [])], remainder='passthrough')

feature_expansion = FeatureUnion([("do nothing", do_nothing_imputer),
                                  ("add features for -7", MissingIndicator(missing_values=-7, features='missing-only')),
                                  ("add features for -8", MissingIndicator(missing_values=-8, features='missing-only'))])
 
from sklearn.preprocessing import MinMaxScaler
# Min-Max Scaling 
#scaler = MinMaxScaler()
#scaled_values = scaler.fit_transform(X_train_t)
# include standlization using Standard Scaler

pipeline = Pipeline([("replace -7 with -8", SimpleImputer(missing_values=-7, strategy='constant', fill_value=-8)),
                     ("replace -8 with mean", SimpleImputer(missing_values=-8, strategy='most_frequent')),
                     ("scaling", StandardScaler())
                    ])


X1 = X.copy()
X1 = feature_expansion.fit_transform(X1)
X_processed = pipeline.fit_transform(X1[:, :23])

minus_7_indicator_transformer_X = MissingIndicator(missing_values=-7, features='missing-only').fit(X)
minus_8_indicator_transformer_X = MissingIndicator(missing_values=-8, features='missing-only').fit(X)

col_names_minus_7_Xtrain = X.columns.values[minus_7_indicator_transformer_X.features_].tolist() 
col_names_minus_7_Xtrain = list(map(lambda s:str(s)+'=-7',col_names_minus_7_Xtrain)) 
col_names_minus_8_Xtrain = X.columns.values[minus_8_indicator_transformer_X.features_].tolist() 
col_names_minus_8_Xtrain = list(map(lambda s:str(s)+'=-8',col_names_minus_8_Xtrain))
column_names = X.columns.values.tolist() + col_names_minus_7_Xtrain + col_names_minus_8_Xtrain

df_processed = np.concatenate((X_processed, X1[:,23:]), axis=1)

# convert the result back to a pandas data frame
df_processed = pd.DataFrame(df_processed, columns=column_names)

# Split data
from sklearn.model_selection import train_test_split

X_train, X_test = train_test_split(X, test_size=0.2, random_state=10)
Y_train, Y_test = train_test_split(Y, test_size=0.2, random_state=10)

X_train_t_tr, X_train_t_val, Y_train_t_tr, Y_train_t_val = train_test_split(X_train, Y_train, test_size=0.25, random_state=1234)

# training data

#log_reg = LogisticRegression(max_iter=10000, random_state=0).fit(X_train_t_tr, Y_train_t_tr)
clf_SVM = svm.SVC(kernel='linear').fit(X_train_t_tr, Y_train_t_tr)
