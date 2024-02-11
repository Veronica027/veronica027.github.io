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
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.impute import MissingIndicator
from sklearn.pipeline import Pipeline
from sklearn.pipeline import FeatureUnion
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


st.markdown(
    """
    <style>
        .yellow-title {
            background-color: lightyellow;
            padding: 10px;
            border-radius: 5px;
            color: black;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Add the title with the yellow background
st.markdown('<h1 class="yellow-title">Home Equity Line of Credit</h1>', unsafe_allow_html=True)

st.header('Application Evaluation')
#st.write('Team Members: Dan Luo, Ying Huang, liqing Huang, Shaozhuo Xu')

df = pd.read_csv('/Users/veronica/Downloads/hw5 team project/heloc_dataset_v1.csv')

####################### Example Customer Blackground information ################
# Create a dictionary with the customer ID and their information
customer_data = {'Customer ID': [111245, 222443, 124123, 46374],
        'ExternalRiskEstimate': [100000, 63, 54, 63],
        'MSinceOldestTradeOpen': [-8, 125, 162, -7],
        'MSinceMostRecentTradeOpen': [100, 2, 7, 18],
        'AverageMInFile': [100, 57, 87, 80],
        'NumSatisfactoryTrades': [10000, 39, 24, 23],
        'NumTrades60Ever2DerogPubRec': [10000, 0, 1, 0],
        'NumTrades90Ever2DerogPubRec': [100, 0, 0, 0],
        'PercentTradesNeverDelq': [100, 100, 85, 83],
        'MSinceMostRecentDelq': [-7, -8, 7, 1],
        'MaxDelq2PublicRecLast12M': [10000, 7, 4, 4],
        'MaxDelqEver': [100, 7, 4, 4],
        'NumTotalTrades': [100, 53, 41, 23],
        'NumTradesOpeninLast12M': [100, 5, 2, 2],
        'PercentInstallTrades': [100, 29, 31, 4],
        'MSinceMostRecentInqexcl7days': [-8, 0, -8, 0],
        'NumInqLast6M': [100, 7, 2, 0],
        'NumInqLast6Mexcl7days': [100000, 6, 2, 0],
        'NetFractionRevolvingBurden': [-8, 69, 65, 15],
        'NetFractionInstallBurden': [100000, 85, 70, -8],
        'NumRevolvingTradesWBalance': [-8, 10, 9, 9],
        'NumInstallTradesWBalance': [10000, -8, 3, 1],
        'NumBank2NatlTradesWHighUtilization': [-8, 7, 3, 2],
        'PercentTradesWBalance': [-8, 71, 80, -90]}

# Create a DataFrame from the dictionary
customer_df = pd.DataFrame(customer_data)

# Create a text input for the user to enter the customer ID
customer_id = st.text_input("Enter Applicant ID:")
customer_id = int(customer_id)

#customer_id = st.selectbox("Select a customer ID", customer_data['Customer ID'])

st.write('Applicant Information:')

# Filter the DataFrame to only display the row that matches the entered customer ID
customer_row = customer_df[customer_df['Customer ID'] == int(customer_id)]


# Check if a row was found, and if so, display it
if len(customer_row) > 0:
    st.write(customer_row)
else:
    st.write("")
####################### Example Customer Blackground information ################


####################### Pipeline #######################

# use pipeline clean the exmaple customer infor 
X = customer_df.iloc[:, 1:24]

X = X.loc[X['ExternalRiskEstimate'] != -9]


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
                     #("scaling", StandardScaler())#
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

df_processed = pd.DataFrame(df_processed, columns=column_names)

####################### Pipeline #######################


####################### Model Import #######################

# refer the best model we have

with open('SVM_model.p', 'rb') as f2:
    loaded_model = pickle.load(f2)

############################################## Weight ###################################################


# Define the feature names and coefficients
feature_names = ['ExternalRiskEstimate', 'MSinceOldestTradeOpen', 'MSinceMostRecentTradeOpen', 'AverageMInFile', 
                 'NumSatisfactoryTrades', 'NumTrades60Ever2DerogPubRec', 'NumTrades90Ever2DerogPubRec', 
                 'PercentTradesNeverDelq', 'MSinceMostRecentDelq', 'MaxDelq2PublicRecLast12M', 'MaxDelqEver', 
                 'NumTotalTrades', 'NumTradesOpeninLast12M', 'PercentInstallTrades', 'MSinceMostRecentInqexcl7days', 
                 'NumInqLast6M', 'NumInqLast6Mexcl7days', 'NetFractionRevolvingBurden', 'NetFractionInstallBurden', 
                 'NumRevolvingTradesWBalance', 'NumInstallTradesWBalance', 'NumBank2NatlTradesWHighUtilization', 
                 'PercentTradesWBalance', 'MSinceMostRecentDelq=-7', 'MSinceMostRecentInqexcl7days=-7', 
                 'MSinceOldestTradeOpen=-8', 'MSinceMostRecentDelq=-8', 'MSinceMostRecentInqexcl7days=-8', 
                 'NetFractionRevolvingBurden=-8', 'NetFractionInstallBurden=-8', 'NumRevolvingTradesWBalance=-8', 
                 'NumInstallTradesWBalance=-8', 'NumBank2NatlTradesWHighUtilization=-8', 'PercentTradesWBalance=-8']
coefficients = [-4.27243403e-02, -3.57347584e-04, 2.73745113e-03, -5.63592547e-03, -2.95286882e-02, 
                -3.57539576e-02, 7.67601117e-02, -1.00448205e-02, -8.88751146e-03, -2.96532750e-02, 
                2.10700800e-02, 9.01434119e-04, 1.71448674e-02, 6.71223086e-03, -4.01132983e-02, 
                3.19007458e-01, -2.65397953e-01, 9.74286823e-03, 1.77813989e-03, 5.91776327e-02, 
                -1.76242593e-02, 8.35200677e-02, -2.75255873e-03, -4.90266370e-01, 1.61716532e-01, 
                -3.49177798e-02, -3.13406530e-03, -8.42887127e-01, -1.69362840e-02, -6.92346488e-02, 
                1.49988449e-01, 1.68861080e-01, 4.02031888e-01, -9.20446347e-01]

# Create the DataFrame
df_coef = pd.DataFrame({'feature': feature_names, 'coefficient': coefficients})
df_coef['weights'] = abs(df_coef['coefficient'] )
sorted_df = df_coef.sort_values(by='weights', ascending=False)
averages = df[df['RiskPerformance']== "Good"].mean()
new_series = averages.to_frame()
new_series = new_series.rename_axis('feature')
merged_df = sorted_df.merge(new_series, on='feature', how='outer')
merged_df = merged_df.rename(columns={0: 'threshold'})
#columns_to_drop = ['Average_for_accpt]
#merged_df = merged_df.drop(columns=columns_to_drop)

description_table = [
    ['PercentTradesWBalance=-8', 'No Usable/Valid Trades or Inquiries for Percent Trades with Balance'],
    ['MSinceMostRecentInqexcl7days=-8', 'No Usable/Valid Trades or Inquiries for Months Since Most Recent Inq excl 7days'],
    ['MSinceMostRecentDelq=-7', 'Condition not Met (e.g. No Inquiries, No Delinquencies) for Months Since Most Recent Delinquency'],
    ['NumBank2NatlTradesWHighUtilization=-8', 'No Usable/Valid Trades or Inquiries for Number Bank/Natl Trades w high utilization ratio'],
    ["ExternalRiskEstimate", "Consolidated version of risk markers"],
    ["MSinceOldestTradeOpen", "Months Since Oldest Trade Open"],
    ["MSinceMostRecentTradeOpen", "Months Since Most Recent Trade Open"],
    ["AverageMInFile", "Average Months in File"],
    ["NumSatisfactoryTrades", "Number Satisfactory Trades"],
    ["NumTrades60Ever2DerogPubRec", "Number Trades 60+ Ever"],
    ["NumTrades90Ever2DerogPubRec", "Number Trades 90+ Ever"],
    ["PercentTradesNeverDelq", "Percent Trades Never Delinquent"],
    ["MSinceMostRecentDelq", "Months Since Most Recent Delinquency"],
    ["MaxDelq2PublicRecLast12M", "Max Delq/Public Records Last 12 Months. See tab 'MaxDelq' for each category"],
    ["MaxDelqEver", "Max Delinquency Ever. See tab 'MaxDelq' for each category"],
    ["NumTotalTrades", "Number of Total Trades (total number of credit accounts)"],
    ["NumTradesOpeninLast12M", "Number of Trades Open in Last 12 Months"],
    ["PercentInstallTrades", "Percent Installment Trades"],
    ["MSinceMostRecentInqexcl7days", "Months Since Most Recent Inq excl 7days"],
    ["NumInqLast6M", "Number of Inq Last 6 Months"],
    ["NumInqLast6Mexcl7days", "Number of Inq Last 6 Months excl 7days. Excluding the last 7 days removes inquiries that are likely due to price comparison shopping."],
    ["NetFractionRevolvingBurden", "Net Fraction Revolving Burden. This is revolving balance divided by credit limit"],
    ["NetFractionInstallBurden", "Net Fraction Installment Burden. This is installment balance divided by original loan amount"],
    ["NumRevolvingTradesWBalance", "Number Revolving Trades with Balance"],
    ["NumInstallTradesWBalance", "Number Installment Trades with Balance"],
    ["NumBank2NatlTradesWHighUtilization", "Number Bank/Natl Trades w high utilization ratio"],
    ["PercentTradesWBalance", "Percent Trades with Balance"]
]



description_table = pd.DataFrame(description_table, columns=['feature', 'Description'])

df_withDescription = merged_df.merge(description_table, on='feature', how='outer')

index_to_extract = customer_df[customer_df['Customer ID'] == customer_id].index

extracted_info = df_processed.loc[index_to_extract]

extracted_info = pd.melt(extracted_info, var_name='feature', value_name='customer_info')

merged_table = pd.merge(df_withDescription, extracted_info, on='feature')

merged_table.loc[:3, 'threshold'] = 0


######################################################################################################
if len(customer_row) > 0:
    x_selected = customer_row.iloc[:, -23:]
    customer_row_flat = np.ravel(x_selected)
    prediction = loaded_model.predict([customer_row_flat])
    st.header('Our Recommendation:')

    if prediction == 1:
        st.write('<div style="text-align: center;"><span style="color:green; font-size: 24px; font-weight: bold;">Accept the Applicantion</span></div>', unsafe_allow_html=True)
        st.write("Based on our prediction model, this application shares **similarities with accepted applications in the past**. This is why we recommend **accepting this application**.")
    else:
        st.write('<div style="text-align: center;"><span style="color:red; font-size: 24px; font-weight: bold;">Decline the Applicantion</span></div>', unsafe_allow_html=True)
        st.write("Based on our prediction model, this application shares **similarities with declined applications in the past**. This is why we recommend **declining this application** at this time.")
        reasons = []
        for index, row in merged_table.iterrows():
            if row['customer_info'] > row['threshold']:
                reasons.append(row['Description'])
            if len(reasons) == 3:
                 break
        st.write("**Top 3 factors that contributed to this decision include:**")
        for reason in reasons:
            st.write("- ",reason)
        st.write("We recommend the applicant improve these factors and reapply in the future.")
    #st.write('The', customer_id,'Customer predicted outcome is:', prediction)   
else:
    st.write("No customer with that information was found.")

#################







