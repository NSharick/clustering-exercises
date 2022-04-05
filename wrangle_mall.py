## Functions for wrangling the mall customer dataset - clustering exercises

import numpy as np
import pandas as pd
import os
from env import get_db_url
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

#acquire the dataset 
def acquire_mall_customers():
    '''
    This function checks for a copy of the dataset in the local directory 
    and pulls a new copy and saves it if there is not one,
    it then cleans the data by removing significant outliers then
    removing the rows with null values for 'yearbuilt'
    '''
    #assign the file name
    filename = 'mall_clustering.csv'
    #check if the file exists in the current directory and read it if it is
    if os.path.exists(filename):
        print('Reading from csv file...')
        #read the local .csv into the notebook
        df = pd.read_csv(filename)
        return df
    #assign the sql query to a variable for use in pulling a new copy of the dataset from the database
    query = '''
    SELECT * FROM customers;
    '''
    #if needed pull a fresh copy of the dataset from the database
    print('Getting a fresh copy from SQL database...')
    df = pd.read_sql(query, get_db_url('mall_customers'))
    #save a copy of the dataset to the local directory as a .csv file
    df.to_csv(filename, index=False)
    return df

#remove outiers
def remove_outliers(df, k, col_list):
    ''' this function will remove outliers from a list of columns in a dataframe 
        and return that dataframe. A list of columns with significant outliers is 
        assigned to a variable in the below wrangle function and can be modified if needed
    '''
    #loop throught the columns in the list
    for col in col_list:
        q1, q3 = df[col].quantile([.25, .75])  # get quartiles
        iqr = q3 - q1   # calculate interquartile range
        upper_bound = q3 + k * iqr   # get upper bound
        lower_bound = q1 - k * iqr   # get lower bound
        # return dataframe without outliers
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)] 
    return df

#split the data
def split_data(df):
    '''
    this function takes the full dataset and splits it into three parts (train, validate, test) 
    and returns the resulting dataframes
    '''
    train_val, test = train_test_split(df, train_size = 0.8, random_state=123)
    train, validate = train_test_split(train_val, train_size = 0.7, random_state=123)
    return train, validate, test

#encode the categorical columns
def encode_cats(df):
    encode_cols = [col for col in df.columns if df[col].dtype == 'O']
    for col in encode_cols:
        dummie_df = pd.get_dummies(df[col], prefix = df[col].name, drop_first = True)
        df = pd.concat([df, dummie_df], axis=1)
    return df

#scale the data
def scale_data(df):
    scaler = MinMaxScaler()
    scaler.fit(df)
    scaled_df = scaler.transform(df)
    df = pd.DataFrame(scaled_df, columns=df.columns, index=df.index)
    return df

