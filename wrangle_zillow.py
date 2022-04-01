#Acquire and prep function for the zillow dataset - clustering module#

#imports
import numpy as np
import pandas as pd
import os
from env import get_db_url

###################################################################################################

#acquire zillow function
def acquire_zillow():
    '''
    This function checks for a copy of the dataset in the local directory 
    and pulls a new copy and saves it if there is not one,
    it then cleans the data by removing significant outliers then
    removing the rows with null values for 'yearbuilt'
    '''
    #assign the file name
    filename = 'zillow_clustering.csv'
    #check if the file exists in the current directory and read it if it is
    if os.path.exists(filename):
        print('Reading from csv file...')
        #read the local .csv into the notebook
        df = pd.read_csv(filename)
        return df
    #assign the sql query to a variable for use in pulling a new copy of the dataset from the database
    query = '''
    SELECT 
    prop_2017.*,
    log.logerror,
    log.transactiondate,
    airconditioningtype.airconditioningdesc,
    architecturalstyletype.architecturalstyledesc,
    buildingclasstype.buildingclassdesc,
    heatingorsystemtype.heatingorsystemdesc,
    propertylandusetype.propertylandusedesc,
    storytype.storydesc,
    typeconstructiontype.typeconstructiondesc
    FROM properties_2017 AS prop_2017
    JOIN (SELECT parcelid, MAX(transactiondate) AS max FROM predictions_2017 GROUP BY parcelid) AS pred_2017 USING(parcelid)
    LEFT JOIN (SELECT * FROM predictions_2017) AS log ON log.parcelid = pred_2017.parcelid AND log.transactiondate = pred_2017.max
    LEFT JOIN airconditioningtype USING(airconditioningtypeid) 
    LEFT JOIN architecturalstyletype USING(architecturalstyletypeid) 
    LEFT JOIN buildingclasstype USING(buildingclasstypeid) 
    LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid) 
    LEFT JOIN propertylandusetype USING(propertylandusetypeid) 
    LEFT JOIN storytype USING(storytypeid)
    LEFT JOIN typeconstructiontype USING(typeconstructiontypeid)
    WHERE prop_2017.latitude IS NOT NULL;
    '''
    #if needed pull a fresh copy of the dataset from the database
    print('Getting a fresh copy from SQL database...')
    df = pd.read_sql(query, get_db_url('zillow'))
    #save a copy of the dataset to the local directory as a .csv file
    df.to_csv(filename, index=False)
    return df

#############################################################################

#Missing values by column
def missing_col_values(df):
    cols_df = pd.DataFrame({'count' : df.isna().sum(), 'percent' : df.isna().mean()})
    return cols_df

#############################################################################

#missing values by row
def missing_row_values(df):
    rows_df = pd.concat([
    df.isna().sum(axis=1).rename('num_cols_missing'),
    df.isna().mean(axis=1).rename('pct_cols_missing'),
    ], axis=1).value_counts().to_frame(name='num_cols').sort_index().reset_index()
    return rows_df

#############################################################################

#find single unit properties function
def single_unit_properties(df):
    type_values = [261.0, 263.0, 275.0, 265.0]
    df = df[df.propertylandusetypeid.isin(type_values) == True]
    unit_values = [2.0, 3.0]
    df = df[df.unitcnt.isin(unit_values) == False]
    return df

##############################################################################

#remove columns you want to drop
def remove_columns(df, cols_to_remove):  
    df = df.drop(columns=cols_to_remove)
    return df

#handle missing values by missing value percentage by columns then rows
def handle_missing_values(df, prop_required_column = .5, prop_required_row = .75):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

#finish pre prep by dealing with remaining null values
def data_prep(df, cols_to_remove=[], prop_required_column=.5, prop_required_row=.75):
    df = remove_columns(df, cols_to_remove)
    df = handle_missing_values(df, prop_required_column, prop_required_row)
    df['unitcnt'].fillna(1, inplace=True)
    df['structuretaxvaluedollarcnt'].fillna(df.taxvaluedollarcnt - df.landtaxvaluedollarcnt, inplace=True)
    df = df.dropna()
    return df

