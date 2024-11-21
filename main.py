import os
import pandas as pd
import numpy as np

#Working Directory location
FOLDER_PATH  = ("C:/FOLDER/")
FILE_NAME = ("DATA.xlsx")

#
#Joseph Demey
#'analyze batches.py' for Analyzing QC Results of a part, given the location of the DATA output
#Program Class: ETL Script
#November 21, 2024
#Version 01.00
#       Function creates desired output spreadsheets and master sheet
#

#Silence warning
pd.set_option('future.no_silent_downcasting', True)

def load_data():
    '''
    Loads a dataframe from the global values for folder and file
    '''
    df = pd.read_excel(FOLDER_PATH+FILE_NAME)
    return df

def group_by_batch(df):
    '''
    Returns a list of dataframes that represent each batch

    Input: Dataframe
    Return: list of dataframes where each dataframe is one batch    
    '''
    dfs = [group for _, group in df.groupby('Job Number')]
    return dfs

def drop_duplicate_tests(df):
    '''
    Drops duplicate tests from the frame and keeps the "final" value

    Input: Data frame
    Return: Data frame with only the last pass result for each test
    '''
    final_qc = []

    #Seperate the QC Tests into their own dataframes
    dfs = [group for _, group in df.groupby('QC Test')]

    #iterate through those dataframes and determine which is the final pass result given
    for d in dfs:
        max_value = d['Pass'].max()
        final_qc.append(d[d['Pass'] == max_value])
    
    #create the cleaned dataframe by concat of the final passes
    clean_df = pd.concat(final_qc)

    #return the cleaned up df
    return clean_df

def send_to_spreadsheet(dfs):
    '''
    This function will take the qc_test_specs dataframe and send it into an excel formated document, and creates one spreadsheet for each batch additionally
    '''
    for d in dfs:
        batch_name = d.iloc[0,0]
        d.to_excel("C:/FOLDER/OUTPUT" + batch_name + ".xlsx")
    pd.concat(dfs).to_excel("C:/FOLDER/OUTPUT/alldata.xlsx")

#Blank list to hold all final dataframes
batch_dfs = []

#Main functions, load and group
df = load_data()
dfs = group_by_batch(df)

#Iterate through the batches and clean up duplicate tests
for d in dfs:
    new = drop_duplicate_tests(d)
    batch_dfs.append(new)

#Print known troublesome for confirmation of working script
print(batch_dfs[-2])

#Write to files
send_to_spreadsheet(batch_dfs)
