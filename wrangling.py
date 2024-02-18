import pandas as pd

def handling_accuracy(data):
    # Code for handling accuracy
    # formats and types
    
    return data

def imputate_mean(data,column_name):
    """
    Impute the mean of a column into the missing values of that column in a Pandas DataFrame.

    Parameters:
    data (pandas.DataFrame): The DataFrame containing the column to be imputed.
    column_name (str): The name of the column in which missing values are to be imputed with the mean.

    Returns:
    pandas.DataFrame: A new DataFrame with missing values in the specified column imputed with the mean.
    """
     # Calculate the mean of the column
    mean_value = data[column_name].mean()
    
    # Impute the mean into missing values of the column
    data[column_name].fillna(mean_value, inplace=True)
    
    return data


def handling_completeness(data):
    # Code for handling completeness
    # missing columns
    # imputations
    data = imputate_mean(data,'total_acc')

    return data

def handling_integrity(data):
    # Code for handling integrity
    # business rules
    return data

def handling_text(data):
    # Code for handling text
    # harmonisation,business rules, deduplication
    return data

def data_protection(data):
    # Code for data protection
    # anonymisation,data retention
    return data


def wrangling(data):
    data = handling_accuracy(data)
    data = handling_completeness(data)
    data = handling_integrity(data)
    data = handling_text(data)
    data = data_protection(data)
    
    return data

if __name__ == "__main__":
    data = pd.read_csv("dirty-loan-data.csv",low_memory = False)
    wrangling(data)
    data.to_csv("wrangled-loan-data.csv")
