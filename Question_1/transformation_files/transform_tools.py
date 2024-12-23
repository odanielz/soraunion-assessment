# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:27:26 2024

@author: odanielz
"""

import sys
sys.path.append("..")

import pandas as pd
from typing import List, Union
from create_logger import LogActivities


# Initialize a logger for transformation operations
transform_logger = LogActivities("transform", "info")


    # A class responsible for applying various transformations to a DataFrame. 
    # The transformations include checking for duplicate rows, null values, 
    # renaming columns, and combining data from multiple data sources.
class Transform:
    

    
    def __init__(self, df, df_name: str = None):
        

        
        self._df = df
        self.df_name = df_name
        transform_logger.log_message(f"Beginning Transform Phase for {self.df_name}..." if self.df_name else "Beginning Transform Phase for DataFrame")

#        Checks for and removes duplicate rows from the DataFrame.

    def check_duplicate_rows(self)-> pd.DataFrame:
        

        
        transform_logger.log_message("Checking for duplicate rows in DataFrame")
        
        # Count the number of duplicated rows
        num_duplicates = self._df.duplicated().sum().item()
        
        transform_logger.log_message(f"{num_duplicates} duplicate rows identified")
        transform_logger.log_message("" if not num_duplicates else f"Dropping the {num_duplicates} duplicate rows")
        
        # Drop duplicate rows
        self._df = self._df.drop_duplicates()
        transform_logger.log_message("Duplicate Rows Check Complete for DataFrame")
    
        return self
    
        # Checks for null values in the DataFrame, logs the null values breakdown by column, 
        # and removes rows with null values.   
    def check_null_values(self)-> "Transform":
        

        
        transform_logger.log_message("Checking for null values in DataFrame")
        
        # Count total null values in the DataFrame and breakdown by column
        num_nulls = self._df.isnull().sum().sum().item()
        column_nulls = self._df.isnull().sum()
        
        transform_logger.log_message(f"{num_nulls} null values identified")
        transform_logger.log_message(f"Column Null Values Breakdown:\n{column_nulls}")
        
        transform_logger.log_message("Dropping rows with nulls")
        
        # Drop rows with null values
        self._df = self._df.dropna()
        
        return self
    
    
    def add_suffix_to_column_names(self, *column_names, suffix = "name")-> "Transform":
        

        
        transform_logger.log_message(f"Adding suffix '{suffix}' to the selected column names")
        for column_name in column_names:
            
            # Rename columns by adding the suffix
            self._df = self._df.rename(columns = {column_name: f"{column_name.lower()}_{suffix}"})
        
        transform_logger.log_message(f"New DataFrame columns {self._df.columns}")
        
        return self
    
    
    def rename_column(self, column_name: str, new_name: str)-> "Transform":

        
        transform_logger.log_message(f"Renaming column '{column_name}' to '{new_name}'")
        self._df.rename(columns={column_name: new_name}, inplace = True)
        return self
    
    
    def parse_date_column(self, date_column: str)-> "Transform":
        
        
        transform_logger.log_message(f"Convert '{date_column}' to Datetime column type")
        self._df[date_column] = pd.to_datetime(self._df[date_column])
        return self
        
    @classmethod
    def combine_dataframes(cls, 
                         dataframe_1: pd.DataFrame, 
                         dataframe_2: pd.DataFrame, 
                         merge_columns: List = [], 
                         merge_method: str = "left",
                         pandas_method: str = "merge"
                         )-> Union[pd.DataFrame, pd.Series]:
        
        
        if pandas_method == "concat":
            return pd.concat([dataframe_1["task_name"], dataframe_2["task_name"]])
        
        return dataframe_1.merge(dataframe_2, on = merge_columns, how = merge_method)
    
    
    @property
    def get_transformed_dataframe(self):
        
        
        return self._df
        
