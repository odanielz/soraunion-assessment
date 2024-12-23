# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:39:34 2024

@author: odanielz
"""

import sys
sys.path.append("..")

from transformation_files.transform_tools import Transform, pd
from etl_utilities.etl_utilities import ETLUtilities, Tuple


def transform_float_data(float_df)-> pd.DataFrame:
    
    # Transforms the Float data by applying a series of transformations:
    # - Checking for duplicate rows
    # - Checking for null values
    # - Adding suffixes to specified columns.
    
    # Initialize the Transform object for Float data
    float_transformer = Transform(df = float_df, df_name = "Float_DataFrame")
    
    # Apply transformations: check duplicates, check nulls, and add suffix to column names
    float_transformer.check_duplicate_rows() \
                        .check_null_values() \
                        .add_suffix_to_column_names("Client", "Project", "Role", "Task")
    
    # Get the transformed dataframe
    transformed_float_df = float_transformer.get_transformed_dataframe
    
    return transformed_float_df


def transform_clickup_data(clickup_df)-> pd.DataFrame:
    

    
    # Initialize the Transform object for ClickUp data
    clickup_transformer = Transform(df = clickup_df, df_name = "Clickup DataFrame")
    
    # Apply transformations: parse date column, rename column, and add suffix to column names
    clickup_transformer.parse_date_column("Date") \
                        .rename_column("Date", "date") \
                        .add_suffix_to_column_names("Client", "Project", "Role", "Task")
    
    # Retrieve the transformed dataframe after applying all transformations
    transformed_clickup_df = clickup_transformer.get_transformed_dataframe
    
    return transformed_clickup_df



def retrieve_task_data(transformed_clickup_df: pd.DataFrame, 
                       transformed_float_df: pd.DataFrame
                       )-> pd.DataFrame:
    

    
    # Concatenate both dataframes on the 'task_name' column
    return ETLUtilities.concatenate_dataframes(transformed_clickup_df, transformed_float_df, "task_name")

                                  
def create_fact_and_meeting_hour_dataframes(transformed_clickup_df: pd.DataFrame, 
                                            transformed_float_df: pd.DataFrame
                                            )-> Tuple[pd.DataFrame]:
    

    
    # Merge the dataframes on multiple common columns to create a fact_hours dataframe
    fact_hours = ETLUtilities.merge_dataframes(transformed_clickup_df, transformed_float_df, ["client_name", "project_name", "Name", "task_name"])
    
    # Break the fact_hours dataframe into two: fact_hours and meeting_hours based on task_name
    fact_hours, meeting_hours = ETLUtilities.breakup_dataframe(fact_hours, "task_name", "project meeting")
    
    return fact_hours, meeting_hours


def transform_fact_hours_dataframe(fact_hours: pd.DataFrame)-> pd.DataFrame:
    
    """
    Transforms the fact_hours dataframe by renaming specific columns.
    This is used to prepare the fact_hours data for loading into the DataWarehouse.
    
    Args:
        fact_hours (pd.DataFrame): The dataframe containing the fact hours data.
    
    Returns:
        pd.DataFrame: The transformed fact_hours dataframe with renamed columns.
    """
    
    # Initialize the Transform object for the fact_hours dataframe
    fact_transformer = Transform(df = fact_hours, df_name = "Fact_Hours_DataFrame")
    
    # Rename the 'Hours' column to 'total_hours_logged' and 'Billable' to 'is_billable'
    fact_transformer.rename_column("Hours", "total_hours_logged") \
                    .rename_column("Billable", "is_billable")
    
    # Retrieve the transformed dataframe after applying the renaming transformations
    transformed_fact_hours = fact_transformer.get_transformed_dataframe                
    
    return transformed_fact_hours


def transform_meeting_hours_dataframe(meeting_hours: pd.DataFrame)-> pd.DataFrame:
    
    """
    Transforms the meeting_hours dataframe by renaming specific columns.
    This is used to prepare the meeting_hours data for loading into the DataWarehouse.
    
    Args:
        meeting_hours (pd.DataFrame): The dataframe containing the meeting hours data.
    
    Returns:
        pd.DataFrame: The transformed meeting_hours dataframe with renamed columns.
    """
    
    meeting_transformer = Transform(df = meeting_hours, df_name = "Meeting_Hours_DataFrame")
    meeting_transformer.rename_column("Hours", "meeting_duration") \
                    .rename_column("Billable", "is_billable")
    
    transformed_meeting_hours = meeting_transformer.get_transformed_dataframe
    
    return transformed_meeting_hours
