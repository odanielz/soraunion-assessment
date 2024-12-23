# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:22:49 2024

@author: odanielz
"""

import sys
sys.path.append("..")

import pandas as pd
from typing import List, Union
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError
from create_logger import LogActivities
from etl_utilities.etl_utilities import ETLUtilities



load_logger = LogActivities("load", "info")


# The Load class handles writing data to the data warehouse.
# It performs operations for loading dimension data, fact data, 
# and handling any potential errors related to unique constraints in the database. 
class Load(ETLUtilities):


# Initializes the Load object with the database engine and default column mappings.

    def __init__(self, engine):
        
        
        self.engine = engine
        self.dim_tables_list = []
        self.fact_hours_columns = ['client_id', 'project_id', 'role_id', 'task_id', 'date_id', 'total_hours_logged', 'is_billable']
        self.meeting_hours_columns = ['client_id', 'project_id', 'task_id', 'date_id', 'meeting_duration', 'is_billable']
        load_logger.log_message("Beginning Loading Phase...")
    
# Writes the given dataframe to the specified table in the database.

    def write_to_database(self, df: pd.DataFrame, table_name: str):
        
        
        try:
            df.to_sql(table_name, con = self.engine, if_exists = "append", index = False)
        except (IntegrityError, UniqueViolation) as e:
            load_logger.log_message(f"Could not write to table '{table_name}' because columns were not unique")
            print(e)
        
# Writes dimension data to the specified dimension table.
               
    def dim_data(self, df: Union[pd.DataFrame, pd.Series], dim_table: str, dim_column: str = "")-> "Load":
        
        
        load_logger.log_message(f"Writing {dim_column} to Dimension table '{dim_table}'")
        
        if isinstance(df, pd.DataFrame):
            dim_data = df[[dim_column]].drop_duplicates()
            #self.write_series_to_table(dim_data, dim_table, dim_column)
            self.write_to_database(dim_data, dim_table)
            load_logger.log_message(f"Dimension table '{dim_table}' updated successfully")
            self.dim_tables_list.append(dim_table)
            return self
            
        # If the input is a Series, handle it as such and remove duplicates
        dim_data = df.drop_duplicates()
        #self.write_series_to_table(dim_data, dim_table, dim_column)
        self.write_to_database(dim_data, dim_table)
        load_logger.log_message(f"Dimension table '{dim_table}' updated successfully")
        self.dim_tables_list.append(dim_table)
        return self
        
# Writes fact data to the specified fact table, ensuring dimension IDs are mapped correctly.
    
    def fact_data(self, df:pd.DataFrame, fact_table: str):
        

        
        load_logger.log_message(f"Writing to Fact table '{fact_table}'")
        
        # Retrieve dimension data for mapping
        dim_tables_map = ETLUtilities.read_dim_data(self.engine, self.dim_tables_list)
        
        # Map dimension columns to their respective ID columns
        for table_name in self.dim_tables_list:
        
            dim_table = dim_tables_map[table_name]
            id_column = [column for column in dim_table.columns if column.endswith("id")][0]
            dim_column = [column for column in dim_table.columns if not column.endswith("id")][0]
            dim_dict = dim_table.set_index(dim_column)[id_column].to_dict() # Create a dictionary for mapping
            
            # Map the dimension column values to their respective IDs in the fact dataframe
            df[id_column] = df[dim_column].map(dim_dict)
        
        # Select relevant columns for the fact data and write to the database
        fact_df = df[self.fact_hours_columns]
        
        self.write_to_database(fact_df, fact_table)
        
# Writes meeting fact data to the specified fact table, ensuring dimension IDs are mapped correctly, 
# excluding certain dimension tables as specified.       
    def meeting_fact_data(self, df:pd.DataFrame, fact_table: str, unwanted_dim_tables: List):
        
        
        load_logger.log_message(f"Writing to Fact table '{fact_table}'")
        
        # Filter dimension tables to exclude those in the unwanted list
        meeting_dim_tables_list = [table_name for table_name in self.dim_tables_list if table_name not in unwanted_dim_tables]
        
        # Retrieve dimension data for mapping
        dim_tables_map = ETLUtilities.read_dim_data(self.engine, meeting_dim_tables_list)
        
        # Map dimension columns to their respective ID columns
        for table_name in meeting_dim_tables_list:
        
            dim_table = dim_tables_map[table_name]
            id_column = [column for column in dim_table.columns if column.endswith("id")][0]
            dim_column = [column for column in dim_table.columns if not column.endswith("id")][0]
            dim_dict = dim_table.set_index(dim_column)[id_column].to_dict() # Create a dictionary for mapping
            
            # Map the dimension column values to their respective IDs in the fact dataframe
            df[id_column] = df[dim_column].map(dim_dict)
        
        
        # Select relevant columns for the meeting fact data and write to the database
        fact_df = df[self.meeting_hours_columns]
        
        self.write_to_database(fact_df, fact_table)
  