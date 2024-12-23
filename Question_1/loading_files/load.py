# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 13:01:06 2024

@author: odanielz
"""

import sys
sys.path.append("..")

from loading_files.load_tools import Load, pd
from config_files.config import engine

from config_files.config import DimTableNames as dtn
from config_files.config import FactTableNames as ftn


dw_loader = Load(engine)

#    Writes dimension data to the respective dimension tables in the data warehouse.

def write_to_dim_table(transformed_float_df:pd.DataFrame, 
                       transformed_clickup_df: pd.DataFrame,
                       task_df: pd.DataFrame
                       )-> None:
    
    """
    Writes dimension data to the respective dimension tables in the data warehouse.
    
    This method will:
    - Write transformed Float data to client, project, and role dimension tables.
    - Write transformed ClickUp data to the date dimension table.
    - Write task data to the task dimension table.
    """    
    
    dw_loader.dim_data(transformed_float_df, dtn.CLIENT.value, "client_name", ) \
                .dim_data(transformed_float_df, dtn.PROJECT.value, "project_name", ) \
                .dim_data(transformed_float_df, dtn.ROLE.value, "role_name", ) \
                .dim_data(transformed_clickup_df, dtn.DATE.value, "date") \
                .dim_data(task_df, dtn.TASK.value)
                

def write_to_fact_table(transformed_fact_hours: pd.DataFrame):
    
    """
    Writes the transformed fact data related to hours to the fact table in the data warehouse.

    This method will:
    - Write the transformed fact hours data (including client, project, role, task, date, etc.)
      to the fact table that contains total hours logged and whether the hours are billable.
    """
    
    dw_loader.fact_data(transformed_fact_hours, ftn.HOURS.value)

#     Writes the transformed meeting hours data to the meeting fact table in the data warehouse.
def write_to_meeting_fact_table(transformed_meeting_hours: pd.DataFrame):
