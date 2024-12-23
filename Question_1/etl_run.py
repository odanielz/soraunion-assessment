# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 13:19:39 2024

@author: odanielz
"""


from create_logger import LogActivities
from extraction_files.extract import extract_clickup_data, extract_float_data
from transformation_files.transform import (
    transform_clickup_data,
    transform_float_data,
    retrieve_task_data,
    create_fact_and_meeting_hour_dataframes,
    transform_fact_hours_dataframe,
    transform_meeting_hours_dataframe
    )
from loading_files.load import write_to_dim_table, write_to_fact_table, write_to_meeting_fact_table

# Initialize the ETL logger
etl_logger = LogActivities("etl", "info")


# Extract Phase: Extract data from external source (Google Docs)
etl_logger.log_message("===================ETL PROCESS: EXTRACT =======================")

# Extract data from ClickUp and Float systems
clickup_df = extract_clickup_data()
float_df = extract_float_data()

# Log completion of extraction
etl_logger.log_message("===================EXTRACT COMPLETED ======================= \n\n")



# Transform Phase: Apply transformations to the raw data
etl_logger.log_message("===================ETL PROCESS: TRANSFORM =======================")

# Log transformation of dimension table data
etl_logger.log_message("Creating Dimension table data: transformed_float_df, transformed_clickup_df, and task_df")
transformed_float_df = transform_float_data(float_df)
transformed_clickup_df = transform_clickup_data(clickup_df)
task_df = retrieve_task_data(transformed_clickup_df, transformed_float_df)

etl_logger.log_message("Dimension table data created!")


# Log transformation of fact table data
etl_logger.log_message("Creating Fact table data: transformed_fact_hours, transformed_meeting_hours")
fact_hours, meeting_hours = create_fact_and_meeting_hour_dataframes(transformed_clickup_df, transformed_float_df) 
transformed_fact_hours = transform_fact_hours_dataframe(fact_hours)
transformed_meeting_hours = transform_meeting_hours_dataframe(meeting_hours)

etl_logger.log_message("Fact Table data created!")
etl_logger.log_message("===================TRANSFORM COMPLETED ======================= \n\n")



# Load Phase: Load the transformed data into the data warehouse
etl_logger.log_message("===================ETL PROCESS: LOAD =======================")

# Log loading dimension table data
etl_logger.log_message("Writing to the Dimension Tables")
write_to_dim_table(transformed_float_df, transformed_clickup_df, task_df)

# Log loading fact hours data
etl_logger.log_message("Writing to the Fact Hours Table")
write_to_fact_table(transformed_fact_hours)

# Log loading meeting hours data
etl_logger.log_message("Writing to the Meeting Hours Table")
write_to_meeting_fact_table(transformed_meeting_hours)

etl_logger.log_message("===================LOAD COMPLETED ======================= \n\n")