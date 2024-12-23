# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:21:02 2024

@author: odanielz
"""


import pandas as pd
from typing import List
import sys
sys.path.append("..")
from create_logger import LogActivities


# Initialize the logger for the extraction process
extract_logger = LogActivities("extract", "info")


# A class responsible for handling the extraction phase of ETL, which includes:
# Validating links to data sources
# Reading data from the source into a pandas DataFrame
class Extract:
    

    
    def __init__(self, file_format: str = "csv", filename: str = None):
        

        
        self.file_format = file_format
        self.filename = filename
        extract_logger.log_message(f"Beginning Extract Phase for {self.filename}..." if self.filename else f"Beginning Extract Phase for {self.file_format} file...")

# Validates a single file link by converting it to the appropriate format for downloading.

    def validate_link(self, file_link: str, file_format: str = "csv")-> str:
        
        
        extract_logger.log_message(f"Validating link to {file_format} file")
        
        # Modify the link to specify the export format (e.g., converting "edit?" to "export?")
        validated_file_link = file_link.replace("edit?", f"export?format={file_format}&")
        extract_logger.log_message("Link Validation Complete")
        return validated_file_link


# Validates multiple file links by converting them to the appropriate format for downloading.

    def validate_links(self, *file_links: str, file_format: str = "csv")-> List[str]:
        

        
        extract_logger.log_message(f"Validating link to {file_format} file")
        
        validated_file_links = []
        for file_link in file_links:
            validated_file_link = file_link.replace("edit?", f"export?format={file_format}&")
            validated_file_links.append(validated_file_link)
        
        extract_logger.log_message("Link Validation Complete\n\n")
        return validated_file_links
# Reads data from a validated file link into a pandas DataFrame.
   
    def read_data(self, file_link: str)-> pd.DataFrame:
        
        
        extracted_df = pd.read_csv(file_link)
        extract_logger.log_message(f"{self.filename} extracted successfully\n\n" if self.filename else "{file_format} file extracted successfully\n\n")
        return extracted_df
