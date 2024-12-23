# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:33:18 2024

@author: odanielz
"""

import sys
sys.path.append("..")


from config_files.config import float_link, clickup_link
from extraction_files.extract_tools import Extract, pd



#     Extracts data from the Float platform by validating the link and reading the data.

def extract_float_data()-> pd.DataFrame:
    

    
    # Initialize the Extract object for Float data
    float_extractor = Extract(filename="Float_data")
    
    # Validate the Float data link
    validated_float_link = float_extractor.validate_link(float_link)
    
    # Read the Float data from the validated link into a dataframe
    float_df = float_extractor.read_data(validated_float_link)
    
    return float_df
    
#  Extracts data from the ClickUp platform by validating the link and reading the data.

def extract_clickup_data()-> pd.DataFrame:
    
    
    # Initialize the Extract object for ClickUp data
    clickup_extractor = Extract(filename= "Clickup")
    
    # Validate the ClickUp data link
    validated_clickup_link = clickup_extractor.validate_link(clickup_link)
    
    # Read the ClickUp data from the validated link into a dataframe
    clickup_df = clickup_extractor.read_data(validated_clickup_link)
    
    return clickup_df

