from typing import List, Dict, Tuple, Union
import pandas as pd


# An utility class for performing common ETL operations
class ETLUtilities:
    

# Read data from a list of dimension tables into pandas dataframes.
# Returns a dictionary where the keys are the table names and the values are the corresponding dataframes.
    @classmethod
    def read_dim_data(cls, engine, dim_tables: List)-> Dict:
        

        
        table_map = {}
        for dim_table in dim_tables:
            query = f"SELECT * FROM {dim_table}"
            dim_df = pd.read_sql_query(query, con = engine)
            print(f"{dim_df = }")
            table_map[dim_table] = dim_df
            
        return table_map
    
    @classmethod
    def breakup_dataframe(cls, df: pd.DataFrame, breakup_column: str, breakup_value: str)-> Tuple[pd.DataFrame]:
        

        
        breakup_mask = df[breakup_column].str.lower() == breakup_value.lower()
        df_1 = df.loc[~breakup_mask, :]
        df_2 = df.loc[breakup_mask, :]
        
        return df_1, df_2
# merged dataframe.    
    @classmethod
    def merge_dataframes(cls, dataframe_1: pd.DataFrame, dataframe_2: pd.DataFrame, merge_columns: List, merge_method: str = "left")-> pd.DataFrame:
        

        
        return dataframe_1.merge(dataframe_2, on = merge_columns, how = merge_method)
    
    @classmethod
    def concatenate_dataframes(cls, 
                               dataframe_1: Union[pd.Series, pd.DataFrame], 
                               dataframe_2: Union[pd.Series, pd.DataFrame],
                               concat_column: Union[str, None]
                               )-> Union[pd.Series, pd.DataFrame]:
        

        
        if not concat_column:
            return pd.concat([dataframe_1, dataframe_2]) #returns DataFrame
        
        return pd.concat([dataframe_1[concat_column], dataframe_2[concat_column]]) #returns Series
