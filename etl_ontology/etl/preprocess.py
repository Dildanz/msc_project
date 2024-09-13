"""
preprocess.py

This module is responsible for preprocessing the extracted real estate data from various UK gov sources.
It handles tasks such as renaming columns, setting data types, melting dataframes, and applying
source-specific operations as specified in the data_sources.yaml configuration file.

The main functions in this module are:
- generate_sources_list: Generates a list of data sources and their file paths
- rename_and_drop_columns: Renames columns and drops unnecessary ones
- convert_column_type: Converts a column to a specified data type
- set_column_types: Sets the data types for multiple columns
- melt: Reshapes the dataframe from wide to long format
- drop_rows_with_missing_data: Removes rows with missing data in critical columns
- apply_source_specific_operations: Applies specific preprocessing steps for each data source
- process_chunk: Processes a chunk of data from a specific source
- preprocess_data: Main function that orchestrates the preprocessing process for a single source
- main: Coordinates the preprocessing of all data sources

Dependencies:
- pandas: For data manipulation and analysis
- numpy: For numerical operations
- datetime: For date and time operations
- dateutil: For relative date calculations
- pathlib: For handling file paths
- DataLoader: Custom class for loading configuration and managing file paths
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from data_loader import DataLoader

loader = DataLoader()

def generate_sources_list():
    """
    Generate a list of data sources and their corresponding file paths.
    
    Returns:
    list: A list of tuples containing (source_name, input_file_path, output_file_path)
    """
    try:
        return [
            (
                source['name'],
                *loader.get_file_paths(source['name'], 'raw')
            )
            for source in loader.get_data_sources()
        ]
    except Exception as e:
        print(f"Error generating sources list: {e}")
        return []

def rename_and_drop_columns(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """
    Rename columns according to the provided mapping and drop unnecessary columns.
    
    Args:
    df (pd.DataFrame): The input dataframe
    column_mapping (dict): A dictionary mapping old column names to new ones
    
    Returns:
    pd.DataFrame: The dataframe with renamed columns and unnecessary columns dropped
    """
    try:
        df = df.rename(columns=column_mapping)
        columns_to_keep = list(column_mapping.values())
        return df[columns_to_keep]
    except KeyError as e:
        print(f"Column renaming or dropping error: {e}")
        return df

def convert_column_type(series: pd.Series, dtype: str) -> pd.Series:
    """
    Convert a pandas Series to the specified data type.
    
    Args:
    series (pd.Series): The input series
    dtype (str): The target data type ('int', 'float', or 'str')
    
    Returns:
    pd.Series: The series converted to the specified data type
    """
    try:
        if dtype == 'int':
            return series.astype(float).round().astype('Int64')
        elif dtype == 'float':
            return series.astype(float)
        elif dtype == 'str':
            return series.astype(str).str.lower()
        else:
            print(f"Unsupported dtype: {dtype}. Returning original series.")
            return series
    except Exception as e:
        print(f"Error converting column type: {e}")
        return series

def set_column_types(df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
    """
    Set the data types for multiple columns in a dataframe.
    
    Args:
    df (pd.DataFrame): The input dataframe
    column_types (dict): A dictionary mapping column names to their target data types
    
    Returns:
    pd.DataFrame: The dataframe with updated column data types
    """
    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                df[column] = convert_column_type(df[column], dtype)
            except Exception as e:
                print(f"Error setting column type for {column}: {e}")
    return df

def melt(df: pd.DataFrame, id_vars: list, var_name: str, value_name: str) -> pd.DataFrame:
    """
    Reshape the dataframe from wide to long format.
    
    Args:
    df (pd.DataFrame): The input dataframe
    id_vars (list): Columns to use as identifier variables
    var_name (str): Name to use for the variable column
    value_name (str): Name to use for the value column
    
    Returns:
    pd.DataFrame: The reshaped dataframe
    """
    try:
        return pd.melt(df, id_vars=id_vars, var_name=var_name, value_name=value_name)
    except Exception as e:
        print(f"Error melting dataframe: {e}")
        return df

def drop_rows_with_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with missing data in critical columns (location_name and date).
    
    Args:
    df (pd.DataFrame): The input dataframe
    
    Returns:
    pd.DataFrame: The dataframe with rows containing missing critical data removed
    """
    try:
        if 'location_name' in df.columns:
            df = df.dropna(subset=['location_name'])
        if 'date' in df.columns:
            df = df.dropna(subset=['date'])
        return df
    except Exception as e:
        print(f"Error dropping rows with missing data: {e}")
        return df

def apply_source_specific_operations(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Apply specific preprocessing steps for each data source.
    The data sources required 'trimming' to remove unusued space (rows, columns),
    as some of the datasets are in a presentation format.
    
    Args:
    df (pd.DataFrame): The input dataframe
    source_name (str): The name of the data source
    
    Returns:
    pd.DataFrame: The dataframe with source-specific operations applied
    """
    try:
        if source_name == 'additional_dwellings':
            # Remove unnecessary rows and reset the index
            df = df.drop([0, 1, 3, 4]).reset_index(drop=True)
            # Set the column names to the values in the first row
            df.columns = df.iloc[0]
            # Remove the first row (now redundant) and reset the index
            df = df[1:].reset_index(drop=True)
            # Replace '[x]' with NaN
            df = df.replace('[x]', np.nan)
        elif source_name == 'mortgage_rate':
            # Select specific rows and reset the index
            df = df.iloc[24:26].reset_index(drop=True)
            # Drop unnecessary columns
            df = df.drop(df.columns[[0, 1, 3]], axis=1)
            # Generate date columns
            start_date = datetime(2007, 3, 1)
            date_columns = ['rate_type']
            current_date = start_date
            for _ in range(len(df.columns) - 1):
                date_columns.append(current_date.strftime('%Y-%m'))
                current_date += relativedelta(months=3)
            df.columns = date_columns
        elif source_name == 'unemployment':
            # Remove the first 8 rows and reset the index
            df = df.drop(range(0, 8)).reset_index(drop=True)
        elif source_name == 'price_paid':
            # Set column names to numeric indices
            header = [str(i) for i in range(len(df.columns))]
            df.columns = header
        return df
    except Exception as e:
        print(f"Error applying source-specific operations for {source_name}: {e}")
        return df

def process_chunk(chunk: pd.DataFrame, source_name: str, column_config: dict) -> pd.DataFrame:
    """
    Process a chunk of data from a specific source.
    
    Args:
    chunk (pd.DataFrame): A chunk of the input dataframe
    source_name (str): The name of the data source
    column_config (dict): Configuration for column operations
    
    Returns:
    pd.DataFrame: The processed chunk
    """
    try:
        # Apply source-specific operations
        chunk = apply_source_specific_operations(chunk, source_name)

        # Rename and drop columns if enabled in the column_config.py
        if 'column_mapping' in column_config and column_config['column_mapping']:
            chunk = rename_and_drop_columns(chunk, column_config['column_mapping'])

        # Melt the dataframe if enabled in the column_config.py
        if 'melt_config' in column_config and column_config['melt_config']:
            melt_config = column_config['melt_config']
            chunk = melt(chunk, **melt_config)

        # Drop rows with missing data in critical columns
        chunk = drop_rows_with_missing_data(chunk)

        # Set column types if enabled in the column_config.py
        if 'column_types' in column_config and column_config['column_types']:
            chunk = set_column_types(chunk, column_config['column_types'])

        return chunk
    except Exception as e:
        print(f"Error processing chunk for {source_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def preprocess_data(source_name: str, input_file: Path, output_file: Path, column_config: dict):
    """
    Preprocess data for a single source.
    
    Args:
    source_name (str): The name of the data source
    input_file (Path): The path to the input file
    output_file (Path): The path to the output file
    column_config (dict): Configuration for column operations
    """
    try:
        total_chunks = loader.get_total_chunks(input_file)
        progress_bar = loader.create_progress_bar(source_name, "Preprocessing", total_chunks)

        for i, chunk in enumerate(loader.get_chunked_data(input_file)):
            processed_chunk = process_chunk(chunk, source_name, column_config)
            if i == 0:
                processed_chunk.to_csv(output_file, index=False, mode='w')
            else:
                processed_chunk.to_csv(output_file, index=False, mode='a', header=False)
            loader.update_progress_bar(progress_bar)

        loader.close_progress_bar(progress_bar)
        print(f"Completed preprocessing {source_name}")
    except Exception as e:
        print(f"Error in preprocessing data for {source_name}: {e}")
    finally:
        print("-" * 100)

def main():
    """
    Main function to coordinate the preprocessing of all data sources.
    """
    sources = generate_sources_list()
    
    for source_name, input_file, output_file in sources:
        column_config = loader.get_column_config(source_name)
        preprocess_data(source_name, input_file, output_file, column_config)

if __name__ == "__main__":
    main()