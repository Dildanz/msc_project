"""
data_loader.py

This module provides our pipeline the DataLoader class that handles configuration loading,
file path management, and data loading operations. 
It serves as a central point for accessing various configurations and data sources.

The DataLoader class includes methods for:
- Loading YAML and Python configurations
- Retrieving data source information
- Managing file paths for raw, processed, and transformed data
- Loading and saving RDF datasets
- Creating and managing progress bars for data processing operations

Dependencies:
- yaml: For parsing YAML configuration files
- pandas: For data manipulation and CSV operations
- tqdm: For creating progress bars
- rdflib: For handling RDF data

Usage:
    loader = DataLoader()
    data_sources = loader.get_data_sources()
    ontology_config = loader.get_ontology_config()
    # ... other operations ...
"""

import yaml
from pathlib import Path
import importlib.util
import sys
import pandas as pd
from tqdm import tqdm
from rdflib import Graph, Namespace

class DataLoader:
    def __init__(self):
        """
        Initialise the DataLoader with project directories and configurations.
        """
        self.PROJECT_ROOT = Path(__file__).resolve().parents[1]
        self.config_directory = self.PROJECT_ROOT / 'config'
        self.data_directory = self.PROJECT_ROOT / 'data'
        self.etl_directory = self.PROJECT_ROOT / 'etl'
        self.ontology_directory = self.PROJECT_ROOT / 'ontology'

        # Add project root to sys.path for imports
        if str(self.PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(self.PROJECT_ROOT))

        # Load configurations with error checking
        self.data_sources = self._load_yaml_config('data_sources.yaml')['sources']
        self.ontology_config = self._load_python_config('ontology_config.py')
        self.column_configs = self._load_python_config('column_configs.py')
        self.valid_locations = self._load_yaml_config('valid_locations.yaml').get('locations', [])
        self.CHUNK_SIZE = 100000
        self.LARGE_FILE_THRESHOLD = 100 * 1024 * 1024

    def _load_yaml_config(self, filename):
        """
        Load a YAML configuration file.
        
        Args:
        filename (str): The name of the YAML file to load
        
        Returns:
        dict: The loaded configuration
        
        Raises:
        FileNotFoundError: If the configuration file is not found
        ValueError: If the configuration file is empty or invalid
        """
        config_path = self.config_directory / filename
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file {filename} not found in {self.config_directory}.")
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                if not config:
                    raise ValueError(f"Configuration file {filename} is empty or invalid.")
                return config
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {filename}: {str(e)}")

    def _load_python_config(self, filename):
        """
        Load a Python configuration file.
        
        Args:
        filename (str): The name of the Python file to load
        
        Returns:
        module: The loaded Python module
        
        Raises:
        FileNotFoundError: If the configuration file is not found
        ImportError: If there's an error importing the module
        """
        config_path = self.config_directory / filename
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file {filename} not found in {self.config_directory}.")
        try:
            spec = importlib.util.spec_from_file_location(filename[:-3], config_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load the Python configuration module {filename}.")
            module = importlib.util.module_from_spec(spec)
            sys.modules[filename[:-3]] = module
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            raise ImportError(f"Error loading Python configuration {filename}: {str(e)}")

    def get_data_sources(self):
        """
        Return the list of data sources.
        
        Returns:
        list: The list of data sources
        
        Raises:
        ValueError: If the data sources configuration is missing or empty
        """
        if not self.data_sources:
            raise ValueError("Data sources configuration is missing or empty.")
        return self.data_sources

    def get_source_names(self):
        """
        Return a list of source names.
        
        Returns:
        list: The list of source names
        
        Raises:
        ValueError: If no source names are found in the configuration
        """
        source_names = [source['name'] for source in self.data_sources]
        if not source_names:
            raise ValueError("No source names found in the data sources configuration.")
        return source_names

    def get_file_paths(self, source_name, file_type='raw'):
        """
        Get input and output file paths for a given source and file type.
        
        Args:
        source_name (str): The name of the data source
        file_type (str): The type of file ('raw' or 'processed')
        
        Returns:
        tuple: A tuple containing the input and output file paths
        
        Raises:
        ValueError: If the source is not found or if an invalid file_type is provided
        KeyError: If there's a missing configuration for the source
        """
        source = next((s for s in self.data_sources if s['name'] == source_name), None)
        if not source:
            raise ValueError(f"Source '{source_name}' not found in configuration.")
        
        try:
            if file_type == 'raw':
                input_file = self.PROJECT_ROOT / 'data' / 'raw' / Path(source['output_file']).name
                output_file = self.PROJECT_ROOT / 'data' / 'processed' / f"processed_{Path(source['output_file']).name}"
            elif file_type == 'processed':
                input_file = self.PROJECT_ROOT / 'data' / 'processed' / f"processed_{Path(source['output_file']).name}"
                output_file = self.PROJECT_ROOT / 'data' / 'transformed' / f"transformed_{Path(source['output_file']).name}"
            else:
                raise ValueError(f"Invalid file_type: {file_type}. Must be 'raw' or 'processed'.")
        except KeyError as e:
            raise KeyError(f"Missing expected configuration for source '{source_name}': {str(e)}")

        return input_file, output_file

    def get_ontology_config(self):
        """
        Return the ontology configuration module.
        
        Returns:
        module: The ontology configuration module
        
        Raises:
        ValueError: If the ontology configuration is missing or invalid
        """
        if not self.ontology_config:
            raise ValueError("Ontology configuration is missing or invalid.")
        return self.ontology_config

    def get_column_config(self, source_name):
        """
        Return the column configuration for a specific source.
        
        Args:
        source_name (str): The name of the data source
        
        Returns:
        dict: The column configuration for the specified source
        
        Raises:
        ValueError: If the column configuration for the source is not found
        """
        config = getattr(self.column_configs, source_name, None)
        if config is None:
            raise ValueError(f"Column configuration for source '{source_name}' not found.")
        return config

    def get_valid_locations(self):
        """
        Return the list of valid locations.
        
        Returns:
        list: The list of valid locations
        
        Raises:
        ValueError: If the valid locations configuration is missing or empty
        """
        if not self.valid_locations:
            raise ValueError("Valid locations configuration is missing or empty.")
        return self.valid_locations
    
    def get_ontology_database(self):
        """
        Import and return the OntologyDatabase instance.
        
        Returns:
        OntologyDatabase: An instance of the OntologyDatabase class
        
        Raises:
        ImportError: If there's an error importing the OntologyDatabase
        """
        try:
            from ontology.ontology_database import OntologyDatabase
            return OntologyDatabase(self)
        except ImportError as e:
            raise ImportError(f"Error importing OntologyDatabase: {str(e)}")

    def get_chunked_data(self, file_path):
        """
        Generator function to yield chunks of data from a CSV file.
        If the file is larger than LARGE_FILE_THRESHOLD, it yields chunks.
        Otherwise, it yields the entire file as a single chunk.
        
        Args:
        file_path (Path): The path to the CSV file
        
        Yields:
        DataFrame: Chunks of data from the CSV file
        
        Raises:
        FileNotFoundError: If the specified file is not found
        ValueError: If the file is empty or cannot be read
        IOError: If there's an error reading the file
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found.")
        try:
            file_size = file_path.stat().st_size
            if file_size > self.LARGE_FILE_THRESHOLD:
                for chunk in pd.read_csv(file_path, chunksize=self.CHUNK_SIZE):
                    yield chunk
            else:
                yield pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            raise ValueError(f"File {file_path} is empty or could not be read.")
        except Exception as e:
            raise IOError(f"Error reading file {file_path}: {str(e)}")

    def get_total_chunks(self, file_path):
        """
        Calculate the total number of chunks for a given file.
        
        Args:
        file_path (Path): The path to the file
        
        Returns:
        int: The total number of chunks
        
        Raises:
        FileNotFoundError: If the specified file is not found
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found.")
        file_size = file_path.stat().st_size
        if file_size > self.LARGE_FILE_THRESHOLD:
            return -(-file_size // (self.CHUNK_SIZE * 1024))  # Ceiling division
        else:
            return 1

    def create_progress_bar(self, source_name, process_name, total_chunks):
        """
        Create and return a tqdm progress bar.
        
        Args:
        source_name (str): The name of the data source
        process_name (str): The name of the process (e.g., "Extracting", "Transforming")
        total_chunks (int): The total number of chunks to process
        
        Returns:
        tqdm: A tqdm progress bar object
        
        Raises:
        ValueError: If total_chunks is less than 1
        """
        if total_chunks < 1:
            raise ValueError("Total chunks must be a positive integer.")
        return tqdm(total=total_chunks, desc=f"{process_name} {source_name}", 
                    unit="chunk", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}')

    def update_progress_bar(self, progress_bar, increment=1):
        """
        Update the progress bar by the specified increment.
        
        Args:
        progress_bar (tqdm): The progress bar to update
        increment (int): The amount to increment the progress bar by
        
        Raises:
        ValueError: If increment is not a positive integer
        """
        if not isinstance(increment, int) or increment < 1:
            raise ValueError("Increment must be a positive integer.")
        progress_bar.update(increment)

    def close_progress_bar(self, progress_bar):
        """
        Close the progress bar.
        
        Args:
        progress_bar (tqdm): The progress bar to close
        
        Raises:
        ValueError: If the progress bar is not initialised
        """
        if not progress_bar:
            raise ValueError("Progress bar is not initialised.")
        progress_bar.close()

    def load_rdf_dataset(self):
        """
        Load the RDF dataset and return a Graph object.
        
        Returns:
        Graph: An RDF Graph object containing the loaded dataset
        
        Raises:
        FileNotFoundError: If the RDF dataset file is not found
        IOError: If there's an error loading the RDF dataset
        """
        rdf_dataset_path = self.PROJECT_ROOT / 'data' / 'ontology' / 'populated_real_estate_ontology.ttl'
        if not rdf_dataset_path.exists():
            raise FileNotFoundError(f"RDF dataset file {rdf_dataset_path} not found.")
        try:
            graph = Graph()
            graph.parse(rdf_dataset_path, format="turtle")
            
            # Add namespaces
            for prefix, uri in self.ontology_config.NAMESPACES.items():
                graph.bind(prefix, Namespace(uri))
            
            return graph
        except Exception as e:
            raise IOError(f"Error loading RDF dataset: {str(e)}")

    def save_query_results(self, data, filename):
        """
        Save query results to a CSV file.
        
        Args:
        data (DataFrame): The data to save
        filename (str): The name of the file to save the data to
        
        Returns:
        Path: The path where the file was saved
        
        Raises:
        ValueError: If the data is empty
        IOError: If there's an error saving the query results
        """
        if data.empty:
            raise ValueError("Cannot save empty data.")
        output_path = self.PROJECT_ROOT / 'data' / 'output' / filename
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(output_path, index=False)
            return output_path
        except Exception as e:
            raise IOError(f"Error saving query results to {filename}: {str(e)}")

# Global instance of DataLoader
data_loader = DataLoader()