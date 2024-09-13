"""
ontology_database.py

Provides a layer of functionality over the ontology for querying and extracting data.
It defines the OntologyDatabase class, which loads an RDF graph and allows for SPARQL
query execution and data extraction.

The main functionalities of this module are:
1. Loading an RDF dataset into memory
2. Executing SPARQL queries on the loaded dataset
3. Converting query results to pandas DataFrames for easy manipulation
4. Extracting and saving data cubes based on specific queries

Dependencies:
    - rdflib: For working with RDF data and executing SPARQL queries
    - pandas: For data manipulation and storage of query results
    - DataLoader: Custom class for loading RDF datasets and managing file paths

Note: This script doesn't have a main() function as it's designed to be initialised in query scripts.
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path to allow imports from sibling directories
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from etl.data_loader import DataLoader
from rdflib.plugins.sparql import prepareQuery
import pandas as pd

class OntologyDatabase:
    """
    A class for managing and querying the ontology graph.

    This class provides methods to load the ontology graph, execute SPARQL queries,
    and extract data cubes from the graph.

    Attributes:
        loader (DataLoader): An instance of DataLoader for accessing the RDF dataset.
        graph (rdflib.Graph): The loaded graph containing the dataset.
    """

    def __init__(self, loader):
        """
        Initialises the OntologyDatabase with a DataLoader and loads the ontology graph.

        Args:
            loader (DataLoader): An instance of DataLoader to access the RDF dataset.
        """
        self.loader = loader
        self.graph = self.loader.load_rdf_dataset()

    def execute_query(self, query_string):
        """
        Executes a SPARQL query on the ontology graph and returns the results as a pandas DataFrame.

        This method handles different types of query result formats and converts them into a
        consistent DataFrame structure.

        Args:
            query_string (str): The SPARQL query to execute.

        Returns:
            pandas.DataFrame: A DataFrame containing the query results.
        """
        results = self.graph.query(query_string)
        
        data = []
        for row in results:
            if hasattr(row, 'asdict'):
                # If the result row has an asdict method, use it
                data.append(row.asdict())
            elif isinstance(row, tuple):
                # If the row is a tuple, create a dictionary using variable names
                data.append(dict(zip(results.vars, map(str, row))))
            else:
                # For any other type, create a dictionary with string conversions
                data.append({str(var): str(value) for var, value in zip(results.vars, row)})
        
        return pd.DataFrame(data)

    def get_rdf_dataset_info(self):
        """
        Returns basic information about the ontology.

        This method provides a quick overview of the ontology's size and namespaces.

        Returns:
            dict: A dictionary containing the number of triples and list of namespaces.
        """
        return {
            "triples_count": len(self.graph),
            "namespaces": list(self.graph.namespaces())
        }

    def extract_and_save_data_cube(self, query, output_filename):
        """
        Extracts a data cube from the ontology based on a given query and saves it to a CSV file.

        This method executes the provided query, creates a data cube as a DataFrame,
        displays a preview, and saves the result to a CSV file.

        Args:
            query (str): The SPARQL query to extract the data cube.
            output_filename (str): The name of the file to save the data cube.

        Prints:
            Information about the extraction process, including a data preview and save location.
        """
        print("Extracting data cube from RDF dataset...")
        data_cube = self.execute_query(query)
        
        print("\nData Cube Preview:")
        print(data_cube.head())
        
        print(f"\nTotal records in the data cube: {len(data_cube)}")
        
        # Save the data cube to a CSV file
        output_path = self.loader.save_query_results(data_cube, output_filename)
        print(f"\nData cube saved to: {output_path}")