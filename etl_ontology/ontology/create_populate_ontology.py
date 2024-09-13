"""
create_populate_ontology.py

Responsible for creating and populating our ontology.
It defines the OntologyCreator class, which handles the creation of classes, properties, and relationships
as defined in our ontology configuration. It then populates the ontology with transformed data.

The main functions of this module are:
1. Creating the basic structure of the ontology (classes, subclasses, properties)
2. Saving the empty ontology to a file
3. Populating the ontology with transformed data from TTL files
4. Saving the populated ontology to a file

Dependencies:
    - rdflib: For creating and manipulating RDF graphs
    - DataLoader: Custom class for loading configuration and managing file paths
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path to allow imports from sibling directories
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal
from etl.data_loader import DataLoader
import os

class OntologyCreator:
    """
    A class for creating and populating an RDF ontology based on a predefined configuration.

    This class handles the creation of ontology elements (classes, subclasses, properties)
    and populates the ontology with data from transformed TTL files.

    Attributes:
        data_loader (DataLoader): An instance of DataLoader for accessing configuration and file paths.
        ontology_config: The ontology configuration loaded from DataLoader.
        graph (rdflib.Graph): The RDF graph representing the ontology.
        namespaces (dict): A dictionary of namespace prefixes and URIs.
    """

    def __init__(self):
        """
        Initialises the OntologyCreator with necessary configurations and graph.

        Raises:
            SystemExit: If there's an error in initialisation.
        """
        try:
            self.data_loader = DataLoader()
            self.ontology_config = self.data_loader.get_ontology_config()
            self.graph = Graph()
            self.namespaces = self.ontology_config.NAMESPACES
        except Exception as e:
            print(f"Error initialising OntologyCreator: {e}")
            sys.exit(1)

    def create_ontology(self):
        """
        Creates the basic structure of the ontology based on the configuration.

        This method adds classes, subclasses, object properties, and data properties
        to the RDF graph as defined in the ontology configuration.
        """
        try:
            print("Creating ontology...")
            # Add namespaces to the graph
            for prefix, uri in self.namespaces.items():
                self.graph.bind(prefix, uri)

            # Create classes
            for class_info in self.ontology_config.CLASSES:
                self.graph.add((class_info['uri'], RDF.type, OWL.Class))
                self.graph.add((class_info['uri'], RDFS.label, Literal(class_info['name'])))

            # Create subclasses
            for subclass_info in self.ontology_config.SUBCLASSES:
                self.graph.add((subclass_info['uri'], RDF.type, OWL.Class))
                self.graph.add((subclass_info['uri'], RDFS.subClassOf, subclass_info['parent']))
                self.graph.add((subclass_info['uri'], RDFS.label, Literal(subclass_info['name'])))

            # Create object properties
            for prop_info in self.ontology_config.OBJECT_PROPERTIES:
                self.graph.add((prop_info['uri'], RDF.type, OWL.ObjectProperty))
                self.graph.add((prop_info['uri'], RDFS.domain, prop_info['domain']))
                self.graph.add((prop_info['uri'], RDFS.range, prop_info['range']))
                self.graph.add((prop_info['uri'], RDFS.label, Literal(prop_info['name'])))

            # Create data properties
            for prop_name, prop_uri in self.ontology_config.DATA_PROPERTIES.items():
                self.graph.add((prop_uri, RDF.type, OWL.DatatypeProperty))
                self.graph.add((prop_uri, RDFS.label, Literal(prop_name)))

            print("Ontology created successfully.")
        except Exception as e:
            print(f"Error creating ontology: {e}")

    def save_ontology(self, output_path):
        """
        Saves the current state of the ontology to a file in Turtle format.

        Args:
            output_path (str or Path): The path where the ontology file will be saved.
        """
        try:
            print(f"Saving ontology to {output_path}...")
            self.graph.serialize(destination=str(output_path), format="turtle")
            print("Ontology saved successfully.")
        except Exception as e:
            print(f"Error saving ontology to {output_path}: {e}")

    def populate_ontology(self, transformed_data_dir):
        """
        Populates the ontology with data from transformed TTL files.

        This method reads all TTL files in the specified directory and adds their contents
        to the ontology graph.

        Args:
            transformed_data_dir (str or Path): The directory containing transformed TTL files.
        """
        try:
            print("Populating ontology with transformed data...")
            transformed_data_path = Path(transformed_data_dir)
            if not transformed_data_path.exists() or not transformed_data_path.is_dir():
                print(f"Transformed data directory {transformed_data_dir} does not exist or is not a directory.")
                return
            
            ttl_files = list(transformed_data_path.glob("*.ttl"))
            if not ttl_files:
                print("No TTL files found in the transformed data directory.")
                return
            
            for file_path in ttl_files:
                try:
                    print(f"Processing file: {file_path}")
                    self.graph.parse(file_path, format="turtle")
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

            print("Ontology populated successfully.")
        except Exception as e:
            print(f"Error populating ontology: {e}")

def main():
    """
    Main function to create and populate the ontology.

    Orchestrates the process of creating, saving, and populating the ontology.
    """
    creator = OntologyCreator()
    
    # Create the ontology
    creator.create_ontology()
    
    # Save the empty ontology
    try:
        ontology_output_path = parent_dir / 'data' / 'ontology' / 'real_estate_ontology.ttl'
        ontology_output_path.parent.mkdir(parents=True, exist_ok=True)
        creator.save_ontology(ontology_output_path)
    except Exception as e:
        print(f"Error saving the empty ontology: {e}")
    
    # Populate the ontology with transformed data
    try:
        transformed_data_dir = parent_dir / 'data' / 'transformed'
        creator.populate_ontology(transformed_data_dir)
        
        # Save the populated ontology
        populated_ontology_output_path = parent_dir / 'data' / 'ontology' / 'populated_real_estate_ontology.ttl'
        creator.save_ontology(populated_ontology_output_path)
    except Exception as e:
        print(f"Error populating and saving the populated ontology: {e}")

if __name__ == "__main__":
    main()