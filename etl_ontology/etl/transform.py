"""
transform.py

This module is responsible for transforming preprocessed data into RDF (Resource Description Framework) format.
It serves as a crucial step in our data pipeline, converting structured data into a semantic web format
that allows for rich, interconnected representations of real estate information.

Key Concepts:
1. RDF Transformation: Converting tabular data into subject-predicate-object triples.
2. Ontology Mapping: Aligning data fields with predefined ontological concepts.
3. Data Cleaning and Validation: Ensuring data quality and consistency before transformation.
4. Temporal Alignment: Handling various date formats and establishing a common temporal reference.
5. Geospatial Data Handling: Validating and standardising location information.
6. Unique Identifier Generation: Creating persistent and unique identifiers for each data entity.
7. Scalable Processing: Implementing chunk-based processing for large datasets.

The main functions in this module are:
- parse_date: Parses and standardises various date formats
- extract_earliest_year: Determines the earliest year in a dataset for temporal alignment
- find_common_earliest_year: Establishes a common temporal reference across all datasets
- map_date_to_ontology: Maps standardised dates to ontological time concepts
- clean_location_name: Standardises location names for consistency
- validate_location: Ensures location data aligns with a predefined set of valid locations
- generate_unique_identifier: Creates unique, persistent identifiers for data entities
- sanitize_uri: Prepares strings for use in URIs, ensuring valid syntax
- transform_row_to_rdf: Converts a single data row into a set of RDF triples
- save_graph_to_disk: Persists RDF data to disk in chunks for efficient processing
- load_and_transform_data: Orchestrates the transformation process for a single data source
- combine_ttl_files: Aggregates chunked RDF data into a single, coherent dataset
- main: Coordinates the entire transformation process across all data sources

Dependencies:
- pandas: For efficient data manipulation and analysis
- rdflib: For creating and manipulating RDF graphs
- DataLoader: Custom class for configuration management and file operations
- uuid: For generating unique identifiers
- datetime: For date and time operations
- pathlib: For cross-platform file path handling
- re: For regular expression operations in data cleaning
- urllib.parse: For URL encoding in URI generation
"""

import pandas as pd
from rdflib import Graph, Literal, RDF, URIRef
from rdflib.namespace import XSD
from data_loader import DataLoader
import uuid
from datetime import datetime
import os
from pathlib import Path
import re
from urllib.parse import quote

# Initialise DataLoader for configuration and file management
data_loader = DataLoader()

# Load ontology configuration and valid locations
ontology_config = data_loader.get_ontology_config()
valid_locations = data_loader.get_valid_locations()
mappings = ontology_config.MAPPINGS
DATE_FORMATS = ontology_config.DATE_FORMATS

# Set up directory for intermediate results
intermediate_dir = 'data/intermediate'
os.makedirs(intermediate_dir, exist_ok=True)

from dateutil.parser import parse

def parse_date(date_str, date_formats):
    """
    Parse and standardise date strings from various formats.
    
    This function is crucial for temporal alignment across different data sources,
    which may use different date representations. It handles multiple date formats,
    including academic years and quarterly dates, to ensure consistent temporal representation.
    
    Args:
    date_str (str): The date string to parse
    date_formats (str or list): The expected date format(s)
    
    Returns:
    str: The standardised date string (YYYY-MM-DD, YYYY-MM, or YYYY), or None if parsing fails
    
    Note:
    - The function prioritises maintaining the original granularity of the date (year, month, day).
    - For academic years (e.g., '2019-2020'), it returns the start year.
    - For quarterly dates (e.g., '2019 Q1'), it returns the start month of the quarter.
    """
    if pd.isna(date_str):
        return None

    date_str = str(date_str).strip()
    date_formats = [date_formats] if isinstance(date_formats, str) else date_formats

    for date_format in date_formats:
        try:
            if date_format == DATE_FORMATS['YYYY']:
                return str(int(float(date_str)))
            elif date_format == DATE_FORMATS['ACADEMIC_YEAR']:
                year = int(str(date_str)[:4])
                return f"{year}"  # Return start year of academic year
            elif date_format in [DATE_FORMATS['DD MMM YY'], DATE_FORMATS['YYYY-MM-DD'], DATE_FORMATS['YYYY-MM']]:
                parsed_date = datetime.strptime(date_str, date_format)
                return parsed_date.strftime('%Y-%m-%d' if 'd' in date_format else '%Y-%m')
            elif date_format == DATE_FORMATS['YYYY MMM']:
                parsed_date = parse(date_str)
                return parsed_date.strftime('%Y-%m')
            elif date_format == DATE_FORMATS['YYYY Q']:
                year, q = date_str.split()
                month = {'Q1': '01', 'Q2': '04', 'Q3': '07', 'Q4': '10'}[q.upper()]
                return f"{year}-{month}"
        except ValueError:
            continue
    return None

def extract_earliest_year(source_name):
    """
    Extract the earliest year from a dataset's date column.
    
    This function is essential for establishing a temporal baseline for each dataset.
    It helps in identifying the historical reach of each data source, which is crucial
    for temporal analysis and data integration across multiple sources.
    
    Args:
    source_name (str): The name of the data source
    
    Returns:
    int: The earliest year in the dataset, or None if extraction fails
    
    Note:
    - The function assumes that the 'date' column exists in the processed data.
    - It uses the date parsing logic from parse_date() to handle various date formats.
    - The earliest year is determined based on the standardised date representation.
    """
    input_file, _ = data_loader.get_file_paths(source_name, file_type='processed')
    try:
        # Read only the 'date' column for efficiency
        df = pd.read_csv(input_file, usecols=['date'])
        
        # Parse dates and extract years
        df['parsed_date'] = df['date'].apply(lambda x: parse_date(str(x), mappings[source_name]['date_format']))
        df['year'] = df['parsed_date'].apply(lambda x: int(x.split('-')[0]) if x else None)
        
        # Find the minimum year, ignoring NaN values
        earliest_year = df['year'].min()
        return None if pd.isna(earliest_year) else earliest_year
    except Exception as e:
        print(f"Error extracting earliest year for {source_name}: {e}")
        return None

def find_common_earliest_year(source_names):
    """
    Find the common earliest year across all datasets.
    
    This function is crucial for temporal alignment across multiple data sources.
    It establishes a common starting point for all datasets, ensuring that any
    temporal analysis or integration is based on a consistent temporal range.
    
    Args:
    source_names (list): A list of data source names
    
    Returns:
    int: The common earliest year (the maximum of all earliest years), or None if no common year is found
    
    Note:
    - This approach ensures that all datasets have data for the returned year.
    - It may result in data loss for sources with earlier records, but ensures temporal consistency.
    - The choice of using the maximum (latest) of the earliest years is a trade-off between
      data completeness and historical reach.
    """
    earliest_years = [extract_earliest_year(source_name) for source_name in source_names]
    earliest_years = [year for year in earliest_years if year is not None]
    return max(earliest_years) if earliest_years else None

def map_date_to_ontology(date_str, graph, common_earliest_year):
    """
    Map parsed date strings to appropriate ontology classes based on granularity.
    
    This function is key to semantically representing temporal data in the RDF graph.
    It creates the appropriate time entities and properties based on the granularity
    of the parsed date, aligning with the time ontology used in the project.
    
    Args:
    date_str (str): The parsed date string (YYYY, YYYY-MM, or YYYY-MM-DD)
    graph (rdflib.Graph): The RDF graph to add triples to
    common_earliest_year (int): The common earliest year across all datasets
    
    Returns:
    rdflib.URIRef: The URI of the mapped date entity, or None if mapping fails
    
    Note:
    - The function creates different types of time entities (Year, YearMonth, FullDate, AcademicYear)
      based on the granularity of the input date.
    - It ensures that only dates on or after the common_earliest_year are mapped.
    - The URIs and properties used align with the time ontology specified in the project configuration.
    """
    if not date_str:
        return None

    try:
        year = int(date_str.split('-')[0])
        if year < common_earliest_year:
            return None

        if len(date_str) == 4:  # YYYY
            date_uri = URIRef(f"{ontology_config.NAMESPACES['time']}Year/{date_str}")
            graph.add((date_uri, RDF.type, ontology_config.NAMESPACES['time'].Year))
            graph.add((date_uri, ontology_config.NAMESPACES['time'].year, Literal(date_str, datatype=XSD.gYear)))
        elif len(date_str) == 7:  # YYYY-MM
            date_uri = URIRef(f"{ontology_config.NAMESPACES['time']}YearMonth/{date_str}")
            graph.add((date_uri, RDF.type, ontology_config.NAMESPACES['time'].YearMonth))
            graph.add((date_uri, ontology_config.NAMESPACES['time'].yearMonth, Literal(date_str, datatype=XSD.gYearMonth)))
        elif len(date_str) == 10:  # YYYY-MM-DD
            date_uri = URIRef(f"{ontology_config.NAMESPACES['time']}FullDate/{date_str}")
            graph.add((date_uri, RDF.type, ontology_config.NAMESPACES['time'].FullDate))
            graph.add((date_uri, ontology_config.NAMESPACES['time'].date, Literal(date_str, datatype=XSD.date)))
        elif len(date_str) == 9:  # YYYY-YYYY (Academic Year)
            start_year, end_year = date_str.split('-')
            date_uri = URIRef(f"{ontology_config.NAMESPACES['time']}AcademicYear/{date_str}")
            graph.add((date_uri, RDF.type, ontology_config.NAMESPACES['time'].AcademicYear))
            graph.add((date_uri, ontology_config.NAMESPACES['time'].startYear, Literal(start_year, datatype=XSD.gYear)))
            graph.add((date_uri, ontology_config.NAMESPACES['time'].endYear, Literal(end_year, datatype=XSD.gYear)))
        else:
            return None
        return date_uri
    except Exception as e:
        print(f"Error mapping date to ontology: {e}")
        return None

def clean_location_name(location_name):
    """
    Clean location names by removing ' ua' if it exists at the end.
    
    This function standardises location names by removing the ' ua' suffix,
    which stands for 'urban area'. This standardisation is crucial for
    consistent geospatial representation across datasets.
    
    Args:
    location_name (str): The location name to clean
    
    Returns:
    str: The cleaned location name
    
    Note:
    - The removal of ' ua' is a specific cleaning rule that may need to be updated
      or expanded based on the evolving nature of the data sources.
    """
    return location_name[:-3] if location_name.endswith(" ua") else location_name

def validate_location(location_name):
    """
    Check for partial matches of location names against a list of valid locations.
    
    This function ensures that only recognised and standardised location names
    are used in the RDF transformation. It's crucial for maintaining consistent
    geospatial references across all datasets.
    
    Args:
    location_name (str): The location name to validate
    
    Returns:
    str: The valid location name if a partial match is found, or None otherwise
    
    Note:
    - The function uses case-insensitive partial matching to accommodate slight variations in naming.
    - This approach may need refinement if there are ambiguous partial matches between different locations.
    """
    for valid_location in valid_locations:
        if valid_location.lower() in location_name.lower():
            return valid_location
    return None

def generate_unique_identifier(row, prefix):
    """
    Generate a unique identifier using UUID for rows without natural unique IDs.
    
    This function ensures that each data entity has a unique, persistent identifier
    in the RDF graph. This is crucial for maintaining entity identity and enabling
    proper linking and querying of the data.
    
    Args:
    row (pandas.Series): The row of data (not used in the current implementation, but could be used for deterministic ID generation)
    prefix (str): A prefix for the identifier, typically based on the entity type
    
    Returns:
    str: A unique identifier string
    
    Note:
    - The current implementation uses random UUIDs, which ensures uniqueness but not reproducibility.
    - The prefix helps in identifying the type of entity and in avoiding potential collisions across different entity types.
    """
    return f"{prefix}_{str(uuid.uuid4()).replace('-', '')}"

def sanitize_uri(value):
    """
    Sanitise a string to be used in a URI.
    
    This function ensures that strings used in URIs are valid and conform to URI syntax rules.
    It's crucial for creating consistent and valid URIs for all entities in the RDF graph.
    
    Args:
    value (str): The string to sanitise
    
    Returns:
    str: The sanitised string, safe for use in a URI
    
    Note:
    - The function replaces non-alphanumeric characters (except dashes) with underscores.
    - It ensures that the resulting string doesn't start with a number, as this is invalid in some URI schemes.
    - This sanitisation approach strikes a balance between readability and strict URI compliance.
    """
    # Remove any character that isn't alphanumeric, dash, or underscore
    value = re.sub(r'[^\w\-]', '_', value)
    # Ensure it doesn't start with a number
    if value[0].isdigit():
        value = '_' + value
    return value

def transform_row_to_rdf(row, mapping, graph, common_earliest_year, summary):
    """
    Transform a single row of data into RDF triples.
    
    This function is the core of the RDF transformation process. It takes a row of data
    and converts it into a set of RDF triples based on the provided mapping configuration.
    It handles various data types, relationships, and applies necessary transformations.
    
    Args:
    row (pandas.Series): The row of data to transform
    mapping (dict): The mapping configuration for the data source
    graph (rdflib.Graph): The RDF graph to add triples to
    common_earliest_year (int): The common earliest year across all datasets
    summary (dict): A dictionary to store summary information about the transformation process
    
    Returns:
    bool: True if the transformation was successful, False otherwise
    
    Note:
    - This function handles both data properties and object properties (relationships).
    - It applies data type conversions and validations based on the mapping configuration.
    - Date fields are specially processed to ensure temporal consistency across the dataset.
    - The function adds detailed error information to the summary dict for debugging and quality assessment.
    """
    try:
        # Generate the main entity URI
        if 'transaction_id' in row:
            transaction_id = str(row['transaction_id']).replace('{', '').replace('}', '')  
            entity_uri = URIRef(mapping['uri_pattern'].format(transaction_id=transaction_id))
        else:
            unique_id = generate_unique_identifier(row, mapping['class'].split('#')[-1])
            entity_uri = URIRef(mapping['uri_pattern'].format(unique_id=unique_id))

        # Add the type triple for the main entity
        graph.add((entity_uri, RDF.type, mapping['class']))
        
        # Process each field in the row
        for field, details in mapping['fields'].items():
            if field in row and pd.notna(row[field]): 
                original_value = row[field]
                
                # Validate the value if valid_values are specified
                if 'valid_values' in details and original_value not in details['valid_values']:
                    summary['errors'].append(f"Invalid value '{original_value}' for field '{field}'")
                    return False

                # Apply value mapping if specified
                if 'mapping' in details:
                    if original_value in details['mapping']:
                        value = details['mapping'][original_value]
                    else:
                        summary['errors'].append(f"No mapping found for value '{original_value}' in field '{field}'")
                        return False
                else:
                    value = original_value

                # Special handling for date fields
                if field == 'date':
                    parsed_date = parse_date(value, mapping['date_format'])
                    if parsed_date:
                        year = int(parsed_date.split('-')[0])
                        if year >= common_earliest_year:
                            date_uri = map_date_to_ontology(parsed_date, graph, common_earliest_year)
                            if date_uri:
                                graph.add((entity_uri, details['property'], date_uri))
                            else:
                                summary['skipped_dates'].add(value)
                                return False
                        else:
                            summary['skipped_dates'].add(value)
                            return False
                    else:
                        summary['skipped_dates'].add(value)
                        return False
                elif 'object' in details and details['object']:
                    # Handle object properties (relationships)
                    sanitized_value = sanitize_uri(str(value))
                    object_uri = URIRef(f"{details['class']}/{sanitized_value}")
                    graph.add((entity_uri, details['property'], object_uri))
                    graph.add((object_uri, RDF.type, details['class']))
                else:
                    # Handle data properties
                    if 'datatype' in details:
                        graph.add((entity_uri, details['property'], Literal(value, datatype=details['datatype'])))
                    else:
                        # If datatype is not specified, use string as default
                        graph.add((entity_uri, details['property'], Literal(value, datatype=XSD.string)))

        return True
    except Exception as e:
        summary['errors'].append(str(e))
        return False

def save_graph_to_disk(graph, chunk_number):
    """
    Save the RDF graph to a Turtle file for each chunk.
    
    This function persists the transformed RDF data to disk in chunks. This approach
    allows for more efficient processing of large datasets by keeping memory usage manageable.
    However, a more robust method needs to be implemented for a better user experience,
    as some large datasets still take a while to process.
    
    Args:
    graph (rdflib.Graph): The RDF graph to save
    chunk_number (int): The number of the current chunk
    
    Note:
    - The function saves the graph in Turtle (.ttl) format, which is a compact and readable RDF serialisation.
    - Each chunk is saved as a separate file, allowing for parallel processing and easier error recovery.
    - The intermediate files are saved in a predefined directory (intermediate_dir).
    """
    output_file = os.path.join(intermediate_dir, f"transformed_data_chunk_{chunk_number}.ttl")
    graph.serialize(destination=output_file, format="turtle")

def load_and_transform_data(source_name, common_earliest_year):
    """
    Load and transform data for a single source.
    
    This function orchestrates the entire process of loading, transforming, and saving
    RDF data for a single data source. It handles the data in chunks to manage memory
    usage for large datasets.
    
    Args:
    source_name (str): The name of the data source
    common_earliest_year (int): The common earliest year across all datasets
    
    Returns:
    dict: A summary of the transformation process, including statistics and error information
    
    Note:
    - The function uses a chunked approach to process large datasets efficiently.
    - It applies location validation and cleaning if the data source includes location information.
    - Progress is tracked and displayed using a progress bar for each chunk.
    - Detailed summary information is collected, including error counts and samples of problematic data.
    """
    input_file, _ = data_loader.get_file_paths(source_name, file_type='processed')

    total_chunks = data_loader.get_total_chunks(input_file)
    progress_bar = data_loader.create_progress_bar(source_name, "Transforming", total_chunks)

    summary = {
        'total_rows': 0,
        'mapped_rows': 0,
        'skipped_dates': set(),
        'invalid_locations': set(),
        'errors': []
    }

    mapping = mappings.get(source_name)
    if not mapping:
        summary['errors'].append(f"No mapping configuration found for source '{source_name}'")
        return summary

    has_location = 'location_name' in mapping['fields']

    for chunk_number, chunk in enumerate(data_loader.get_chunked_data(input_file)):
        if chunk.empty:
            print(f"Skipping empty chunk {chunk_number}")
            continue

        processed_chunk = chunk.copy()

        if has_location:
            processed_chunk['valid_location'] = processed_chunk['location_name'].apply(clean_location_name).apply(validate_location)
            processed_chunk = processed_chunk.dropna(subset=['valid_location'])
            if processed_chunk.empty:
                print(f"No valid locations in chunk {chunk_number}, skipping.")
                continue
            processed_chunk.drop('location_name', axis=1, inplace=True)
            processed_chunk.rename(columns={'valid_location': 'location_name'}, inplace=True)

        chunk_graph = Graph()

        for _, row in processed_chunk.iterrows():
            summary['total_rows'] += 1
            if transform_row_to_rdf(row, mapping, chunk_graph, common_earliest_year, summary):
                summary['mapped_rows'] += 1

        save_graph_to_disk(chunk_graph, chunk_number)
        data_loader.update_progress_bar(progress_bar)

    data_loader.close_progress_bar(progress_bar)
    return summary

def combine_ttl_files(source_name):
    """
    Combine all chunked TTL files for a source into a single file.
    
    This function aggregates all the intermediate Turtle files created during the chunked
    processing into a single, coherent RDF dataset for each source. This step is crucial
    for creating a unified view of the data and enabling querying.
    
    Args:
    source_name (str): The name of the data source
    
    Note:
    - The function reads all intermediate .ttl files and combines them into a single graph.
    - The combined graph is then serialised into a single Turtle file.
    - After successful combination, the intermediate files are deleted to save space.
    - Progress is tracked and displayed using a progress bar.
    """
    _, output_file = data_loader.get_file_paths(source_name, file_type='processed')
    combined_graph = Graph()

    chunk_files = sorted(Path(intermediate_dir).glob(f"transformed_data_chunk_*.ttl"))

    progress_bar = data_loader.create_progress_bar(f"Combining TTL files for {source_name}", "Combining", len(chunk_files))

    for chunk_file in chunk_files:
        combined_graph += Graph().parse(chunk_file, format="turtle")
        data_loader.update_progress_bar(progress_bar)

    data_loader.close_progress_bar(progress_bar)

    output_path = Path('data/transformed') / f"{Path(output_file).stem.replace('processed_', '')}.ttl"
    print(f"Saving combined TTL file for {source_name}")
    combined_graph.serialize(destination=str(output_path), format="turtle")

    print(f"Cleaning up intermediate files for {source_name}")
    for chunk_file in chunk_files:
        chunk_file.unlink()

def main():
    """
    Coordinate the transformation process for all data sources.
    
    This is the main entry point of the script. It orchestrates the entire process of
    transforming all data sources into RDF format. The function handles the high-level
    flow of the transformation process, including finding the common earliest year,
    processing each source, and providing summary information.
    
    Note:
    - The function first determines a common earliest year across all datasets to ensure temporal consistency.
    - It then processes each data source individually, transforming the data and combining the results.
    - Detailed summaries are printed for each source, providing insights into the transformation process.
    """
    source_names = data_loader.get_source_names()

    common_earliest_year = find_common_earliest_year(source_names)
    if common_earliest_year is None:
        print("No common earliest year found; exiting.")
        return

    print(f"Common earliest year across all datasets: {common_earliest_year}")

    for source_name in source_names:
        print(f"\nProcessing source: {source_name}")
        summary = load_and_transform_data(source_name, common_earliest_year)
        combine_ttl_files(source_name)

        print(f"Summary for {source_name}:")
        print(f"Total rows processed: {summary['total_rows']}")
        print(f"Rows successfully mapped: {summary['mapped_rows']}")
        print(f"Rows skipped due to invalid dates: {len(summary['skipped_dates'])}")
        if summary['skipped_dates']:
            print(f"Sample of skipped dates: {list(summary['skipped_dates'])[:5]}")
        print(f"Rows filtered due to invalid locations: {len(summary['invalid_locations'])}")
        if summary['invalid_locations']:
            print(f"Sample of invalid locations: {list(summary['invalid_locations'])[:5]}")
        if summary['errors']:
            print(f"Errors encountered: {len(summary['errors'])}")
            print(f"Sample of errors: {summary['errors'][:5]}")

    print("\nTransformation complete. Combined TTL files saved in the transformed folder.")

if __name__ == "__main__":
    main()