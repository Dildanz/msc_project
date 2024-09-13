"""
extract.py

This module is responsible for the extraction of real estate data from various UK gov sources as specified in the
data_sources.yaml configuration file. It supports direct downloads and web scraping,
and can handle different file formats including CSV, Excel, ODS, and ZIP files.

The main functions in this module are:
- download_file: Downloads a file from a given URL
- scrape_web_page: Scrapes a web page using BeautifulSoup
- find_download_link: Finds a download link on a web page
- extract_table_data: Extracts table data from a web page
- convert_to_csv: Converts various file formats to CSV
- extract_data: Main function that orchestrates the extraction process
- print_summary: Prints a summary of the extraction results

Dependencies:
- requests: For making HTTP requests
- BeautifulSoup: For parsing HTML content
- pandas: For data manipulation and CSV conversion
- zipfile: For handling ZIP files
- DataLoader: Custom class for loading configuration and managing file paths
"""

import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
from pathlib import Path
import shutil
from data_loader import DataLoader

loader = DataLoader()
config = {'sources': loader.get_data_sources()}

def download_file(url, output_file):
    """
    Download a file from the given URL and save it to the specified output file.
    
    Args:
    url (str): The URL of the file to download
    output_file (str): The path where the downloaded file will be saved
    
    Returns:
    Path: The path of the downloaded file, or None if the download failed
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB

        output_path = loader.PROJECT_ROOT / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)

        progress_bar = loader.create_progress_bar("Downloading", "Extracting", total_size)

        start_time = time.time()  # Start timing the download

        with open(output_path, 'wb') as file:
            for data in response.iter_content(block_size):
                size = file.write(data)
                loader.update_progress_bar(progress_bar, size)

        loader.close_progress_bar(progress_bar)

        end_time = time.time()  # End timing the download
        download_time = end_time - start_time  # Calculate download duration

        # Print download details
        print(f"Downloaded file: {output_path.name}")
        print(f"Size: {total_size / (1024 * 1024):.2f} MB")  # Size in MB
        print(f"Download time: {download_time:.2f} seconds")

        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error during file download: {str(e)}")
        return None

def scrape_web_page(url, parser):
    """
    Scrape a web page using the specified parser.
    
    Args:
    url (str): The URL of the web page to scrape
    parser (str): The parser to use with BeautifulSoup
    
    Returns:
    BeautifulSoup: The parsed HTML content, or None if scraping failed
    """
    try:
        # Use a custom User-Agent to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        soup = BeautifulSoup(response.content, parser)
        return soup
    except requests.exceptions.RequestException as e:
        print(f"Error scraping web page: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error during web scraping: {str(e)}")
        return None

def find_download_link(soup, link_text):
    """
    Find a download link in the parsed HTML content based on the given link text.
    
    Args:
    soup (BeautifulSoup): The parsed HTML content
    link_text (str): The text to search for in the link
    
    Returns:
    str: The URL of the download link, or None if not found
    """
    try:
        links = soup.find_all('a')
        for link in links:
            if link_text.lower() in link.text.lower():
                return link.get('href')
        print(f"No download link found with text: {link_text}")
        return None
    except Exception as e:
        print(f"Error finding download link: {str(e)}")
        return None

def extract_table_data(soup, output_file):
    """
    Extract table data from the parsed HTML content and save it to a CSV file.
    
    Args:
    soup (BeautifulSoup): The parsed HTML content
    output_file (str): The path where the extracted data will be saved
    
    Returns:
    bool: True if extraction was successful, False otherwise
    """
    try:
        # Find the table with id 'stats-table'
        table = soup.find('table', {'id': 'stats-table'})
        if table:
            # Extract headers and rows from the table
            headers = [header.text.strip() for header in table.find('thead').find_all('th')]
            rows = []
            progress_bar = loader.create_progress_bar("Extracting table data", "Extracting", len(table.find('tbody').find_all('tr')))

            for row in table.find('tbody').find_all('tr'):
                columns = [col.text.strip() for col in row.find_all('td')]
                rows.append(columns)
                loader.update_progress_bar(progress_bar)

            loader.close_progress_bar(progress_bar)

            # Create a DataFrame and save to CSV
            df = pd.DataFrame(rows, columns=headers)
            df.to_csv(output_file, index=False)

            # Print extracted table information
            print(f"Extracted table to: {output_file}")
            print(f"Number of rows: {len(rows)}")

            return True
        else:
            print("No table found with the specified ID.")
            return False
    except Exception as e:
        print(f"Error extracting table data: {str(e)}")
        return False

def convert_to_csv(input_file, output_file, file_type, sheet_name=None, zip_target_file=None):
    """
    Convert various file types to CSV format.
    
    Args:
    input_file (str): The path of the input file
    output_file (str): The path where the CSV file will be saved
    file_type (str): The type of the input file ('xlsx', 'ods', 'zip', or 'csv')
    sheet_name (str, optional): The name of the sheet to extract (for Excel files)
    zip_target_file (str, optional): The name of the target file within a ZIP archive
    
    Returns:
    bool: True if conversion was successful, False otherwise
    """
    try:
        start_time = time.time()  # Start timing the conversion

        if file_type in ['xlsx', 'ods']:
            # Handle Excel and ODS files
            df = pd.read_excel(input_file, sheet_name=sheet_name, engine='openpyxl' if file_type == 'xlsx' else 'odf')
            df.to_csv(output_file, index=False)
        elif file_type == 'zip':
            # Handle ZIP files
            with zipfile.ZipFile(input_file, 'r') as zip_ref:
                temp_dir = input_file.parent / 'temp_extract'
                temp_dir.mkdir(exist_ok=True)
                
                zip_ref.extractall(temp_dir)
                
                # Find the target file within the extracted contents
                target_files = list(temp_dir.rglob(zip_target_file))
                
                if target_files:
                    target_file = target_files[0]
                    shutil.copy(target_file, output_file)
                    print(f"File found and extracted: {target_file}")
                else:
                    print(f"Target CSV file not found in the zip archive. Pattern: {zip_target_file}")
                    return False
                
                shutil.rmtree(temp_dir)
                
        elif file_type == 'csv':
            # For CSV files, just copy the file
            shutil.copy(input_file, output_file)
        else:
            print(f"Unsupported file type: {file_type}")
            return False

        # Remove the temporary input file if it's different from the output file
        if Path(input_file) != Path(output_file) and Path(input_file).exists():
            Path(input_file).unlink()

        end_time = time.time()  # End timing the conversion
        conversion_time = end_time - start_time  # Calculate conversion duration

        # Print conversion details
        output_size = Path(output_file).stat().st_size / (1024 * 1024)  # Size in MB
        print(f"Converted file: {output_file}")
        print(f"Size: {output_size:.2f} MB")
        print(f"Conversion time: {conversion_time:.2f} seconds")

        return True
    except Exception as e:
        print(f"Error converting file: {str(e)}")
        return False

def extract_data(config):
    """
    Main function to extract data from all configured sources.
    
    Args:
    config (dict): The configuration dictionary containing data sources
    
    Returns:
    list: A list of tuples containing the results of each extraction (source name, success status, error message)
    """
    results = []
    for source in config['sources']:
        progress_bar = loader.create_progress_bar(source['name'], "Extracting", 1)
        success = False
        error_message = ""
        
        try:
            if source['type'] == 'direct_download':
                # Handle direct download sources
                temp_file = download_file(source['url'], f"data/raw/temp_{source['name']}.{source['file_type']}")
                if temp_file:
                    sheet_name = source.get('sheet_name')
                    zip_target_file = source.get('zip_target_file')
                    success = convert_to_csv(temp_file, loader.PROJECT_ROOT / source['output_file'], source['file_type'], sheet_name, zip_target_file)
                else:
                    error_message = "Failed to download file"
            
            elif source['type'] == 'web_scrape':
                # Handle web scraping sources
                soup = scrape_web_page(source['url'], source['parser'])
                if soup:
                    if source.get('link_text'):
                        # If link_text is provided, find and download the linked file
                        download_url = find_download_link(soup, source['link_text'])
                        if download_url:
                            if not download_url.startswith('http'):
                                base_url = source.get('base_url', '')
                                download_url = f"{base_url}{download_url}"
                            temp_file = download_file(download_url, f"data/raw/temp_{source['name']}.{source['file_type']}")
                            if temp_file:
                                sheet_name = source.get('sheet_name')
                                zip_target_file = source.get('zip_target_file')
                                success = convert_to_csv(temp_file, loader.PROJECT_ROOT / source['output_file'], source['file_type'], sheet_name, zip_target_file)
                            else:
                                error_message = "Failed to download file"
                        else:
                            error_message = "Failed to find download link"
                    else:
                        # If no link_text, extract table data directly from the page
                        success = extract_table_data(soup, loader.PROJECT_ROOT / source['output_file'])
                else:
                    error_message = "Failed to scrape webpage"
        except Exception as e:
            error_message = str(e)

        loader.update_progress_bar(progress_bar)
        loader.close_progress_bar(progress_bar)
        results.append((source['name'], success, error_message))
        
        print("-" * 100)
    
    return results

def print_summary(results):
    """
    Print a summary of the extraction results.
    
    Args:
    results (list): A list of tuples containing the results of each extraction
    """
    successful = sum(1 for _, success, _ in results if success)
    total = len(results)
    
    print(f"\n{successful}/{total} sources completed extraction")
    
    if successful < total:
        print("\nFailed sources:")
        for name, success, error_message in results:
            if not success:
                print(f"- {name}: {error_message}")

if __name__ == "__main__":
    results = extract_data(config)
    print_summary(results)