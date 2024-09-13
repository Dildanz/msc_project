import sys
from pathlib import Path

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from etl.data_loader import DataLoader

ExploreData = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prop: <http://example.org/property#>

SELECT ?predicate (COUNT(?predicate) as ?count)
WHERE {
  ?subject rdf:type prop:Property ;
           ?predicate ?object .
}
GROUP BY ?predicate
ORDER BY DESC(?count)
LIMIT 20
"""

# Basic Property Data Query
BasicPropertyData = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prop: <http://example.org/property#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?property ?price ?date ?postcode ?propertyType ?oldNew ?tenure
WHERE {
  ?property rdf:type prop:Property ;
            prop:price ?price ;
            prop:date ?date ;
            prop:postcode ?postcode ;
            prop:propertyType ?propertyType ;
            prop:oldNew ?oldNew ;
            prop:tenure ?tenure .
}
LIMIT 1000
"""

# Property Price by Type Query
PropertyPriceByType = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prop: <http://example.org/property#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?propertyType (AVG(?price) AS ?avgPrice) (COUNT(?property) AS ?count)
WHERE {
  ?property rdf:type prop:Property ;
            prop:price ?price ;
            prop:propertyType ?propertyType .
}
GROUP BY ?propertyType
ORDER BY DESC(?count)
"""

# Property Sales Over Time Query
PropertySalesOverTime = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prop: <http://example.org/property#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?year (COUNT(?property) AS ?salesCount) (AVG(?price) AS ?avgPrice)
WHERE {
  ?property rdf:type prop:Property ;
            prop:price ?price ;
            prop:date ?fullDate .
  BIND(YEAR(xsd:dateTime(?fullDate)) AS ?year)
}
GROUP BY ?year
ORDER BY ?year
"""

# New vs Old Properties Price Comparison
NewVsOldComparison = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX prop: <http://example.org/property#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?oldNew (AVG(?price) AS ?avgPrice) (COUNT(?property) AS ?count)
WHERE {
  ?property rdf:type prop:Property ;
            prop:price ?price ;
            prop:oldNew ?oldNew .
}
GROUP BY ?oldNew
ORDER BY ?oldNew
"""

def execute_and_print_query(ontology_db, query, query_name):
    print(f"\nExecuting {query_name}...")
    results = ontology_db.execute_query(query)
    
    print(f"\n{query_name} Results:")
    if results.empty:
        print(f"No data returned from the {query_name}.")
    else:
        print(f"Number of results: {len(results)}")
        print("\nFirst few rows:")
        print(results.head())
        
        # Save the results to a CSV file
        output_file = f'{query_name.lower().replace(" ", "_")}.csv'
        ontology_db.extract_and_save_data_cube(query, output_file)
        print(f"\nResults saved to {output_file}")

def main():
    try:
        loader = DataLoader()
        ontology_db = loader.get_ontology_database()
        
        execute_and_print_query(ontology_db, BasicPropertyData, "Basic Property Data")
        execute_and_print_query(ontology_db, PropertyPriceByType, "Property Price by Type")
        execute_and_print_query(ontology_db, PropertySalesOverTime, "Property Sales Over Time")
        execute_and_print_query(ontology_db, NewVsOldComparison, "New vs Old Properties Comparison")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        print("Traceback:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()