"""
ontology_config.py

This module defines the ontology structure for a real estate and economic data knowledge graph.
It includes definitions for namespaces, classes, properties, and mappings used in the RDF transformation process.

Ontology Structure:

Property
|-- price (Data Property)
|-- propertyType (Data Property)
|-- newBuild (Data Property)
|-- tenure (Data Property)
|-- transactionStatus (Data Property)
|-- transactionCategory (Data Property)
|-- address1 (Data Property)
|-- address2 (Data Property)
|-- street (Data Property)
|-- postcode (Data Property)
|-- hasLocation (Object Property) -> Location
|-- soldAt (Object Property) -> TimePoint

Location
|-- name (Data Property)
|-- hasProperty (Object Property) -> Property
|-- hasEconomicIndicator (Object Property) -> EconomicIndicator

TimePoint
|-- date (Data Property)
|-- Year (Subclass)
    |-- year (Data Property)
|-- YearMonth (Subclass)
    |-- yearMonth (Data Property)
|-- FullDate (Subclass)

EconomicIndicator
|-- rate (Data Property)
|-- rateType (Data Property)
|-- measuredAt (Object Property) -> TimePoint
|-- NationalEconomicIndicator (Subclass)
    |-- unemploymentRate (Data Property)
|-- LocalEconomicIndicator (Subclass)
    |-- HousingMarketIndicator (Subclass)
        |-- additionalDwellings (Data Property)
        |-- schoolCount (Data Property)

Justification for Ontology Design:
1. Modularity is achieved through separate namespaces (prop, loc, time, econ, house).
2. Reusable classes like TimePoint and Location support various data types and analyses.
3. Granularity is handled via subclasses (e.g., Year, YearMonth, FullDate for TimePoint).
4. The structure allows representation of both specific properties and aggregate economic data.
5. Relationships between concepts are defined through object properties.
6. Flexibility in data representation is supported through various data properties and mappings.

This design supports efficient querying, data integration, and extensibility for future data sources.
"""

from rdflib import Namespace
from rdflib.namespace import XSD

# Define Namespaces
# These namespaces provide a modular structure for the ontology,
# allowing clear separation of concerns for different aspects of the data.
NAMESPACES = {
    'prop': Namespace("http://example.org/property#"),
    'loc': Namespace("http://example.org/location#"),
    'time': Namespace("http://example.org/time#"),
    'econ': Namespace("http://example.org/economic#"),
    'house': Namespace("http://example.org/housing#"),
}

# Main Classes
# These represent the core concepts in our ontology.
CLASSES = [
    {'name': 'Property', 'uri': NAMESPACES['prop'].Property},
    {'name': 'TimePoint', 'uri': NAMESPACES['time'].TimePoint},
    {'name': 'Location', 'uri': NAMESPACES['loc'].Location},
    {'name': 'EconomicIndicator', 'uri': NAMESPACES['econ'].EconomicIndicator},
]

# Subclasses
# These provide more specific categorisations and allow for granular data representation.
SUBCLASSES = [
    {'name': 'NationalEconomicIndicator', 'uri': NAMESPACES['econ'].NationalEconomicIndicator, 'parent': NAMESPACES['econ'].EconomicIndicator},
    {'name': 'LocalEconomicIndicator', 'uri': NAMESPACES['econ'].LocalEconomicIndicator, 'parent': NAMESPACES['econ'].EconomicIndicator},
    {'name': 'HousingMarketIndicator', 'uri': NAMESPACES['house'].HousingMarketIndicator, 'parent': NAMESPACES['econ'].LocalEconomicIndicator},
    {'name': 'Year', 'uri': NAMESPACES['time'].Year, 'parent': NAMESPACES['time'].TimePoint},
    {'name': 'YearMonth', 'uri': NAMESPACES['time'].YearMonth, 'parent': NAMESPACES['time'].TimePoint},
    {'name': 'FullDate', 'uri': NAMESPACES['time'].FullDate, 'parent': NAMESPACES['time'].TimePoint},
]

# Object Properties (Relationships)
# These define how different entities in the ontology relate to each other.
OBJECT_PROPERTIES = [
    {'name': 'hasLocation', 'uri': NAMESPACES['prop'].hasLocation, 'domain': NAMESPACES['prop'].Property, 'range': NAMESPACES['loc'].Location},
    {'name': 'soldAt', 'uri': NAMESPACES['prop'].soldAt, 'domain': NAMESPACES['prop'].Property, 'range': NAMESPACES['time'].TimePoint},
    {'name': 'hasProperty', 'uri': NAMESPACES['loc'].hasProperty, 'domain': NAMESPACES['loc'].Location, 'range': NAMESPACES['prop'].Property},
    {'name': 'hasEconomicIndicator', 'uri': NAMESPACES['loc'].hasEconomicIndicator, 'domain': NAMESPACES['loc'].Location, 'range': NAMESPACES['econ'].EconomicIndicator},
    {'name': 'measuredAt', 'uri': NAMESPACES['econ'].measuredAt, 'domain': NAMESPACES['econ'].EconomicIndicator, 'range': NAMESPACES['time'].TimePoint},
]

# Data Properties
# These define the attributes of our classes, allowing us to attach specific data points to our entities.
DATA_PROPERTIES = {
    'price': NAMESPACES['prop'].price,
    'propertyType': NAMESPACES['prop'].propertyType,
    'newBuild': NAMESPACES['prop'].newBuild,
    'tenure': NAMESPACES['prop'].tenure,
    'name': NAMESPACES['loc'].name,
    'date': NAMESPACES['time'].date,
    'year': NAMESPACES['time'].year,
    'yearMonth': NAMESPACES['time'].yearMonth,
    'rate': NAMESPACES['econ'].rate,
    'rateType': NAMESPACES['econ'].rateType,
    'additionalDwellings': NAMESPACES['house'].additionalDwellings,
    'schoolCount': NAMESPACES['house'].schoolCount,
    'unemploymentRate': NAMESPACES['econ'].unemploymentRate,
    'transactionStatus': NAMESPACES['prop'].transactionStatus,
    'transactionCategory': NAMESPACES['prop'].transactionCategory,
    'address1': NAMESPACES['prop'].address1,
    'address2': NAMESPACES['prop'].address2,
    'street': NAMESPACES['prop'].street,
    'locationName': NAMESPACES['prop'].locationName,
    'postcode': NAMESPACES['prop'].postcode,
}

# Date Formats
# These define the various date formats used in our data sources,
# allowing for flexible handling of temporal data.
DATE_FORMATS = {
    'YYYY': '%Y',
    'YYYY-MM': '%Y-%m',
    'YYYY-MM-DD': '%Y-%m-%d',
    'DD MMM YY': '%d %b %y',
    'ACADEMIC_YEAR': 'ACADEMIC_YEAR',
    'YYYY MMM': 'YYYY MMM',  # Placeholder, we will dynamically handle this in code
    'YYYY Q': 'YYYY Q'       # Placeholder for quarters
}

# Mappings
# These define how data from various sources should be mapped to our ontology.
# Each mapping includes the class, URI pattern, date format, and field mappings for a data source.
MAPPINGS = {
    'price_paid': {
        'class': NAMESPACES['prop'].Property,
        'uri_pattern': f"{NAMESPACES['prop']}Property/{{transaction_id}}",
        'date_format': DATE_FORMATS['YYYY-MM-DD'],
        'fields': {
            'transaction_id': {'property': NAMESPACES['prop'].transactionId, 'datatype': XSD.string},
            'price': {'property': NAMESPACES['prop'].price, 'datatype': XSD.integer},
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.dateTime},
            'postcode': {'property': NAMESPACES['prop'].postcode, 'datatype': XSD.string},
            'property_type': {'property': NAMESPACES['prop'].propertyType, 'datatype': XSD.string, 'mapping': {'d': 'detached', 's': 'semi-detached', 't': 'terraced', 'f': 'flats/maisonettes', 'o': 'other'}, 'valid_values': ['d', 's', 't', 'f', 'o']},
            'old_new': {'property': NAMESPACES['prop'].oldNew, 'datatype': XSD.string, 'mapping': {'y': 'newly built', 'n': 'established residential building'}, 'valid_values': ['y', 'n']},
            'freehold_leasehold': {'property': NAMESPACES['prop'].tenure, 'datatype': XSD.string, 'mapping': {'f': 'freehold', 'l': 'leasehold'}, 'valid_values': ['f', 'l']},
            'transaction_category': {'property': NAMESPACES['prop'].transactionCategory, 'datatype': XSD.string, 'mapping': {'a': 'standard transaction', 'b': 'non-standard transaction'}, 'valid_values': ['a', 'b']},
            'transaction_status': {'property': NAMESPACES['prop'].transactionStatus, 'datatype': XSD.string, 'valid_values': ['a', 'c']},  # Only 'Addition' and 'Change' are valid
            'address_1': {'property': NAMESPACES['prop'].address1, 'datatype': XSD.string},
            'address_2': {'property': NAMESPACES['prop'].address2, 'datatype': XSD.string},
            'location_name': {'property': NAMESPACES['prop'].hasLocation, 'object': True, 'class': NAMESPACES['loc'].Location},
        }
    },
    'additional_dwellings': {
        'class': NAMESPACES['house'].HousingMarketIndicator,
        'uri_pattern': f"{NAMESPACES['house']}HousingMarketIndicator/{{unique_id}}",
        'date_format': DATE_FORMATS['YYYY'],
        'fields': {
            'location_name': {'property': NAMESPACES['prop'].hasLocation, 'datatype': XSD.string},
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.gYear},
            'additional_dwellings': {'property': NAMESPACES['house'].additionalDwellings, 'datatype': XSD.decimal},
        }
    },
    'boe_rate': {
        'class': NAMESPACES['econ'].NationalEconomicIndicator,
        'uri_pattern': f"{NAMESPACES['econ']}NationalEconomicIndicator/{{unique_id}}",
        'date_format': DATE_FORMATS['DD MMM YY'],
        'fields': {
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.date},
            'rate': {'property': NAMESPACES['econ'].rate, 'datatype': XSD.decimal},
        }
    },
    'mortgage_rate': {
        'class': NAMESPACES['econ'].LocalEconomicIndicator,
        'uri_pattern': f"{NAMESPACES['econ']}LocalEconomicIndicator/{{unique_id}}",
        'date_format': DATE_FORMATS['YYYY-MM'],
        'fields': {
            'rate_type': {'property': NAMESPACES['econ'].rateType, 'datatype': XSD.string},
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.gYearMonth},
            'rate': {'property': NAMESPACES['econ'].rate, 'datatype': XSD.decimal},
        }
    },
    'school_count': {
        'class': NAMESPACES['house'].HousingMarketIndicator,
        'uri_pattern': f"{NAMESPACES['house']}HousingMarketIndicator/{{unique_id}}",
        'date_format': DATE_FORMATS['ACADEMIC_YEAR'],
        'fields': {
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.gYear},
            'location_name': {'property': NAMESPACES['prop'].hasLocation, 'datatype': XSD.string},
            'school_count': {'property': NAMESPACES['house'].schoolCount, 'datatype': XSD.integer},
        }
    },
    'unemployment': {
        'class': NAMESPACES['econ'].NationalEconomicIndicator,
        'uri_pattern': f"{NAMESPACES['econ']}NationalEconomicIndicator/{{unique_id}}",
        'date_format': [DATE_FORMATS['YYYY'], DATE_FORMATS['YYYY MMM'], DATE_FORMATS['YYYY Q']],  # Array of formats
        'fields': {
            'date': {'property': NAMESPACES['time'].date, 'datatype': XSD.gYear},
            'unemployment_rate': {'property': NAMESPACES['econ'].unemploymentRate, 'datatype': XSD.decimal},
        }
    }
}