"""
column_configs.py

This module defines the configuration dictionaries for preprocessing the data sources.
Each dictionary specifies column mappings, data types, and (where applicable) melt configurations for a specific data source.

The configurations are used to ensure consistent handling of data across different sources.

Key components:
- column_mapping: Specifies how original column names should be renamed
- column_types: Defines the data type for each column after preprocessing
- melt_config: Provides parameters for reshaping data from wide to long format (where applicable)
"""

# Configuration for additional dwellings data
additional_dwellings = {
    'column_mapping': {
        'Authority Data': 'location_name',  # mapping for location column
        '2001-02': '2002',  # mapping year data to standard format
        '2002-03': '2003',
        '2003-04': '2004',
        '2004-05 [note 1]': '2005',
        '2005-06 [note 2]': '2006',
        '2006-07 [note 3]': '2007',
        '2007-08 [note 4]': '2008',
        '2008-09 [note 5]': '2009',
        '2009-10 [note 6]': '2010',
        '2010-11 [note 7]': '2011',
        '2011-12 [note 8]': '2012',
        '2012-13 [note 9]': '2013',
        '2013-14 [note 10]': '2014',
        '2014-15 [note 11]': '2015',
        '2015-16 [note 12]': '2016',
        '2016-17 [note 13]': '2017',
        '2017-18 [note 14]': '2018',
        '2018-19': '2019',
        '2019-20': '2020',
        '2020-21': '2021',
        '2021-22 [r] [note 15]': '2022',
        '2022-23 [p] [note 16]': '2023'
    },
    'column_types': {
        'location_name': 'str',  # set location name as string
        'date': 'str',  # set date as string
        'additional_dwellings': 'int'  # set dwellings count as integer
    },
    'melt_config': {
        'id_vars': ['location_name'],  # variables to keep intact during melt
        'var_name': 'date',  # name for variable column in melt
        'value_name': 'additional_dwellings'  # name for values column in melt
    }
}

# Configuration for Bank of England base rate data
boe_rate = {
    'column_mapping': {
        'Date Changed': 'date',  # mapping date column
        'Rate': 'rate'  # mapping rate column
    },
    'column_types': {
        'date': 'str',  # set date as string
        'rate': 'float'  # set rate as float
    }
}

# Configuration for mortgage rate data
mortgage_rate = {
    'column_types': {
        'rate_type': 'str',  # set rate type as string
        'date': 'str',  # set date as string
        'rate': 'float'  # set rate as float
    },
    'melt_config': {
        'id_vars': ['rate_type'],  # variables to keep intact during melt
        'var_name': 'date',  # name for variable column in melt
        'value_name': 'rate'  # name for values column in melt
    }
}

# Configuration for school count data
school_count = {
    'column_mapping': {
        'time_period': 'date',  # mapping time period to date
        'la_name': 'location_name',  # mapping local authority name
        'number_of_schools': 'school_count',  # mapping count of schools
    },
    'column_types': {
        'date': 'str',  # set date as string
        'location_name': 'str',  # set location name as string
        'school_count': 'int'  # set school count as integer
    }
}

# Configuration for unemployment data
unemployment = {
    'column_mapping': {
        'Title': 'date',  # mapping title column to date
        'Unemployment rate (aged 16 and over, seasonally adjusted): %': 'unemployment_rate'  # mapping unemployment rate
    },
    'column_types': {
        'date': 'str',  # set date as string
        'unemployment_rate': 'float'  # set unemployment rate as float
    }
}

# Configuration for price paid data
price_paid = {
    'column_mapping': {
        '0': 'transaction_id',  # mapping transaction ID
        '1': 'price',  # mapping price
        '2': 'date',  # mapping date
        '3': 'postcode',  # mapping postcode
        '4': 'property_type',  # mapping property type
        '5': 'old_new',  # mapping old/new status
        '6': 'freehold_leasehold',  # mapping tenure type
        '7': 'address_1',  # mapping address line 1
        '8': 'address_2',  # mapping address line 2
        '9': 'street',  # mapping street
        '11': 'location_name',  # mapping location name
        '14': 'transaction_category',  # mapping transaction category
        '15': 'transaction_status'  # mapping transaction status
    },
    'column_types': {
        'transaction_id': 'str',  # set transaction ID as string
        'price': 'int',  # set price as integer
        'date': 'str',  # set date as string
        'postcode': 'str',  # set postcode as string
        'property_type': 'str',  # set property type as string
        'old_new': 'str',  # set old/new as string
        'freehold_leasehold': 'str',  # set tenure type as string
        'address_1': 'str',  # set address line 1 as string
        'address_2': 'str',  # set address line 2 as string
        'street': 'str',  # set street as string
        'location_name': 'str',  # set location name as string
        'transaction_category': 'str',  # set transaction category as string
        'transaction_status': 'str'  # set transaction status as string
    }
}