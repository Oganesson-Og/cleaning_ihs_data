# XML to CSV Converters for Port Data

## Overview

This project contains scripts to convert XML files containing various port-related data into structured JSON and CSV formats. The scripts handle nested XML elements such as ports, countries, sections, tables, and PAR elements to produce detailed JSON and CSV files.

## Scripts

### 1. `general_complete_parser.py`

This script parses an XML file containing general port data and converts it to a dataframe by first generating a JSON object. THe dataframe is then saced as a csv.

#### Functions:
- `parse_port(port_element)`: Parses an individual port element from the XML.
- `parse_xml_to_json(xml_file)`: Parses the entire XML file and converts it to JSON format.

### 2. `pre_arrival_parser.py`

This script reads an XML file containing pre-arrival information for ports, extracts relevant details, and converts it to a structured JSON format.

#### Functions:
- `parse_port(port_element)`: Parses an individual port element from the XML.
- `parse_xml_to_json(xml_file)`: Parses the entire XML file and converts it to JSON format.

### 3. `port_description_complete_parser.py`

This script reads an XML file containing port description data, extracts relevant details, and converts it to a structured JSON format. It then processes the JSON data to create a DataFrame and saves it as a CSV file.

#### Functions:
- `parse_port(port_element)`: Parses an individual port element from the XML.
- `parse_xml_to_json(xml_file)`: Parses the entire XML file and converts it to JSON format.
- `process_json_to_csv(json_data, csv_file)`: Processes the JSON data to create a DataFrame and saves it as a CSV file.

### 4. `port_countries_complete_parser.py`

This script reads an XML file containing port countries data, extracts relevant details, and converts it to a structured JSON format. It then processes the JSON data to create a DataFrame and saves it as a CSV file.

#### Functions:
- `parse_country(country_element)`: Parses an individual country element from the XML.
- `parse_xml_to_json(xml_file)`: Parses the entire XML file and converts it to JSON format.
- `process_json_to_csv(json_data, csv_file)`: Processes the JSON data to create a DataFrame and saves it as a CSV file.

### 5. `berths_and_cargo_complete_parser.py`

This script reads an XML file containing berths and cargo data, extracts relevant details, and converts it to a structured JSON format. It then processes the JSON data to create a DataFrame and saves it as a CSV file.

#### Functions:
- `parse_port(port_element)`: Parses an individual port element from the XML.
- `parse_xml_to_json(xml_file)`: Parses the entire XML file and converts it to JSON format.
- `process_json_to_csv(json_data, csv_file)`: Processes the JSON data to create a DataFrame and saves it as a CSV file.

## Usage

Each script can be run from the command line. For example, to run the `pre_arrival_parser.py` script:

```sh
python pre_arrival_parser.py
