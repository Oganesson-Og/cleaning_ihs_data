"""
XML to CSV Converter for Port Countries Data

Author: Keith Satuku

Description:
This script reads an XML file containing Port Countries Data, extracts relevant details from the XML structure,
and converts it to a structured JSON format. The script then processes the JSON data to create a DataFrame and saves it as a CSV file.
The script handles nested XML elements such as ports, sections, tables, and PAR elements to produce a detailed JSON file.

Usage:
The script takes an XML file as input (e.g., 'PortCountries.xml') and outputs a corresponding CSV file (e.g., 'PortCountries.csv').

Classes:
- XMLToCSVConverter: Handles parsing of the XML file and conversion to JSON and CSV formats.
- DataProcessor: Processes nested JSON structures and prepares them for DataFrame creation.

"""

import xml.etree.ElementTree as ET
import pandas as pd
import ast

class XMLToCSVConverter:
    """
    A class to handle XML to CSV conversion.

    Attributes:
        xml_file (str): Path to the XML file to be parsed.
        csv_file (str): Path to the output CSV file.
    """

    def __init__(self, xml_file: str, csv_file: str):
        """
        Initializes the XMLToCSVConverter with the XML and CSV file paths.

        Args:
            xml_file (str): Path to the input XML file.
            csv_file (str): Path to the output CSV file.
        """
        self.xml_file = xml_file
        self.csv_file = csv_file

    def parse_xml(self)-> ET.Element:
        """
        Parses the XML file and returns the root element.

        Returns:
            xml.etree.ElementTree.Element: The root element of the parsed XML tree, or None if parsing fails.
        """
        try:
            tree = ET.parse(self.xml_file)
            root = tree.getroot()
            return root
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
        except FileNotFoundError:
            print(f"File not found: {self.xml_file}")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def parse_country(country_element: ET.Element) -> dict:
        """
        Parses an individual country element from the XML.

        Args:
            country_element (xml.etree.ElementTree.Element): XML element representing a country.

        Returns:
            dict: A dictionary containing parsed data for the country.
        """
        country_data = {
            'CountryName': country_element.find('CountryName').text,
            'CountryCode': country_element.find('CountryCode').text,
        }

        for group in country_element.findall('Group'):
            group_header = group.find('GroupHeader')
            if group_header is None:
                continue

            if group_header.text == 'General Information':
                XMLToCSVConverter._parse_sections(group, country_data)

            elif group_header.text == 'General Marine Information':
                XMLToCSVConverter._parse_sections(group, country_data)

        return country_data

    @staticmethod
    def _parse_sections(group: ET.Element, country_data:dict)-> None:
        """
        Helper method to parse sections within a group.

        Args:
            group (xml.etree.ElementTree.Element): XML element representing a group.
            country_data (dict): Dictionary to store parsed section data.
        """
        for section in group.findall('Section'):
            section_header = section.find('SectionHeader')
            if section_header is None:
                continue

            if section_header.text == 'Holidays':
                table = section.find('table')
                if table is not None:
                    holidays = []
                    for row in table.findall(".//row"):
                        entries = [entry.text.strip() if entry.text else "" for entry in row.findall("entry/para")]
                        if len(entries) == 2:
                            holidays.append({entries[0]: entries[1]})
                    country_data.update({section_header.text: holidays})
            else:
                for par in section.findall('PAR'):
                    paragraph_text = par.text if par.text else ""
                    last_updated = par.get('updatedate', "")
                    country_data.update({
                        section_header.text: paragraph_text,
                        f'{section_header.text} LastUpdated': last_updated
                    })

    def parse_xml_to_json(self)-> list:
        """
        Parses the XML file and converts it to JSON format.

        Returns:
            list: A list of dictionaries representing parsed data, or None if parsing fails.
        """
        root = self.parse_xml()
        if root is None:
            return None

        country_data = []
        for country in root.findall('Country'):
            country_data.append(self.parse_country(country))

        return country_data

    @staticmethod
    def process_json_to_csv(json_list: list, csv_file: str)-> None:
        """
        Converts a JSON list to a CSV file.

        Args:
            json_list (list): List of dictionaries to be converted to CSV.
            csv_file (str): Path to the output CSV file.
        """
        processed_data = []

        for json_obj in json_list:
            flattened_data = {}
            for key, value in json_obj.items():
                flattened_data[key] = value
            processed_data.append(flattened_data)

        df = pd.DataFrame(processed_data)
        print(f"Saving the resulting csv file as {csv_file}...")
        df.to_csv(csv_file, index=False)

class DataProcessor:
    """
    A class to process nested JSON structures and prepare data for DataFrame creation.
    """

    @staticmethod
    def intermediate_to_next(df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a DataFrame to extract nested structures.

        Args:
            df (pd.DataFrame): Input DataFrame with nested JSON structures.

        Returns:
            pd.DataFrame: Processed DataFrame with extracted and formatted data.
        """
        processed_data = {}
        trimming_columns = ["CountryName", "CountryCode"]

        columns_to_process = [col for col in df.columns if col not in trimming_columns]

        for col in columns_to_process:
            rows_list, update_dates, pars_list = [], [], []

            for entry in df[col]:
                data = DataProcessor._parse_entry(entry)
                if data is None:
                    rows_list.append(None)
                    update_dates.append(None)
                    pars_list.append({})
                    continue

                DataProcessor._process_tables(data, rows_list, update_dates)
                DataProcessor._process_pars(data, pars_list)

            processed_data[f"{col}"] = rows_list
            processed_data[f"{col}UpdateDate"] = update_dates
            processed_data[f"{col}PARs"] = pars_list

        processed_df = pd.DataFrame(processed_data)
        trimmed_df = df[trimming_columns]
        final_df = pd.concat([trimmed_df, processed_df], axis=1)
        return final_df

    @staticmethod
    def _parse_entry(entry)-> dict:
        """
        Parses an entry from the DataFrame.

        Args:
            entry: The entry to be parsed.

        Returns:
            dict or None: Parsed data or None if parsing fails.
        """
        try:
            if isinstance(entry, str):
                return ast.literal_eval(entry)
            elif isinstance(entry, dict):
                return entry
            return None
        except (ValueError, SyntaxError):
            return None

    @staticmethod
    def _process_tables(data: dict, rows_list: list, update_dates: list)-> None:
        """
        Processes table data within a parsed entry.

        Args:
            data (dict): Parsed entry data.
            rows_list (list): List to store row data.
            update_dates (list): List to store update dates.
        """
        if 'Tables' in data.keys() and data['Tables']:
            tables = data['Tables']
            rows = tables[0].get('Rows', [])
            updatedate = tables[0].get('updatedate', None)
            rows_list.append(rows)
            update_dates.append(updatedate)
        else:
            rows_list.append(None)
            update_dates.append(None)

    @staticmethod
    def _process_pars(data: dict, pars_list: list)-> None:
        """
        Processes PAR data within a parsed entry.

        Args:
            data (dict): Parsed entry data.
            pars_list (list): List to store PAR data.
        """
        if 'PARs' in data.keys() and data['PARs']:
            pars = data['PARs'][0]
            if isinstance(pars, str):
                pars_dict = ast.literal_eval(pars)
            elif isinstance(pars, dict):
                pars_dict = pars
            pars_list.append({key: pars_dict.get(key) for key in ['updatedate', 'Text']})
        else:
            pars_list.append({})

if __name__ == "__main__":
    """
    Main execution block.

    Reads an XML file ('PortCountries.xml'), parses it to JSON format, processes the JSON data to create a DataFrame, 
    and writes the resulting DataFrame to 'PortCountries.csv'.
    """
    xml_file = 'PortCountries.xml'
    csv_file = 'PortCountries.csv'

    converter = XMLToCSVConverter(xml_file, csv_file)
    json_data = converter.parse_xml_to_json()

    if json_data:
        XMLToCSVConverter.process_json_to_csv(json_data, csv_file)
    else:
        print("No data to write to CSV")
