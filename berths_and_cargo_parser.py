"""
XML to CSV Converter for Berths and Cargo Data

Author: Keith Satuku

Description:
This script reads an XML file containing berths and cargo data, extracts relevant details from the XML structure,
and converts it to a structured JSON format. The script then processes the JSON data to create a DataFrame and saves it as a CSV file.
The script handles nested XML elements such as ports, sections, tables, and PAR elements to produce a detailed JSON file.

Usage:
The script takes an XML file as input (e.g., 'BerthsandCargo.xml') and outputs a corresponding CSV file (e.g., 'BerthsandCargo.csv').

Classes:
- XMLToCSVConverter: Handles parsing of the XML file and conversion to JSON and CSV formats.
- DataProcessor: Processes nested JSON structures and prepares them for DataFrame creation.

"""

import xml.etree.ElementTree as ET
import pandas as pd
import ast

class XMLToCSVConverter:
    """
    A class to handle XML to CSV conversion for berths and cargo data.

    Attributes:
        xml_file (str): Path to the XML file to be parsed.
        csv_file (str): Path to the output CSV file.
    """

    def __init__(self, xml_file: str, csv_file:str)-> None:
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
    def parse_port(port_element: ET.Element)-> dict:
        """
        Parses an individual port element from the XML and extracts port details.

        Args:
            port_element (xml.etree.ElementTree.Element): An XML element representing a port.

        Returns:
            dict: A dictionary containing parsed port data, including sections, tables, and PARs.
        """
        port_data = {
            'PortName': port_element.find('PortName').text,
            'PortID': port_element.find('PortID').text,
            'WorldPortNumber': port_element.find('WorldPortNumber').text,
            'Sections': []
        }

        for section in port_element.findall('Section'):
            section_data = {
                'SectionHeader': section.find('SectionHeader').text,
                'ID': section.find('SectionHeader').get('ID'),
                'Tables': [],
                'PARs': []
            }

            for table in section.findall('table'):
                table_data = {
                    'ID': table.get('ID'),
                    'updatedate': table.get('updatedate'),
                    'Rows': []
                }

                for row in table.findall('.//row'):
                    row_data = []
                    for entry in row.findall('entry'):
                        para = entry.find('para')
                        if 'namest' in entry.attrib and 'nameend' in entry.attrib:
                            row_data.append({
                                'span': True,
                                'text': para.text if para is not None else ''
                            })
                        else:
                            row_data.append(para.text if para is not None else '')
                    table_data['Rows'].append(row_data)

                section_data['Tables'].append(table_data)

            for par in section.findall('PAR'):
                par_data = {
                    'ID': par.get('ID'),
                    'updatedate': par.get('updatedate'),
                    'Text': par.text
                }
                section_data['PARs'].append(par_data)

            port_data['Sections'].append(section_data)

        return port_data

    def parse_xml_to_json(self)-> list:
        """
        Parses the XML file and converts it to JSON format.

        Returns:
            list: A list of dictionaries representing parsed port data, or None if parsing fails.
        """
        root = self.parse_xml()
        if root is None:
            return None

        ports_data = []
        for group in root.findall('Group'):
            for port in group.findall('Port'):
                ports_data.append(self.parse_port(port))

        return ports_data

    def process_json_to_csv(self, json_data: list)-> None:
        """
        Converts JSON data to a CSV file.

        Args:
            json_data (list): Parsed JSON data from the XML.
        """
        processed_data = []

        for port in json_data:
            port_info = {
                'PortName': port['PortName'],
                'PortID': port['PortID'],
                'WorldPortNumber': port['WorldPortNumber']
            }

            for section in port['Sections']:
                section_header = section['SectionHeader']
                port_info[section_header] = section

            processed_data.append(port_info)

        df = pd.DataFrame(processed_data)
        processed_df = DataProcessor.intermediate_to_next(df)
        processed_df.to_csv(self.csv_file, index=False)
        print(f"CSV data written to {self.csv_file}")

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
        trimming_columns = ['PortName', 'PortID', 'WorldPortNumber']

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

            processed_data[f"{col}Rows"] = rows_list
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
    def _process_tables(data, rows_list: list, update_dates: list)-> None:
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

    Reads an XML file ('BerthsandCargo.xml'), parses it to JSON format, processes the JSON data to create a DataFrame, 
    and writes the resulting DataFrame to 'BerthsandCargo.csv'.
    """
    xml_file = 'BerthsandCargo.xml'
    csv_file = 'BerthsandCargo.csv'

    converter = XMLToCSVConverter(xml_file, csv_file)
    json_data = converter.parse_xml_to_json()

    if json_data:
        converter.process_json_to_csv(json_data)
    else:
        print("No data to write to CSV")
