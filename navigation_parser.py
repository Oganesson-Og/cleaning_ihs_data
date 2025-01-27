"""
XML to CSV Converter for Navigation Data

Author: Keith Satuku

Description:
This script reads an XML file containing Navigation data, extracts relevant details from the XML structure,
and converts it to a structured JSON format. The script then processes the JSON data to create a DataFrame and saves it as a CSV file.
The script handles nested XML elements such as ports, sections, tables, and PAR elements to produce a detailed JSON file.

Usage:
The script takes an XML file as input (e.g., 'Navigation.xml') and outputs a corresponding CSV file (e.g., 'Navigation.csv').

Functions:
- parse_port(port_element): Parses an individual port element from the XML.
- parse_xml_to_json(xml_file): Parses the entire XML file and converts it to JSON format.
- process_json_to_csv(json_data, csv_file): Processes the JSON data to create a DataFrame and saves it as a CSV file.

"""

import xml.etree.ElementTree as ET
import json
import pandas as pd
import ast

class XMLToCSVConverter:
    """
    A class to convert XML (specifically Navigation data) to CSV by parsing nested elements:
    ports, sections, tables, and PAR elements, then processing them into a pandas DataFrame.
    """

    def __init__(self, xml_file: str, csv_file: str):
        """
        Initializes the converter with file paths.

        Args:
            xml_file (str): The path to the source XML file.
            csv_file (str): The path where the CSV file should be saved.
        """
        self.xml_file = xml_file
        self.csv_file = csv_file

    def parse_port(self, port_element):
        """
        Parses an individual port element from the XML and extracts port details, including sections, tables, and PARs.

        Args:
            port_element (xml.etree.ElementTree.Element): An XML element representing a port.

        Returns:
            dict: A dictionary containing the parsed port data with keys such as 
                  'PortName', 'PortID', 'WorldPortNumber', and 'Sections'.
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

    def parse_xml_to_json(self):
        """
        Parses an entire XML file containing Navigation data and converts it to a JSON format.

        Returns:
            list: A list of dictionaries representing the parsed port data, or None if an error occurs.
        """
        try:
            tree = ET.parse(self.xml_file)
            root = tree.getroot()
            print(f"Root tag: {root.tag}")

            # Debug: Print all child tags of the root
            for child in root:
                print(f"Child tag: {child.tag}")

            ports_data = []
            for group in root.findall('Group'):
                for port in group.findall('Port'):
                    ports_data.append(self.parse_port(port))

            return ports_data
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
        except FileNotFoundError:
            print(f"File not found: {self.xml_file}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def intermediate_to_next(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        An internal helper method that processes JSON-like column data and flattens
        tables and PAR elements into separate rows, update dates, and paragraph data.

        Args:
            df (pd.DataFrame): DataFrame containing columns of JSON-like data.

        Returns:
            pd.DataFrame: A new DataFrame with parsed rows, update dates, and paragraph data.
        """
        processed_data = {}
        # Keep the primary columns intact
        trimming_columns = ['PortName', 'PortID', 'WorldPortNumber']

        # Identify columns we need to process
        columns_to_process = [col for col in df.columns if col not in trimming_columns]

        for col in columns_to_process:
            rows_list = []
            update_dates = []
            pars_list = []

            for entry in df[col]:
                try:
                    # Parse the JSON-like structure in the cell
                    if isinstance(entry, str):
                        data = ast.literal_eval(entry)
                    elif isinstance(entry, dict):
                        data = entry
                    elif entry is None:
                        rows_list.append(None)
                        update_dates.append(None)
                        pars_list.append({})
                        continue

                    # Extract Tables
                    try:
                        if 'Tables' in data and data['Tables']:
                            tables = data['Tables']
                            rows = tables[0].get('Rows', [])
                            updatedate = tables[0].get('updatedate', None)
                            rows_list.append(rows)
                            update_dates.append(updatedate)
                        else:
                            rows_list.append(None)
                            update_dates.append(None)
                    except (KeyError, UnboundLocalError) as e:
                        print(f"Error parsing data for Tables: {entry}. Error: {e}")

                    # Extract PARs
                    try:
                        if 'PARs' in data and data['PARs']:
                            pars = data['PARs'][0]
                            if isinstance(pars, str):
                                pars_dict = ast.literal_eval(pars)
                            elif isinstance(pars, dict):
                                pars_dict = pars
                            pars_dict = {k: pars_dict[k] for k in ['updatedate', 'Text'] if k in pars_dict}
                            pars_list.append(pars_dict)
                        else:
                            pars_list.append({})
                    except Exception as e:
                        print(f"Error parsing data for Paragraphs: {entry}. Error: {e}")
                        pars_list.append({})

                except (ValueError, SyntaxError, KeyError) as e:
                    print(f"Error parsing data for entry: {entry}. Error: {e}")
                    rows_list.append(None)
                    update_dates.append(None)
                    pars_list.append({})

            processed_data[col] = rows_list
            processed_data[f"{col}UpdateDate"] = update_dates
            processed_data[f"{col}PARs"] = pars_list

        processed_df = pd.DataFrame(processed_data)
        trimmed_df = df[trimming_columns]

        # Merge processed columns with the primary columns
        final_df = pd.concat([trimmed_df, processed_df], axis=1)
        return final_df

    def process_json_to_csv(self, json_data):
        """
        Processes the JSON data to create a DataFrame and saves it as a CSV file.

        Args:
            json_data (list): A list of dictionaries representing the parsed port data.
        """
        processed_data = []

        # Build a list of dicts from the JSON data
        for port in json_data:
            port_info = {
                'PortName': port['PortName'],
                'PortID': port['PortID'],
                'WorldPortNumber': port['WorldPortNumber']
            }

            # Add each section under its 'SectionHeader' key
            for section in port['Sections']:
                section_header = section['SectionHeader']
                port_info[section_header] = section

            processed_data.append(port_info)

        # Create a DataFrame and transform it
        df = pd.DataFrame(processed_data)
        transformed_df = self.intermediate_to_next(df)

        # Write to CSV
        transformed_df.to_csv(self.csv_file, index=False)
        print(f"CSV data written to {self.csv_file}")

    def run(self):
        """
        Orchestrates the XML to CSV conversion workflow.
        """
        print(f"Reading file: {self.xml_file}")
        json_data = self.parse_xml_to_json()

        if json_data:
            self.process_json_to_csv(json_data)
        else:
            print("No data to write to CSV")


def main():
    """
    Main execution block.

    Reads an XML file ('Navigation.xml'), parses it to JSON format, processes 
    the JSON data to create a DataFrame, and writes the resulting DataFrame 
    to 'Navigation.csv'.
    """
    xml_file = 'Navigation.xml'
    csv_file = 'Navigation.csv'
    
    converter = XMLToCSVConverter(xml_file, csv_file)
    converter.run()


if __name__ == "__main__":
    main()