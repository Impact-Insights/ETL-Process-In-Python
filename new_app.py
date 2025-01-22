import glob 
import pandas as pd 
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as bs
from datetime import datetime 

log_file = "log_file.txt"
target_file = "transformed_data.csv"

#Creating funcctions for the Individual processes of the ETL processes.


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.read_xml(file_to_process)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns = ["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
    return dataframe
    
def extract_from_json(file_to_process):
    dataframe = pd.json_json(file_to_process)
    return dataframe

# function to call on basis of the filetype of the data file.

def extract():
    extracted_data = pd.DataFrame(columns = ['name', 'weight', 'height']) #create an empty dataframe to hold the extracted data

    #Process all csv files
    for csvfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data,pd.dataFrame(extract_from_csv(csvfile))], ignore_index = True)

    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
     
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
         
    return extracted_data

#TRANSFORMING THE DATA

#1. Convert the height from inches to meters adn wigt from pounds to kilograms. 
def transform(data):
    data['height'] = round(data.height*0.0254, 2)
    data['weight'] = round(data.weight*0.45359237, 2)

    return data