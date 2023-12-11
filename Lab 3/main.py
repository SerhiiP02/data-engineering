# import boto3
import os
import json
import csv
from flatten_json import flatten

def process_json_file(json_file_path):
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
        # Перевіряємо, чи є вкладені дані
        flattened_data = flatten(data)
        return flattened_data

def convert_json_to_csv(json_folder, csv_folder):
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)

    for root, dirs, files in os.walk(json_folder):
        for json_file_name in files:
            if json_file_name.endswith('.json'):
                json_file_path = os.path.join(root, json_file_name)
                csv_file_path = os.path.join(csv_folder, json_file_name.replace('.json', '.csv'))

                json_data = process_json_file(json_file_path)

                with open(csv_file_path, 'w', newline='') as csv_file:
                    csv_writer = csv.DictWriter(csv_file, fieldnames=json_data.keys())
                    csv_writer.writeheader()
                    csv_writer.writerow(json_data)

def main():
    json_folder = './data'  
    csv_folder = './csv_output'

    convert_json_to_csv(json_folder, csv_folder)

if __name__ == "__main__":
    main()
