from flatten_json import flatten

import os
import json
import csv


def find_json(parent_dir: str) -> dict:
  json_files = dict()
  for root, subdirs, files in os.walk(parent_dir):
    for file in files:
      if '.json' in file:
        json_files[file] = root.rstrip('/')

  return json_files


def json_to_csv(json_file: dict, csv_file: str) -> str:
  data_file = open(csv_file, 'w', newline='')
  csv_writer = csv.writer(data_file)

  header = json_file.keys()
  csv_writer.writerow(header)
  csv_writer.writerow(json_file.values())

  data_file.close()

  if os.path.exists(csv_file):
    return csv_file


def main():
  json_files = find_json('data/')

  for fn, directory in json_files.items():
    json_path = f'{directory}/{fn}'
    csv_path = json_path.rstrip('.json')
    csv_path = csv_path + '.csv'

    with open(json_path, 'r') as f:
      flat_json = flatten(json.load(f))
      json_to_csv(flat_json, csv_path)


if __name__ == '__main__':
    main()
