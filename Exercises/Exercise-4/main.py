from flatten_json import flatten

import os
import json


def main():
  json_files = dict()
  for root, subdirs, files in os.walk('data/'):
    for file in files:
      if '.json' in file:
        json_files[file] = root.rstrip('/')

  for fn, directory in json_files.items():
    full_path = f'{directory}/{fn}'
    with open(full_path, 'r') as f:
      flat_json = flatten(json.load(f))
      print(flat_json)

if __name__ == '__main__':
    main()
