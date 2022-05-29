import requests

import os
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor


download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def save_files(uri: str, file_name: str, response: requests.models.Response) -> str:
  if uri.find('/'):
    file_name += uri.rsplit('/', 1)[1]
    open(file_name, 'wb').write(response.content)
    return file_name


def process_files(uri: str, file_name: str, directory: str) -> bool:
  # Download the files, split the name from URI, extract csv files and delete zip
  response = requests.get(uri, allow_redirects=True)
  if response.status_code == 200:
    file_name = save_files(uri, file_name, response)
    open(file_name, 'wb').write(response.content)
    with ZipFile(file_name) as zip_object:
      files_list = zip_object.namelist()
      for file in files_list:
        if file.endswith('.csv'):
          zip_object.extract(file, directory)

    # Delete the zip file
    os.remove(file_name)
    return True


def main():
    # Create downloads directory if there isn't one already
    downloads_path = './downloads/'
    if not os.path.isdir(downloads_path):
      os.mkdir(downloads_path)

    file_name = downloads_path
    no_threads = len(download_uris)
    with ThreadPoolExecutor(no_threads) as executor:
      _ = [executor.submit(process_files, uri, file_name, downloads_path) for uri in download_uris]


if __name__ == '__main__':
    main()
