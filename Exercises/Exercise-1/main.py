import requests

import os
from zipfile import ZipFile


download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]


def download_files(uri):
  response = requests.get(uri, allow_redirects=True)
  if response.status_code == 200:
    return response
  else:
    return False


def split_name(uri, file_name, response):
  if uri.find('/'):
    file_name += uri.rsplit('/', 1)[1]
    open(file_name, 'wb').write(response.content)
    return file_name


def extract_csv(file_name, directory):
  with ZipFile(file_name) as zip_object:
    files_list = zip_object.namelist()
    for file in files_list:
      if file.endswith('.csv'):
        zip_object.extract(file, directory)
        return True
      else:
        return False


def main():
    # Create downloads directory if there isn't one already
    downloads_path = './downloads/'
    if not os.path.isdir(downloads_path):
      os.mkdir(downloads_path)

    # Download the files, split the name from URI and extract csv files
    file_name = downloads_path
    no_threads = len(download_uris)
    for uri in download_uris:
      response = download_files(uri)
      if response:
        file_name = split_name(uri, file_name, response)
        extract_csv(file_name, downloads_path)

        # Delete the zip file
        os.remove(file_name)



if __name__ == '__main__':
    main()
