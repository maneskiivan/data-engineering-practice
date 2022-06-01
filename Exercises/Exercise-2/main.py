import os

import requests
import pandas as pd
from bs4 import BeautifulSoup


def send_request(uri: str) -> requests.models.Response:
  response = requests.get(uri, allow_redirects=True)
  if response.status_code == 200:
    return response


def create_soup(data: requests.models.Response) -> BeautifulSoup:
  return BeautifulSoup(data, 'html.parser')


def find_file_name(soup: BeautifulSoup, dt: str) -> str:
  table = soup.find('table')
  file_name = None
  for i in table.find_all('tr'):
    for a in i.find_all('td'):
      if dt in a.text:
        return file_name
      if '.csv' in a.text:
        file_name = a.text

  return 'not found'


def create_dir(path: str) -> str:
  # Create downloads directory if there isn't one already
  if not os.path.isdir(path):
    os.mkdir(path)

  return path


def clear_old_data(path: str) -> None:
  if os.path.exists(path):
    os.remove(path)


def download_files(uri: str, file_name: str, directory: str) -> str:
  response = send_request(uri)
  file_name = directory + file_name
  clear_old_data(file_name)
  with open(file_name, 'wb') as f:
    f.write(response.content)
    f.close()
  if os.path.exists(file_name):
    return file_name


def main():
    # your code here
    uri = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    dt = '2022-02-07 14:03'
    data = send_request(uri)
    soup = create_soup(data.text)
    file_name = find_file_name(soup, dt)
    download_link = uri + file_name
    download_dir = create_dir('./downloads/')
    download_file = download_files(download_link, file_name, download_dir)


if __name__ == '__main__':
    main()
