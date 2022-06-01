import requests
import pandas
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


def main():
    # your code here
    uri = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    dt = '2022-02-07 14:03'
    data = send_request(uri)
    soup = create_soup(data.text)
    download_link = uri + find_file_name(soup, dt)




if __name__ == '__main__':
    main()
