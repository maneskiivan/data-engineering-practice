import unittest

import requests

from main import create_dir, get_file_name, send_request, download_files


class Test(unittest.TestCase):
  """Tests the funcs from main.py"""

  def setUp(self) -> None:
    self.download_uris = ['https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip']
    self.path = './test_cases/'
    self.file_name = self.directory = create_dir(self.path)

  def test_create_dir(self):
    self.assertEqual(create_dir(self.path), self.path)


  def test_send_request(self):
    self.assertIsInstance(send_request(self.download_uris[0]), requests.models.Response)


  def test_get_file_name(self):
    self.assertEqual(
      get_file_name(
        self.download_uris[0],
        self.file_name,
        send_request(self.download_uris[0])
      ),
      self.file_name + self.download_uris[0].rsplit('/', 1)[1]
    )


  def test_download_files(self):
    self.assertEqual(
      download_files(
        self.download_uris[0],
        self.file_name,
        self.directory
      ),
      True
    )


if __name__ == '__main__':
  unittest.main()
