import boto3

import gzip
import io


def main():
  bucket = 'commoncrawl'
  key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, key)

  buf = io.BytesIO(obj.get()["Body"].read())  # reads whole gz file into memory
  uncompressed = [line.decode('utf-8') for line in gzip.GzipFile(fileobj=buf)]
  key = uncompressed[0].rstrip()

  obj = s3.Object(bucket, key)

  buf = io.BytesIO(obj.get()["Body"].read())  # reads whole gz file into memory
  for line in gzip.GzipFile(fileobj=buf):
    print(line.rstrip().decode('utf-8'))


if __name__ == '__main__':
    main()
