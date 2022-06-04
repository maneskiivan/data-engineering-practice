import boto3

import io
import gzip


def main():
  bucket = 'commoncrawl'
  key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
  file = key.rsplit('/', 1)[1]

  s3 = boto3.client('s3')
  response_obj = s3.get_object(Bucket=bucket, Key=key)
  object_content = response_obj['Body'].read()


if __name__ == '__main__':
    main()
